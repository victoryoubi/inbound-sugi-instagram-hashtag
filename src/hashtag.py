import os
import re
import json
import requests
import random
import time
from datetime import datetime, timezone
from google.cloud import storage
from google.cloud import bigquery

_TZ_NO_COLON = re.compile(r"([+-]\d{2})(\d{2})$")  # +0000 -> +00:00
REDUCE_MSG = "Please reduce the amount of data you're asking for"


def _utc_now_iso_z():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _backoff_sleep(attempt: int, base: float = 1.0, cap: float = 60.0):
    sec = min(cap, base * (2 ** attempt)) + random.random()
    time.sleep(sec)


def _get_json_with_retry(url, params=None, timeout=(10, 120), max_attempts=6):
    """
    Graph API GET with retries for 429/5xx and transient 'reduce amount' / app limit errors.
    """
    last_err = None
    for attempt in range(max_attempts):
        try:
            r = requests.get(url, params=params, timeout=timeout)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            last_err = e
            _backoff_sleep(attempt)
            continue

        if r.status_code == 200:
            return r.json()

        txt = r.text or ""
        err_code = None
        is_transient = False

        try:
            j = r.json()
            err = (j or {}).get("error") or {}
            err_code = err.get("code")
            is_transient = bool(err.get("is_transient"))
            msg = err.get("message") or txt
        except Exception:
            msg = txt

        if (
            r.status_code in (429, 500, 502, 503, 504)
            or REDUCE_MSG in txt
            or (r.status_code == 403 and err_code == 4 and is_transient)
        ):
            last_err = RuntimeError(
                f"Error {r.status_code} (code={err_code}, transient={is_transient}): {msg}"
            )
            _backoff_sleep(attempt, base=2.0, cap=120.0)
            continue

        raise RuntimeError(f"Error {r.status_code}: {txt}")

    raise RuntimeError(f"Retry exhausted: {last_err}")


def _fetch_recent_media_light(hashtag_id, ig_user_id, access_token, api_version, per_page, max_items):
    """
    Stage 1: lightweight recent_media (id,timestamp,media_type,permalink)
    Returns:
      base_rows: list[dict]
      stage1_response: dict
    """
    url = f"https://graph.facebook.com/{api_version}/{hashtag_id}/recent_media"

    params = {
        "user_id": ig_user_id,
        "fields": "id,timestamp,media_type,permalink",
        "limit": per_page,
        "access_token": access_token,
    }

    all_rows = []
    pages = []
    page_count = 0

    while True:
        res = _get_json_with_retry(url, params=params, timeout=(10, 120), max_attempts=6)

        page_count += 1
        pages.append(res) 

        data = res.get("data", [])
        all_rows.extend(data)

        if len(all_rows) >= max_items:
            all_rows = all_rows[:max_items]
            break

        next_url = res.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None

    stage1_response = {
        "page_count": page_count,
        "returned_count": len(all_rows),
        "pages": pages,
    }
    return all_rows, stage1_response


def _fetch_media_detail_batch(media_ids, access_token, api_version):
    """
    Stage 2 (batch): fetch full fields for multiple media_ids in one request using ?ids=
    Returns dict: { "<id>": {...}, ... }
    """
    url = f"https://graph.facebook.com/{api_version}/"
    params = {
        "ids": ",".join(media_ids),
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "access_token": access_token,
    }
    return _get_json_with_retry(url, params=params, timeout=(10, 120), max_attempts=8)



def fetch_all_recent_media_with_snapshots(hashtag_id, ig_user_id, access_token, api_version):
    """
    2段階取得（バッチ版）+ snapshot用 raw response を返す（BQ schema 互換）

    Returns:
      detailed_rows: list[dict]   (mediaテーブル用の詳細行)
      stage1_params: dict         (JSON列に入れる想定)
      stage2_params: dict         (JSON列に入れる想定)
      stage1_response: dict       (JSON列: pages を含む)
      stage2_response: dict       (JSON列: batches を含む)
      stage2_response_meta: dict  (補助情報)
    """
    per_page = int(os.getenv("LIMIT", "10"))
    max_items = int(os.getenv("MAX_ITEMS", "500"))
    batch_size = int(os.getenv("DETAIL_BATCH_SIZE", "50"))
    batch_sleep = float(os.getenv("DETAIL_BATCH_SLEEP_SEC", "0.0"))

    stage1_params = {
        "endpoint": f"/{hashtag_id}/recent_media",
        "user_id": str(ig_user_id),
        "fields": "id,timestamp,media_type,permalink",
        "limit": per_page,
        "max_items": max_items,
        # access_token は保存しない
    }

    stage2_params = {
        "endpoint": "/",
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "detail_batch_size": batch_size,
        "detail_batch_sleep_sec": batch_sleep,
        # access_token は保存しない
    }

    # -----------------------------
    # Stage 1: recent_media（軽量） + ページごとの生レスポンス
    # -----------------------------
    base_rows, stage1_response = _fetch_recent_media_light(
        hashtag_id=hashtag_id,
        ig_user_id=ig_user_id,
        access_token=access_token,
        api_version=api_version,
        per_page=per_page,
        max_items=max_items,
    )

    # -----------------------------
    # Stage 2: detail をバッチで取得
    #   - media 用: stage2_response_merged（id -> detail）
    #   - snapshot 用: stage2_batches（バッチごとの生レスポンス/エラー）
    # -----------------------------
    detailed_rows = []
    stage2_response_merged = {}  # { "<id>": {...}, ... }
    stage2_batches = []          # [{"batch_index":..,"range":..,"ids":[..],"ok":..,"error":..,"response":..}, ...]

    stage2_meta = {
        "total_base_rows": len(base_rows),
        "detail_batch_size": batch_size,
        "failed_batches": [],  # [{"range":[i,j], "ids":[...], "error":"..."}]
    }

    for i in range(0, len(base_rows), batch_size):
        batch = base_rows[i:i + batch_size]
        ids = [x.get("id") for x in batch if x.get("id")]

        batch_rec = {
            "batch_index": i // batch_size,
            "range": [i, i + len(batch)],
            "ids": ids,
            "ok": False,
            "error": None,
            "response": None,  # 成功時は Graph API の生レスポンス dict
        }

        try:
            res = _fetch_media_detail_batch(ids, access_token, api_version)
            batch_rec["ok"] = True
            batch_rec["response"] = res

            if isinstance(res, dict):
                stage2_response_merged.update(res)

        except Exception as e:
            batch_rec["ok"] = False
            batch_rec["error"] = str(e)
            stage2_meta["failed_batches"].append({
                "range": [i, i + len(batch)],
                "ids": ids,
                "error": str(e),
            })

            # 失敗バッチは base の情報で埋める（後段の media テーブル用）
            for b in batch:
                detailed_rows.append({
                    "id": b.get("id"),
                    "caption": None,
                    "timestamp": b.get("timestamp"),
                    "media_type": b.get("media_type"),
                    "media_url": None,
                    "permalink": b.get("permalink"),
                    "comments_count": None,
                    "like_count": None,
                })

            stage2_batches.append(batch_rec)

            if batch_sleep > 0:
                time.sleep(batch_sleep)

            continue

        # 成功バッチ：merged から個別行を作る（落ちてるIDは base で埋める）
        for b in batch:
            mid = b.get("id")
            if mid and mid in stage2_response_merged and isinstance(stage2_response_merged[mid], dict):
                detailed_rows.append(stage2_response_merged[mid])
            else:
                detailed_rows.append({
                    "id": b.get("id"),
                    "caption": None,
                    "timestamp": b.get("timestamp"),
                    "media_type": b.get("media_type"),
                    "media_url": None,
                    "permalink": b.get("permalink"),
                    "comments_count": None,
                    "like_count": None,
                })

        stage2_batches.append(batch_rec)

        if batch_sleep > 0:
            time.sleep(batch_sleep)

        if (i // batch_size + 1) % 5 == 0:
            print(f"detail batch done: {min(i + batch_size, len(base_rows))}/{len(base_rows)}")


    stage2_response = {
        "summary": {
            "total_base_rows": len(base_rows),
            "returned_detail_ids": len(stage2_response_merged),
            "failed_batch_count": len(stage2_meta["failed_batches"]),
        },
        "batches": stage2_batches,  
    }

    return (
        detailed_rows,
        stage1_params,
        stage2_params,
        stage1_response,   
        stage2_response,   
        stage2_meta,
    )

def upload_to_gcs(bucket_name, blob_name, rows):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    ndjson = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
    blob.upload_from_string(ndjson, content_type="application/json")


def load_to_bigquery(dataset_id, table_id, gcs_uri):
    client = bigquery.Client()
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    table = client.get_table(table_ref)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        schema=table.schema,
        write_disposition="WRITE_APPEND",
        ignore_unknown_values=True,
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    
    print("load output_rows:", load_job.output_rows)
    print("load errors:", load_job.errors)
    print("load error_result:", load_job.error_result)


def _to_int_or_none(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    if isinstance(x, str):
        s = x.strip()
        if s == "" or s.lower() in {"n/a", "null", "none"}:
            return None
        try:
            return int(s)
        except ValueError:
            return None
    try:
        return int(x)
    except Exception:
        return None


def _to_ts_or_none(x):
    """
    Normalize various ISO8601-ish strings to RFC3339 that BigQuery accepts.
    Returns 'YYYY-MM-DDTHH:MM:SSZ' (UTC) or None.
    """
    if x is None:
        return None

    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None

        s = _TZ_NO_COLON.sub(r"\1:\2", s)

        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None

        dt_utc = dt.astimezone(timezone.utc).replace(microsecond=0)
        return dt_utc.isoformat().replace("+00:00", "Z")

    return None


def run_hashtag():
    api_version = os.getenv("FB_API_VERSION", "v24.0")
    ig_user_id = os.environ["IG_USER_ID"]
    access_token = os.environ["LONG_TOKEN"]

    bucket = os.environ["GCS_BUCKET"]
    dataset = os.environ["BQ_DATASET"]
    media_table = os.environ["BQ_TABLE"]

    snapshot_table = os.environ["BQ_SNAPSHOT_TABLE"]

    hashtag_ids = [x.strip() for x in os.environ["HASHTAG_IDS"].split(",") if x.strip()]
    sleep_sec = float(os.getenv("TAG_SLEEP_SEC", "10"))

    for i, hashtag_id in enumerate(hashtag_ids):
        print(f"===== Processing {hashtag_id} =====")

        # run_id は「1 hashtag 1 run」で一意になるように
        run_id = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{hashtag_id}"

        collected_at_iso = _utc_now_iso_z() 
        fetched_at_iso = collected_at_iso   

        # 取得 + snapshot raw を受け取る
        (
            rows,
            stage1_params,
            stage2_params,
            stage1_response,
            stage2_response,
            stage2_meta,
        ) = fetch_all_recent_media_with_snapshots(hashtag_id, ig_user_id, access_token, api_version)

        print(f"Fetched {len(rows)} rows")

        # -----------------------------
        # mediaテーブル用
        # -----------------------------
        hid_int = _to_int_or_none(hashtag_id)

        enriched_rows = []
        for row in rows:
            row["id"] = _to_int_or_none(row.get("id"))          
            row["hashtag_id"] = hid_int
            row["comments_count"] = _to_int_or_none(row.get("comments_count"))
            row["like_count"] = _to_int_or_none(row.get("like_count"))
            row["timestamp"] = _to_ts_or_none(row.get("timestamp"))
            row["fetched_at"] = fetched_at_iso
            enriched_rows.append(row)

        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        media_blob_name = f"instagram/hashtag/{hashtag_id}/{now}.ndjson"
        upload_to_gcs(bucket, media_blob_name, enriched_rows)
        media_gcs_uri = f"gs://{bucket}/{media_blob_name}"
        load_to_bigquery(dataset, media_table, media_gcs_uri)

        # -----------------------------
        # snapshotsテーブル用（異なるデータごとに1行）
        # -----------------------------
        snapshot_rows = []

        # stage1: ページごとに1行
        pages = (stage1_response or {}).get("pages", [])
        for page_idx, page_res in enumerate(pages):
            snapshot_rows.append({
                    "run_id": run_id,
                    "hashtag_id": str(hashtag_id),
                    "collected_at": collected_at_iso,
                    "ig_user_id": str(ig_user_id),
                    "api_version": str(api_version),
                
                    "stage1_params": stage1_params,
                    "stage2_params": None,
                
                    "stage1_response": {
                        "seq": int(page_idx),
                        "page": page_res,
                        "meta": {
                            "page_count": stage1_response.get("page_count"),
                            "returned_count": stage1_response.get("returned_count"),
                        },
                    },
                    "stage2_response": None,
                })

        # stage2: バッチごとに1行
        batches = (stage2_response or {}).get("batches", [])
        for b in batches:
            snapshot_rows.append({
                    "run_id": run_id,
                    "hashtag_id": str(hashtag_id),
                    "collected_at": collected_at_iso,
                    "ig_user_id": str(ig_user_id),
                    "api_version": str(api_version),
                
                    "stage1_params": None,
                    "stage2_params": stage2_params,
                
                    "stage1_response": None,
                    "stage2_response": {
                        "seq": int(b.get("batch_index", 0)),
                        "ok": b.get("ok"),
                        "error": b.get("error"),
                        "range": b.get("range"),
                        "ids": b.get("ids"),
                        "response": b.get("response"),
                    },
                })

        snapshot_blob_name = f"instagram/hashtag_snapshots/{hashtag_id}/{run_id}.ndjson"
        upload_to_gcs(bucket, snapshot_blob_name, snapshot_rows)
        snapshot_gcs_uri = f"gs://{bucket}/{snapshot_blob_name}"
        load_to_bigquery(dataset, snapshot_table, snapshot_gcs_uri)

        # 次ハッシュタグ前に待つ（最後は待たない）
        if i < len(hashtag_ids) - 1:
            print(f"Sleeping {sleep_sec}s before next hashtag...")
            time.sleep(sleep_sec)

    print("✅ All hashtags done")


if __name__ == "__main__":
    run_hashtag()
