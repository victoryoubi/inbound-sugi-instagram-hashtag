import os
import json
import requests
import random
import time
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery


REDUCE_MSG = "Please reduce the amount of data you're asking for"

def _backoff_sleep(attempt: int, base: float = 1.0, cap: float = 60.0):
    # 1,2,4,8... + jitter
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

        # 可能ならJSONでエラー詳細を読む（Graphはだいたいこの形）
        try:
            j = r.json()
            err = (j or {}).get("error") or {}
            err_code = err.get("code")
            is_transient = bool(err.get("is_transient"))
            msg = err.get("message") or txt
        except Exception:
            msg = txt

        # ✅ リトライしたいケース
        # - 429: rate limit
        # - 5xx: transient
        # - reduce message
        # - 403 code=4 (Application request limit reached) かつ transient
        if (
            r.status_code in (429, 500, 502, 503, 504)
            or REDUCE_MSG in txt
            or (r.status_code == 403 and err_code == 4 and is_transient)
        ):
            last_err = RuntimeError(f"Error {r.status_code} (code={err_code}, transient={is_transient}): {msg}")
            _backoff_sleep(attempt, base=2.0, cap=120.0)  # ★少し強めに待つ
            continue

        # Hard failure
        raise RuntimeError(f"Error {r.status_code}: {txt}")

    raise RuntimeError(f"Retry exhausted: {last_err}")

def _fetch_recent_media_light(hashtag_id, ig_user_id, access_token, api_version, per_page, max_items):
    """
    Stage 1: lightweight recent_media (IDs only-ish)
    """
    url = f"https://graph.facebook.com/{api_version}/{hashtag_id}/recent_media"

    params = {
        "user_id": ig_user_id,
        "fields": "id,timestamp,media_type,permalink",
        "limit": per_page,
        "access_token": access_token,
    }

    all_rows = []
    while True:
        res = _get_json_with_retry(url, params=params, timeout=(10, 120), max_attempts=6)
        data = res.get("data", [])
        all_rows.extend(data)

        if len(all_rows) >= max_items:
            return all_rows[:max_items]

        next_url = res.get("paging", {}).get("next")
        if not next_url:
            return all_rows

        # next contains token and params in URL already
        url = next_url
        params = None

def _fetch_media_detail_batch(media_ids, access_token, api_version):
    """
    Stage 2 (batch): fetch full fields for multiple media_ids in one request using ?ids=
    戻り値は { "<id>": {...}, "<id>": {...} } のdict
    """
    url = f"https://graph.facebook.com/{api_version}/"
    params = {
        "ids": ",".join(media_ids),
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "access_token": access_token,
    }
    return _get_json_with_retry(url, params=params, timeout=(10, 120), max_attempts=8)

def fetch_all_recent_media(hashtag_id, ig_user_id, access_token, api_version):
    """
    2段階取得（バッチ版）
    Stage1: recent_mediaは軽く（id等のみ）
    Stage2: ids= でまとめて詳細取得（API回数激減）
    env:
      LIMIT: recent_media の1ページ件数（推奨 10〜25）
      MAX_ITEMS: 1ハッシュタグ最大取得件数（要望通り 500）
      DETAIL_BATCH_SIZE: 詳細をまとめる件数（推奨 25〜50）
      DETAIL_BATCH_SLEEP_SEC: バッチ間の待ち（推奨 0〜0.5）
    """
    per_page = int(os.getenv("LIMIT", "10"))
    max_items = int(os.getenv("MAX_ITEMS", "500"))

    batch_size = int(os.getenv("DETAIL_BATCH_SIZE", "50"))  # ★ここが肝
    batch_sleep = float(os.getenv("DETAIL_BATCH_SLEEP_SEC", "0.0"))

    # Stage 1: light list
    base_rows = _fetch_recent_media_light(
        hashtag_id=hashtag_id,
        ig_user_id=ig_user_id,
        access_token=access_token,
        api_version=api_version,
        per_page=per_page,
        max_items=max_items,
    )

    detailed = []

    # Stage 2: batch detail
    for i in range(0, len(base_rows), batch_size):
        batch = base_rows[i:i + batch_size]
        ids = [x["id"] for x in batch]

        try:
            res = _fetch_media_detail_batch(ids, access_token, api_version)
        except Exception as e:
            # バッチが丸ごと落ちたら、最低限を突っ込んで続行（運用優先）
            print(f"warn: batch detail fetch failed ids[{i}:{i+batch_size}]: {e}")
            for b in batch:
                detailed.append({
                    "id": b.get("id"),
                    "caption": None,
                    "timestamp": b.get("timestamp"),
                    "media_type": b.get("media_type"),
                    "media_url": None,
                    "permalink": b.get("permalink"),
                    "comments_count": None,
                    "like_count": None,
                })
            continue

        # res は {id: {...}} 形式
        for b in batch:
            mid = b["id"]
            if isinstance(res, dict) and mid in res and isinstance(res[mid], dict):
                detailed.append(res[mid])
            else:
                # 返ってこなかったIDは最低限で埋める（NULL）
                detailed.append({
                    "id": b.get("id"),
                    "caption": None,
                    "timestamp": b.get("timestamp"),
                    "media_type": b.get("media_type"),
                    "media_url": None,
                    "permalink": b.get("permalink"),
                    "comments_count": None,
                    "like_count": None,
                })

        if batch_sleep > 0:
            time.sleep(batch_sleep)

        if (i // batch_size + 1) % 5 == 0:
            print(f"detail batch done: {min(i+batch_size, len(base_rows))}/{len(base_rows)}")

    return detailed


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


def run_hashtag():
    api_version = os.getenv("FB_API_VERSION", "v24.0")
    ig_user_id = os.environ["IG_USER_ID"]
    access_token = os.environ["LONG_TOKEN"]

    bucket = os.environ["GCS_BUCKET"]
    dataset = os.environ["BQ_DATASET"]
    table = os.environ["BQ_TABLE"]

    hashtag_ids = [x.strip() for x in os.environ["HASHTAG_IDS"].split(",") if x.strip()]

    sleep_sec = float(os.getenv("TAG_SLEEP_SEC", "10"))

    for i, hashtag_id in enumerate(hashtag_ids):
        print(f"===== Processing {hashtag_id} =====")

        rows = fetch_all_recent_media(hashtag_id, ig_user_id, access_token, api_version)
        print(f"Fetched {len(rows)} rows")

        now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        enriched_rows = []
        for row in rows:
            row["fetched_at"] = now_iso
            row["hashtag_id"] = hashtag_id
            enriched_rows.append(row)

        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        blob_name = f"instagram/hashtag/{hashtag_id}/{now}.ndjson"

        upload_to_gcs(bucket, blob_name, enriched_rows)
        gcs_uri = f"gs://{bucket}/{blob_name}"
        load_to_bigquery(dataset, table, gcs_uri)

        # ★追加：次のハッシュタグへ行く前に待つ（最後は待たない）
        if i < len(hashtag_ids) - 1:
            print(f"Sleeping {sleep_sec}s before next hashtag...")
            time.sleep(sleep_sec)

    print("✅ All hashtags done")
