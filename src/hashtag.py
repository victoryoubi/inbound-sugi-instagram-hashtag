import os
import re
import json
import requests
import random
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from google.cloud import storage
from google.cloud import bigquery

_TZ_NO_COLON = re.compile(r"([+-]\d{2})(\d{2})$")  # +0000 -> +00:00
REDUCE_MSG = "Please reduce the amount of data you're asking for"

# token/secret 系はキーとして来ても、URL文字列内に入ってても除去する
SENSITIVE_KEYS = {
    "access_token",
    "authorization",
    "refresh_token",
    "id_token",
    "token",
    "client_secret",
    "appsecret_proof",
}


def _utc_now_iso_z():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _backoff_sleep(attempt: int, base: float = 1.0, cap: float = 60.0):
    sec = min(cap, base * (2 ** attempt)) + random.random()
    time.sleep(sec)


def _strip_token_from_url(s: str) -> str:
    """
    URL文字列に含まれる access_token 等をクエリから除去する
    """
    try:
        parts = urlsplit(s)
        # URLっぽくなければそのまま
        if not parts.scheme or not parts.netloc:
            return s

        q = parse_qsl(parts.query, keep_blank_values=True)
        q2 = [(k, v) for (k, v) in q if str(k).lower() not in SENSITIVE_KEYS]
        new_query = urlencode(q2, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
    except Exception:
        return s


def _redact_sensitive(obj):
    """
    dict/list/str を再帰的に走査して
    - token系キーを削除
    - URL文字列に埋まった access_token 等を除去
    """
    if obj is None:
        return None

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k is None:
                continue
            kl = str(k).lower()
            if kl in SENSITIVE_KEYS:
                continue
            out[k] = _redact_sensitive(v)
        return out

    if isinstance(obj, list):
        return [_redact_sensitive(x) for x in obj]

    if isinstance(obj, str):
        return _strip_token_from_url(obj)

    return obj


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
            try:
                return r.json()
            except Exception as e:
                last_err = e
                _backoff_sleep(attempt)
                continue

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


def _fetch_recent_media_full(hashtag_id, ig_user_id, access_token, api_version, per_page, max_items):
    """
    Single-stage: /{hashtag_id}/recent_media with full fields (limit small + pagination)

    Returns:
      rows: list[dict]  (media table candidate rows)
      stage1_response: dict (snapshot raw pages: redacted)
    """
    url = f"https://graph.facebook.com/{api_version}/{hashtag_id}/recent_media"
    params = {
        "user_id": ig_user_id,
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "limit": per_page,
        "access_token": access_token,
    }

    all_rows = []
    pages = []
    page_count = 0

    while True:
        res = _get_json_with_retry(url, params=params, timeout=(10, 120), max_attempts=6)

        page_count += 1

        # スナップショット用は秘匿情報を除去した上で保存
        pages.append(_redact_sensitive(res))

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

    response = {
        "page_count": page_count,
        "returned_count": len(all_rows),
        "pages": pages,  # ← redacted済みの生レスポンス（丸ごと）
    }
    return all_rows, response


def fetch_recent_media_full_with_snapshots(hashtag_id, ig_user_id, access_token, api_version):
    """
    1段階取得（recent_media一発 + pagination）+ snapshot用 raw response を返す（BQ schema互換）
    """
    per_page = int(os.getenv("LIMIT", "10"))
    max_items = int(os.getenv("MAX_ITEMS", "500"))

    params = {
        "endpoint": f"/{hashtag_id}/recent_media",
        "hashtag_id": _to_int_or_none(hashtag_id),
        "user_id": str(ig_user_id),
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "limit": per_page,
        "max_items": max_items,
        # access_token は保存しない
    }

    rows, response = _fetch_recent_media_full(
        hashtag_id=hashtag_id,
        ig_user_id=ig_user_id,
        access_token=access_token,
        api_version=api_version,
        per_page=per_page,
        max_items=max_items,
    )

    return rows, params, response


def upload_to_gcs(bucket_name, blob_name, rows):
    if not rows:
        # 0件ならアップロードしない（空行NDJSON事故を防ぐ）
        return False

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    ndjson = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    # 末尾改行はあってもなくてもOKだが、付けるなら rows>0 の時だけ
    ndjson += "\n"

    blob.upload_from_string(ndjson, content_type="application/x-ndjson")
    return True


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

    # 行欠損の切り分け用（重要）
    print(f"[BQ] loaded to {table_ref} from {gcs_uri}")
    print(f"[BQ] output_rows: {load_job.output_rows}")
    if load_job.errors:
        print(f"[BQ] errors: {load_job.errors}")
    if load_job.error_result:
        print(f"[BQ] error_result: {load_job.error_result}")


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

    skipped = []

    for i, hashtag_id in enumerate(hashtag_ids):
        print(f"===== Processing {hashtag_id} =====")

        # run_id は「1 hashtag 1 run」で一意になるように
        run_id = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{hashtag_id}"

        collected_at_iso = _utc_now_iso_z()
        fetched_at_iso = collected_at_iso

        try:
            rows, params, response = fetch_recent_media_full_with_snapshots(
                hashtag_id, ig_user_id, access_token, api_version
            )
        except Exception as e:
            print(f"[SKIP] {hashtag_id} error: {e}")
            skipped.append(hashtag_id)
            if i < len(hashtag_ids) - 1:
                time.sleep(sleep_sec)
            continue

        print(f"Fetched {len(rows)} rows")

        # -----------------------------
        # mediaテーブル用
        # -----------------------------
        hid_int = _to_int_or_none(hashtag_id)

        enriched_rows = []
        for row in rows:
            out = dict(row) if isinstance(row, dict) else {}

            out["id"] = _to_int_or_none(out.get("id"))
            out["hashtag_id"] = hid_int
            out["comments_count"] = _to_int_or_none(out.get("comments_count"))
            out["like_count"] = _to_int_or_none(out.get("like_count"))
            out["timestamp"] = _to_ts_or_none(out.get("timestamp"))
            out["fetched_at"] = fetched_at_iso

            # 念のため、media行にもURL内tokenが混ざらないように（通常は無いが保険）
            enriched_rows.append(_redact_sensitive(out))

        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        media_blob_name = f"instagram/hashtag/{hashtag_id}/{now}.ndjson"
        uploaded = upload_to_gcs(bucket, media_blob_name, enriched_rows)
        
        if uploaded:
            media_gcs_uri = f"gs://{bucket}/{media_blob_name}"
            load_to_bigquery(dataset, media_table, media_gcs_uri)
        else:
            print("[SKIP] media rows=0 -> skip upload/load")

        # -----------------------------
        # snapshotsテーブル用
        # -----------------------------
        snapshot_rows = [{
            "run_id": run_id,
            "hashtag_id": _to_int_or_none(hashtag_id),
            "collected_at": collected_at_iso,
            "ig_user_id": str(ig_user_id),
            "api_version": str(api_version),
            "params": params,
            "response": response,
        }]

        snapshot_blob_name = f"instagram/hashtag_snapshots/{hashtag_id}/{run_id}.ndjson"
        uploaded = upload_to_gcs(bucket, snapshot_blob_name, snapshot_rows)
        if uploaded:
            snapshot_gcs_uri = f"gs://{bucket}/{snapshot_blob_name}"
            load_to_bigquery(dataset, snapshot_table, snapshot_gcs_uri)

        if i < len(hashtag_ids) - 1:
            print(f"Sleeping {sleep_sec}s before next hashtag...")
            time.sleep(sleep_sec)

    if skipped:
        print(f"[WARN] Skipped {len(skipped)} hashtags due to errors: {skipped}")
    print("All hashtags done")


if __name__ == "__main__":
    run_hashtag()
