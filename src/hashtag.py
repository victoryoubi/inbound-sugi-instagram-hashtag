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
    Graph API GET with retries for 429/5xx and 'reduce amount' error.
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
        # Rate limit / transient errors / reduce message
        if r.status_code in (429, 500, 502, 503, 504) or REDUCE_MSG in txt:
            last_err = RuntimeError(f"Error {r.status_code}: {txt}")
            _backoff_sleep(attempt)
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

def _fetch_media_detail(media_id, access_token, api_version):
    """
    Stage 2: fetch full fields per media_id
    """
    url = f"https://graph.facebook.com/{api_version}/{media_id}"
    params = {
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "access_token": access_token,
    }
    return _get_json_with_retry(url, params=params, timeout=(10, 120), max_attempts=6)

def fetch_all_recent_media(hashtag_id, ig_user_id, access_token, api_version):
    """
    2段階取得の本体。シグネチャは元のまま。
    env:
      LIMIT: recent_media の 1ページ件数（推奨 10〜25）
      MAX_ITEMS: 1ハッシュタグの最大取得件数（要望どおり 500 のままでOK）
      DETAIL_SLEEP_SEC: 詳細取得の間隔（推奨 0.2〜0.5）
    """
    per_page = int(os.getenv("LIMIT", "10"))
    max_items = int(os.getenv("MAX_ITEMS", "500"))
    detail_sleep = float(os.getenv("DETAIL_SLEEP_SEC", "0.25"))

    # Stage 1: light list
    base_rows = _fetch_recent_media_light(
        hashtag_id=hashtag_id,
        ig_user_id=ig_user_id,
        access_token=access_token,
        api_version=api_version,
        per_page=per_page,
        max_items=max_items,
    )

    # Stage 2: per-id detail
    detailed = []
    for i, b in enumerate(base_rows, start=1):
        media_id = b["id"]
        try:
            d = _fetch_media_detail(media_id, access_token, api_version)
            detailed.append(d)
        except Exception as e:
            # ここは運用方針次第：落とさず続行（推奨）
            print(f"warn: detail fetch failed media_id={media_id}: {e}")
            # 最低限の情報は残す（後で再取得も可能）
            detailed.append(b)

        if detail_sleep > 0:
            time.sleep(detail_sleep)

        if i % 50 == 0:
            print(f"detail fetched: {i}/{len(base_rows)}")

    return detailed


def upload_to_gcs(bucket_name, blob_name, rows):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    ndjson = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    blob.upload_from_string(ndjson, content_type="application/json")


def load_to_bigquery(dataset_id, table_id, gcs_uri):
    client = bigquery.Client()
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )

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
