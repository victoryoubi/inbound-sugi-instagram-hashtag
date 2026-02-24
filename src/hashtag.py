import os
import json
import requests
import time
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery


def fetch_all_recent_media(hashtag_id, ig_user_id, access_token, api_version):
    url = f"https://graph.facebook.com/{api_version}/{hashtag_id}/recent_media"

    per_page = int(os.getenv("LIMIT", "25"))          
    max_items = int(os.getenv("MAX_ITEMS", "200"))    

    params = {
        "user_id": ig_user_id,
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "limit": per_page,
        "access_token": access_token,
    }

    all_rows = []

    while True:
        r = requests.get(url, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Error {r.status_code}: {r.text}")

        res = r.json()
        data = res.get("data", [])
        all_rows.extend(data)

        # ✅ 上限に達したら打ち切り
        if len(all_rows) >= max_items:
            all_rows = all_rows[:max_items]
            break

        next_url = res.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None  # nextはURLにtoken等含む

    return all_rows


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


import time

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
