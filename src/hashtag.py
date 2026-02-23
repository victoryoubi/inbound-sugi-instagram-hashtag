import os
import time
import requests

RETRY_STATUS = {500, 502, 503, 504}

def fetch_recent_media(hashtag_id: str, ig_user_id: str, access_token: str, api_version: str, limit: int = 50) -> dict:
    url = f"https://graph.facebook.com/{api_version}/{hashtag_id}/recent_media"
    params = {
        "user_id": ig_user_id,
        "fields": "id,caption,timestamp,media_type,media_url,permalink,comments_count,like_count",
        "limit": limit,
        "access_token": access_token,
    }

    for attempt in range(1, 6):  # 最大5回
        r = requests.get(url, params=params, timeout=30)

        if r.status_code == 200:
            return r.json()

        # ここ重要：トークンはマスクしてログに出す
        safe_params = dict(params)
        safe_params["access_token"] = "***"

        print(f"[recent_media] failed status={r.status_code} attempt={attempt}")
        print(f"url={url} params={safe_params}")
        print(f"body={r.text}")

        # 5xxだけリトライ
        if r.status_code in RETRY_STATUS:
            time.sleep(min(10, 2 ** (attempt - 1)))  # 1,2,4,8,10秒
            continue

        # 4xxなどは即エラー（権限/ID/制限）
        raise RuntimeError(f"recent_media error {r.status_code}: {r.text}")

    raise RuntimeError("recent_media failed after retries (5xx)")

def run_hashtag():
    api_version = os.getenv("FB_API_VERSION", "v24.0")
    ig_user_id = os.environ["IG_USER_ID"]
    hashtag_id = os.environ["HASHTAG_ID"]
    access_token = os.environ["LONG_TOKEN"]

    res = fetch_recent_media(hashtag_id, ig_user_id, access_token, api_version, limit=50)
    data = res.get("data", [])

    print(f"✅ recent_media fetched. count={len(data)}")
    # 先頭3件だけログに出す（captionは長いので一部だけ）
    for i, row in enumerate(data[:3]):
        cap = (row.get("caption") or "").replace("\n", " ")
        if len(cap) > 120:
            cap = cap[:120] + "..."
        print(f"[{i}] id={row.get('id')} type={row.get('media_type')} time={row.get('timestamp')} user={row.get('username')}")
        print(f"     permalink={row.get('permalink')}")
        print(f"     caption={cap}")

    # ページング確認（次があるか）
    paging = res.get("paging", {})
    next_url = paging.get("next")
    print("paging.next:", next_url if next_url else "(none)")
