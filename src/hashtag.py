import os
import requests

def fetch_recent_media(hashtag_id: str, ig_user_id: str, access_token: str, api_version: str, limit: int = 50) -> dict:
    url = f"https://graph.facebook.com/{api_version}/{hashtag_id}/recent_media"
    params = {
        "user_id": ig_user_id,
        "fields": "id,caption,timestamp,media_type,media_url,permalink,username",
        "limit": limit,
        "access_token": access_token,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

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
