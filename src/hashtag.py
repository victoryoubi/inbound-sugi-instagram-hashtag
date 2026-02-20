import os
import requests

FB_API_VERSION = os.getenv("FB_API_VERSION", "v24.0")

def ig_hashtag_search(ig_user_id: str, q: str, access_token: str) -> dict:
    url = f"https://graph.facebook.com/{FB_API_VERSION}/ig_hashtag_search"
    params = {
        "user_id": ig_user_id,
        "q": q,  # 例: "일본"（#は付けない）
        "access_token": access_token,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    ig_user_id = os.environ["IG_USER_ID"]
    token = os.environ["LONG_TOKEN"]

    q = "일본"
    res = ig_hashtag_search(ig_user_id, q, token)

    data = res.get("data", [])
    if not data:
        raise RuntimeError(f"No hashtag found for q={q}. response={res}")

    hashtag_id = data[0]["id"]
    print("HASHTAG:", q)
    print("HASHTAG_ID:", hashtag_id)
    # 必要なら name も返ってくる時があります（API仕様/権限による）
    print("RAW:", res)

if __name__ == "__main__":
    main()
