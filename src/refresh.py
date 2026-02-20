import os
import requests
from google.cloud import secretmanager

def exchange_to_long_lived(app_id: str, app_secret: str, token: str, api_version: str) -> dict:
    url = f"https://graph.facebook.com/{api_version}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": token,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def add_secret_version(project_id: str, secret_name: str, secret_value: str) -> None:
    client = secretmanager.SecretManagerServiceClient()
    parent = client.secret_path(project_id, secret_name)
    client.add_secret_version(
        request={"parent": parent, "payload": {"data": secret_value.encode("utf-8")}}
    )

def run_refresh():
    project_id = os.environ["PROJECT_ID"]
    secret_name = os.environ.get("TOKEN_SECRET_NAME", "ig-long-token")
    api_version = os.environ.get("FB_API_VERSION", "v24.0")

    long_token = os.environ["LONG_TOKEN"]
    app_id = os.environ["FB_APP_ID"]
    app_secret = os.environ["FB_APP_SECRET"]

    new_json = exchange_to_long_lived(app_id, app_secret, long_token, api_version)
    new_token = new_json.get("access_token")
    expires_in = new_json.get("expires_in")

    if not new_token:
        raise RuntimeError(f"Token refresh failed: {new_json}")

    add_secret_version(project_id, secret_name, new_token)

    print("✅ Token refreshed and stored to Secret Manager.")
    print("expires_in:", expires_in)
