# auth.py

import hashlib
import requests
import logging
from datetime import datetime, timezone
from config import API_KEY, API_SECRET, USER_ID, ORG_ID, SESSION_URL

# — Logging setup —
logging.basicConfig(level=logging.INFO)


def compute_md5_hash(data: str) -> str:
    """Compute MD5 hash used for Filevine partner auth."""
    return hashlib.md5(data.encode("utf-8")).hexdigest()


def refresh_access_token() -> dict:
    """Request a new access token and session from Filevine."""
    # e.g. 2025-08-07T16:00:00.123Z
    api_timestamp = (
        datetime.now(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z"
    )
    api_hash = compute_md5_hash(
        f"{API_KEY}/{api_timestamp}/{API_SECRET}"
    )

    payload = {
        "mode":         "key",
        "apiKey":       API_KEY,
        "apiSecret":    API_SECRET,
        "apiHash":      api_hash,
        "apiTimestamp": api_timestamp,
        "userId":       USER_ID,
        "orgId":        ORG_ID,
    }

    try:
        logging.info("Requesting new access token from /session…")
        resp = requests.post(SESSION_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()
        logging.info("✅ Token refresh successful.")
        return {
            "access_token": data["accessToken"],
            "session_id":   data["refreshToken"],  # x-fv-sessionid
            "user_id":      data["userId"],
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Auth failed: {e}")
        raise


def get_dynamic_headers() -> dict:
    """Return headers with a fresh Filevine token and session."""
    auth = refresh_access_token()
    return {
        "Authorization":    f"Bearer {auth['access_token']}",
        "x-fv-userid":      str(auth["user_id"]),
        "x-fv-orgid":       ORG_ID,
        "x-fv-sessionid":   auth["session_id"],
        "Content-Type":     "application/json",
        "Accept":           "application/json",
    }


if __name__ == "__main__":
    # standalone test
    headers = get_dynamic_headers()
    print(headers)
