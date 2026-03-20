#!/usr/bin/env python3
"""GET http://8.216.32.203:8080/geographical with Bearer auth."""

import json
import os
import sys
import urllib.request
from collections import Counter

from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://8.216.32.203:8080"
GEOGRAPHICAL_PATH = "/global-geographical"
# Override with env GEOGRAPHICAL_AUTH_TOKEN if set
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "empty")

BUCKET_ORDER = ["0–9", "10–99", "100–999", "1k–9,999", "10k+"]


def get_remote_data(path: str) -> dict:
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {AUTH_TOKEN}")
    with urllib.request.urlopen(req) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
        if not raw.get("ok"):
            print("API returned ok=False", file=sys.stderr)
            return
        if raw.get("service") != "geographical":
            print("Unexpected service:", raw.get("service"), file=sys.stderr)
        data = raw.get("data")
        if not data or not data.get("country"):
            print("No geographical data (empty country list).")
            return
        return data



def main() -> None:
    raw = get_remote_data(GEOGRAPHICAL_PATH)
    print(raw)


if __name__ == "__main__":
    main()
