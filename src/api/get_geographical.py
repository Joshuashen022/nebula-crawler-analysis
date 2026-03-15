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
GEOGRAPHICAL_PATH = "/geographical"
# Override with env GEOGRAPHICAL_AUTH_TOKEN if set
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "empty")

BUCKET_ORDER = ["0–9", "10–99", "100–999", "1k–9,999", "10k+"]


def get_geographical() -> dict:
    url = f"{BASE_URL}{GEOGRAPHICAL_PATH}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {AUTH_TOKEN}")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def analyze_geographical_response(raw: dict) -> None:
    """Parse and analyze API response; same shape as geographical.py fetch_geographical_data()."""
    if not raw.get("ok"):
        print("API returned ok=False", file=sys.stderr)
        return
    if raw.get("service") != "geographical":
        print("Unexpected service:", raw.get("service"), file=sys.stderr)
    data = raw.get("data")
    if not data or not data.get("country"):
        print("No geographical data (empty country list).")
        return

    countries = data["country"]
    counts = data["count"]
    buckets = data.get("bucket") or []

    total_addresses = sum(counts)
    n_countries = len(countries)

    print("=== Geographical API Analysis ===\n")
    print(f"Countries/regions: {n_countries}")
    print(f"Total multi-addresses: {total_addresses:,}\n")

    print("--- Top 15 by count ---")
    for i, (c, n) in enumerate(zip(countries, counts), 1):
        if i > 15:
            break
        bucket = buckets[i - 1] if i <= len(buckets) else ""
        print(f"  {i:2}. {c}: {n:,}  ({bucket})")

    if buckets:
        print("\n--- Bucket distribution ---")
        bucket_counts = Counter(buckets)
        for b in BUCKET_ORDER:
            if b in bucket_counts:
                print(f"  {b}: {bucket_counts[b]} countries/regions")


def show_choropleth(raw: dict) -> None:
    """Plot choropleth from API data (same as geographical.py main())."""
    import plotly.express as px

    if not raw.get("ok") or not raw.get("data", {}).get("country"):
        print("No data to plot.", file=sys.stderr)
        return
    data = raw["data"]
    fig = px.choropleth(
        data,
        locations="country",
        color="bucket",
        locationmode="ISO-3",
        title="Multi addresses per country (bucketed)",
        hover_name="country",
        hover_data={"count": True, "bucket": True},
        category_orders={"bucket": BUCKET_ORDER},
        color_discrete_map={
            "0–9": "#f7fbff",
            "10–99": "#c6dbef",
            "100–999": "#6baed6",
            "1k–9,999": "#2171b5",
            "10k+": "#08306b",
        },
    )
    fig.show()


def main() -> None:
    raw = get_geographical()
    analyze_geographical_response(raw)
    show_choropleth(raw)


if __name__ == "__main__":
    main()
