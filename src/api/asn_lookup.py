#!/usr/bin/env python3
"""
Look up ASN data: check database/asn.json first, fetch from Caida ASRank API if missing, then save.
Usage: python -m src.asn_lookup <asn_number>
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

ASN_DB_PATH = Path(__file__).resolve().parent.parent.parent / "database" / "asn.json"
ASRANK_API = "https://api.asrank.caida.org/v2/restful/asns"


def load_asn_db() -> list:
    """Load existing ASN records from database/asn.json."""
    if not ASN_DB_PATH.exists():
        return []
    with open(ASN_DB_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]


def save_asn_db(records: list) -> None:
    """Save ASN records to database/asn.json."""
    ASN_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ASN_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=4)


def find_asn_in_db(records: list, asn: str) -> Optional[dict]:
    """Return the record for this ASN if present (match on data.asn.asn)."""
    asn_str = str(asn)
    for rec in records:
        try:
            if rec.get("data", {}).get("asn", {}).get("asn") == asn_str:
                return rec
        except (AttributeError, TypeError):
            continue
    return None


def fetch_asn_from_api(asn: str) -> dict:
    """Fetch ASN data from Caida ASRank API."""
    url = f"{ASRANK_API}/{asn}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def lookup_asn(asn: str) -> dict:
    """Get ASN data from DB or API, and persist to DB if fetched."""
    records = load_asn_db()
    existing = find_asn_in_db(records, asn)

    if existing is not None:
        return existing

    data = fetch_asn_from_api(asn)
    records.append(data)
    save_asn_db(records)
    return data


def main() -> None:
    try:
        result = lookup_asn(10)
        print(json.dumps(result, indent=2))
    except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
        print(f"API error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
