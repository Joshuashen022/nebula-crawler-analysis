"""
Read all multi_hash from peers table; count those starting with Qm vs 12D3 and check for other prefixes.
"""
import sys
from pathlib import Path

from collections import Counter

# Allow running as script: python src/analysis/multi_hash_prefix_stats.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.peers.read_multi_hashes import fetch_all_multi_hashes


def get_prefix(multi_hash: str, length: int = 4) -> str:
    """Get prefix of multi_hash (default first 4 chars, to distinguish 12D3 from others)."""
    if not multi_hash:
        return ""
    return multi_hash[: min(length, len(multi_hash))]


def main():
    hashes = fetch_all_multi_hashes()
    total = len(hashes)

    count_qm = sum(1 for h in hashes if h and h.startswith("Qm"))
    count_12d3 = sum(1 for h in hashes if h and h.startswith("12D3"))

    # Count all prefixes (first 4 chars to distinguish 12D3 vs Qmxx etc.)
    prefix_counter = Counter(get_prefix(h, 4) for h in hashes if h)

    # Duplicate check
    hash_counter = Counter(hashes)
    unique_count = len(hash_counter)
    duplicates = {h: c for h, c in hash_counter.items() if c > 1}
    n_duplicated_hashes = len(duplicates)
    n_duplicate_occurrences = total - unique_count

    print("=== peers.multi_hash prefix stats ===\n")
    print(f"Total rows: {total:,}")
    print(f"  Starting with Qm:  {count_qm:,}, percentage: {count_qm / total * 100:.2f}%")
    print(f"  Starting with 12D3: {count_12d3:,} percentage: {count_12d3 / total * 100:.2f}%")
    print()
    print("--- Duplicate check ---")
    print(f"Unique count: {unique_count:,}")
    if n_duplicated_hashes == 0:
        print("No duplicate multi_hash.")
    else:
        print(f"Duplicates: {n_duplicated_hashes:,} multi_hashes appear more than once, {n_duplicate_occurrences:,} extra rows.")
        top_dup = sorted(duplicates.items(), key=lambda x: -x[1])[:5]
        print("Top 5 most duplicated:")
        for h, c in top_dup:
            snippet = (h[:20] + "…") if len(h) > 20 else h
            print(f"  {snippet!r}: {c} occurrences")
    print()

    # others = total - count_qm - count_12d3
    # if others > 0:
    #     print(f"Other prefix count: {others:,}\n")
    #     print("--- All prefix distribution (first 4 chars) ---")
    #     for prefix, cnt in prefix_counter.most_common():
    #         print(f"  {prefix!r}: {cnt:,}")
    # else:
    #     print("No other prefixes besides Qm and 12D3.")


if __name__ == "__main__":
    main()

# Total rows: 27,202
#   Starting with Qm:  1,872, percentage: 6.88%
#   Starting with 12D3: 25,329 percentage: 93.11%

# --- Duplicate check ---
# Unique count: 27,202