"""
Aggregate crawler_track_duration and peer_actual_uptime per multi_hash,
and print simple statistics about their relative distributions.
"""
import sys
from pathlib import Path
from collections import defaultdict
from datetime import timedelta
from typing import Optional

import plotly.express as px

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.sessions.read_uptime_duration import fetch_uptime_duration
from src.dbs.multi_hash_prefix_asn import fetch_peer_id_prefix_by_asn
from src.api.asn_lookup import lookup_asn


def aggregate_uptime_by_multi_hash(rows, threshold: float = 0.8):
    """
    Group rows by multi_hash and sum crawler_track_duration and peer_actual_uptime.
    Returns list of (multi_hash, percentage) where
    (peer_actual_uptime / crawler_track_duration) > threshold.
    """
    totals = defaultdict(lambda: {"crawler": timedelta(0), "peer": timedelta(0)})

    for (
        multi_hash,
        _first_successful_visit,
        _updated_at,
        crawler_track_duration,
        _uptime_range,
        peer_actual_uptime,
    ) in rows:
        if crawler_track_duration is not None:
            totals[multi_hash]["crawler"] += crawler_track_duration
        if peer_actual_uptime is not None:
            totals[multi_hash]["peer"] += peer_actual_uptime

    result = []
    for mh, data in totals.items():
        crawler = data["crawler"]
        peer = data["peer"]
        if crawler.total_seconds() == 0:
            continue
        percentage = peer.total_seconds() / crawler.total_seconds()
        if percentage > threshold:
            result.append((mh, percentage))

    # Sort by percentage desc, then by multi_hash
    result.sort(key=lambda x: (-x[1], x[0] or ""))
    return result, totals

def get_rank(asn) -> Optional[int]:
    """Get ASN rank from DB/API (data.asn.rank). Returns None if asn is None or rank missing."""
    if asn is None:
        return None
    try:
        rec = lookup_asn(str(asn))
        return rec.get("data", {}).get("asn", {}).get("rank")
    except Exception:
        return None


def count_reliable_peers_by_asn(reliable_peers, peer_asn_rows):
    """
    Match reliable peers to ASNs and count reliable peers per ASN.
    Returns a dict {asn: count}.
    """
    reliable_ids = {multi_hash for multi_hash, _percentage in reliable_peers}
    asn_counts = defaultdict(int)

    for multi_hash, asn in peer_asn_rows:
        if multi_hash not in reliable_ids:
            continue
        if asn is None:
            continue
        asn_counts[asn] += 1

    return dict(asn_counts)


def add_rank_to_asn_counts(asn_counts, min_count: int = 1):
    """
    Take {asn: count} and aggregate counts by ASN rank using get_rank().
    Returns a dict {rank: count}, sorted by ascending rank.
    """
    result = []
    for asn, count in asn_counts.items():
        # Apply the "min_count" rule *before* calling get_rank(asn)
        # to avoid unnecessary API/DB lookups and respect rate limits.
        if count < min_count:
            continue
        rank = get_rank(asn)
        if rank is None:
            continue
        result.append((asn, rank, count))

    return result


def main():
    """Fetch data and show reliable peers by ASN rank chart."""
    rows = fetch_uptime_duration()
    peer_asn_rows = fetch_peer_id_prefix_by_asn()
    
    threshold = 0.9
    reliable_peers, _totals = aggregate_uptime_by_multi_hash(rows, threshold)
    asn_counts = count_reliable_peers_by_asn(reliable_peers, peer_asn_rows)

    # Display ASNs sorted by reliable peer count (descending)
    sorted_asn_counts = sorted(asn_counts.items(), key=lambda x: x[1], reverse=True)
    index = 0
    for asn, count in sorted_asn_counts:
        if index < 10:
            print(f"{index}\t{asn}\t{count}")
            index += 1
            continue
        break

    # Calculate total count and top-10 sum
    total_count = sum(count for _asn, count in sorted_asn_counts)
    top_10_sum = sum(count for _asn, count in sorted_asn_counts[:10])

    print()
    print(f"Total count: {total_count}")
    print(f"Top 10 sum: {top_10_sum}")
    if total_count > 0:
        share = top_10_sum / total_count * 100
        print(f"Top 10 share: {share:.2f}%")


if __name__ == "__main__":
    main()

# 0       20473   216
# 1       24940   162
# 2       51167   95
# 3       14061   94
# 4       16276   86
# 5       16509   50
# 6       40021   47
# 7       21928   35
# 8       63949   33
# 9       57043   29

# Total count: 1495
# Top 10 sum: 847
# Top 10 share: 56.66%