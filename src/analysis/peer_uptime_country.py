"""
Aggregate crawler_track_duration and peer_actual_uptime per multi_hash,
and print simple statistics about their relative distributions.
"""
import sys
from pathlib import Path
from collections import defaultdict
from datetime import timedelta

import plotly.express as px

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.sessions.read_uptime_duration import fetch_uptime_duration
from src.dbs.multi_hash_prefix_country import fetch_peer_id_prefix_by_country


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


def count_reliable_peers_by_country(reliable_peers, peer_country_rows):
    """Match reliable peers to countries and count reliable peers per country."""
    reliable_ids = {multi_hash for multi_hash, _percentage in reliable_peers}
    country_counts = defaultdict(int)

    for multi_hash, country in peer_country_rows:
        if not country:
            continue
        if multi_hash in reliable_ids:
            country_counts[country] += 1

    # Return a plain dict sorted by count desc, then country code
    return dict(sorted(country_counts.items(), key=lambda x: (-x[1], x[0])))


def main():
    """Fetch data and print uptime percentage distributions."""
    rows = fetch_uptime_duration()
    reliable_peers, totals = aggregate_uptime_by_multi_hash(rows, 0.9)
    peer_country_rows = fetch_peer_id_prefix_by_country()
    country_counts = count_reliable_peers_by_country(reliable_peers, peer_country_rows)

    print(f"Total peers with uptime data: {len(totals)}")
    print(f"Reliable peers (ratio > 0.9): {len(reliable_peers)}")
    print("Reliable peers by country (count):")
    for country, count in country_counts.items():
        print(f"{country}\t{count}")



if __name__ == "__main__":
    main()

# Bin     Count
# 0-5%    548
# 5-10%   70
# 10-15%  66
# 15-20%  68
# 20-25%  45
# 25-30%  92
# 30-35%  84
# 35-40%  101
# 40-45%  92
# 45-50%  121
# 50-55%  149
# 55-60%  142
# 60-65%  146
# 65-70%  177
# 70-75%  266
# 75-80%  473
# 80-85%  970
# 85-90%  1082
# 90-95%  789
# 95-100% 594