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


def plot_reliable_peers_by_country(rows, peer_country_rows):
    """Build a grouped bar chart for reliable peers by country at different thresholds."""
    thresholds = [0.9, 0.8, 0.7]
    plot_rows = []

    for threshold in thresholds:
        reliable_peers, _totals = aggregate_uptime_by_multi_hash(rows, threshold)
        country_counts = count_reliable_peers_by_country(reliable_peers, peer_country_rows)

        for country, count in country_counts.items():
            # Skip countries with very few reliable peers
            if count < 50:
                continue
            plot_rows.append(
                {
                    "country": country,
                    "count": count,
                    "threshold": str(threshold),
                }
            )

    fig = px.bar(
        plot_rows,
        x="country",
        y="count",
        color="threshold",
        barmode="group",
    )

    # Tweak overall font and legend position
    fig.update_layout(
        font=dict(
            family="Arial, sans-serif",
            size=18,
        ),
        legend=dict(
            title="threshold",
            x=0.99,
            y=0.99,
            xanchor="right",
            yanchor="top",
        ),
    )

    # Enlarge x and y axis fonts
    fig.update_xaxes(
        title=None,
        tickfont=dict(size=20),
        tickangle=-30,
    )
    fig.update_yaxes(
        title=None,
        tickfont=dict(size=20),
    )
    fig.show()
    # Save figure as PNG under report/pics
    project_root = Path(__file__).resolve().parents[2]
    output_path = project_root / "report" / "pics" / "peer_uptime_country.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(output_path), scale=2)


def main():
    """Fetch data and show reliable peers by country chart."""
    rows = fetch_uptime_duration()
    peer_country_rows = fetch_peer_id_prefix_by_country()
    plot_reliable_peers_by_country(rows, peer_country_rows)



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