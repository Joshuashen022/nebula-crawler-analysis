"""
Aggregate crawler_track_duration and peer_actual_uptime per multi_hash,
and print simple statistics about their relative distributions.
"""
import sys
from pathlib import Path
from collections import defaultdict
from datetime import timedelta

import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.sessions.read_uptime_duration import fetch_uptime_duration


def aggregate_uptime_by_multi_hash(rows):
    """
    Group rows by multi_hash and sum crawler_track_duration and peer_actual_uptime.
    Returns list of (multi_hash, total_crawler_track_duration, total_peer_actual_uptime).
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

    result = [
        (mh, data["crawler"], data["peer"])
        for mh, data in totals.items()
    ]

    # Sort by total peer_actual_uptime desc, then by multi_hash
    result.sort(key=lambda x: (-x[2].total_seconds(), x[0] or ""))
    return result


def build_percentage_distributions(aggregated):
    """
    Build 5%-bucket distribution for peer/crawler uptime ratio.

    Ratio is defined as peer_sec / crawler_sec.
    Result is two lists: bin labels and percentage of entries per bin.
    """
    # 0-5%, 5-10%, ..., 95-100% -> 20 bins
    num_bins = 20
    bin_counts = [0] * num_bins
    total_entries = 0

    for _mh, crawler_td, peer_td in aggregated:
        crawler_sec = crawler_td.total_seconds()
        peer_sec = peer_td.total_seconds()

        # Skip entries where crawler has no uptime
        if crawler_sec <= 0:
            continue

        ratio = peer_sec / crawler_sec

        if ratio > 1:
            print(f"Ratio is greater than 1: {ratio}")
            raise Exception("Ratio is greater than 1")
        # Map ratio to 5%-wide bins based on percentage
        pct = ratio * 100
        bin_index = min(int(pct // 5), num_bins - 1)

        bin_counts[bin_index] += 1
        total_entries += 1

    bin_labels = [f"{i*5}-{(i+1)*5}%" for i in range(num_bins)]

    # if total_entries == 0:
    #     bin_percentages = [0.0] * num_bins
    # else:
    #     bin_percentages = [count / total_entries for count in bin_counts]

    return bin_labels, bin_counts


def plot_ratio_histogram(bin_labels, bin_counts):
    """
    Plot bar chart for peer/crawler ratio distribution using plotly.
    """
    # Draw bars at bin centers (2.5, 7.5, ..., 97.5) so that x-axis ticks at
    # 5%, 10%, ..., 100% fall between two adjacent bars.
    bin_centers = [i * 5 + 2.5 for i in range(len(bin_counts))]
    tick_vals = list(range(5, 101, 5))
    tick_text = [f"{v}%" for v in tick_vals]

    fig = go.Figure(
        data=[
            go.Bar(
                x=bin_centers,
                y=bin_counts,
                width=5,
            )
        ]
    )
    fig.update_layout(
        title=None,
        xaxis=dict(
            title=None,
            tickmode="array",
            tickvals=tick_vals,
            ticktext=tick_text,
            range=[0, 100],
            tickfont=dict(size=16),
            tickangle=-30,
        ),
        yaxis=dict(
            title=None,
            tickfont=dict(size=30),
        ),
        margin=dict(l=80, r=30, t=80, b=80),
    )

    # Save figure as PNG under report/pics
    project_root = Path(__file__).resolve().parents[2]
    output_path = project_root / "report" / "pics" / "peer_uptime_percentage.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(output_path), scale=2)
    fig.show()


def get_uptime_percentage_distributions():
    rows = fetch_uptime_duration()
    aggregated = aggregate_uptime_by_multi_hash(rows)
    bin_labels, bin_counts = build_percentage_distributions(aggregated)
    return bin_labels, bin_counts

def main():
    """Fetch data and print uptime percentage distributions."""

    bin_labels, bin_counts = get_uptime_percentage_distributions()

    print("Peer/Crawler uptime ratio distribution in 5% bins")
    print("Bin\tCount")
    for label, count in zip(bin_labels, bin_counts):
        print(f"{label}\t{count}")

    # Also show bar chart using plotly
    plot_ratio_histogram(bin_labels, bin_counts)


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