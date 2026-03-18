"""
Read crawl summary JSON files and plot peer counts over time.
"""
import json
import sys
from pathlib import Path
from typing import Dict, Union

import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


def load_crawl_stats(results_dir: Union[str, Path]) -> Dict[str, Dict[str, int]]:
    """
    Read all *crawl.json files under results_dir and collect stats.

    Only records entries where state == "succeeded".
    Returns a dict keyed by started_at ISO string, preserving chronological order.
    """
    base = Path(results_dir)
    stats: Dict[str, Dict[str, int]] = {}

    # Sort by filename to roughly follow time order
    for path in sorted(base.glob("*crawl.json")):
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("state") != "succeeded":
            continue
        started_at = data.get("started_at")
        if not started_at:
            continue
        crawled_peers = int(data.get("crawled_peers", 0))
        dialable_peers = int(data.get("dialable_peers", 0))
        undialable_peers = int(data.get("undialable_peers", 0))
        if crawled_peers <= 10 or dialable_peers <= 10 or undialable_peers <= 10:
            continue

        stats[started_at] = {
            "crawled_peers": crawled_peers,
            "dialable_peers": dialable_peers,
            "undialable_peers": undialable_peers,
        }
    return stats


def plot_crawl_peers_over_time(
    stats: Dict[str, Dict[str, int]],
):
    """
    Plot three line series:
    1) (started_at, crawled_peers)
    2) (started_at, dialable_peers)
    3) (started_at, undialable_peers)
    """
    if not stats:
        raise ValueError("No crawl stats to plot.")

    xs = list(stats.keys())
    crawled = [v["crawled_peers"] for v in stats.values()]
    dialable = [v["dialable_peers"] for v in stats.values()]
    undialable = [v["undialable_peers"] for v in stats.values()]

    fig = go.Figure()
    fig.add_scatter(x=xs, y=crawled, mode="lines+markers", name="crawled")
    fig.add_scatter(x=xs, y=dialable, mode="lines+markers", name="dialable")
    fig.add_scatter(
        x=xs,
        y=undialable,
        mode="lines+markers",
        name="undialable",
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        legend_title="Series",
        margin=dict(l=60, r=40, t=40, b=80),
        # Slightly smaller base font so legend and axis labels
        # take less vertical space and better match table height.
        font=dict(size=14),
        xaxis=dict(
            title_font=dict(size=20),
            tickfont=dict(size=20),
        ),
        yaxis=dict(
            title_font=dict(size=20),
            tickfont=dict(size=20),
        ),
        legend=dict(
            # Make legend text more compact
            font=dict(size=20),
            # Keep items tightly packed vertically
            tracegroupgap=0,
            y=0.5,
            yanchor="middle",
        ),
    )

    out_path = Path(__file__).resolve().parents[2] / "report" / "pics" / "global_each_crawl.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(out_path), width=800, height=400, scale=2)
    fig.show()


def main():
    """Main entry: load stats and show figure."""
    results_dir = Path(__file__).resolve().parent.parent / "results"
    stats = load_crawl_stats(results_dir)
    plot_crawl_peers_over_time(stats)


if __name__ == "__main__":
    main()

