"""
Stats on neighbor_count tiers from neighbors_multihash output.
Counts distinct peerId (multi_hash) in three tiers: 1~9, 10~99, 100+;
reports min and max neighbor_count.
Plot: X = neighbor count, Y = number of nodes (distinct multi_hash per count).
"""
import sys
from pathlib import Path
from collections import defaultdict
from typing import Optional

import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.neighbors_multihash import fetch_neighbor_peer


def dedupe_by_multi_hash(rows):
    """Deduplicate by multi_hash, keeping the row with max neighbor_count per multi_hash."""
    best = {}
    for neighbor_count, multi_hash in rows:
        if multi_hash not in best or neighbor_count > best[multi_hash][0]:
            best[multi_hash] = (neighbor_count, multi_hash)
    return list(best.values())



def compute_neighbor_count_histogram(rows):
    """
    From list of (neighbor_count, multi_hash), build histogram:
    for each neighbor_count value, count distinct multi_hash (nodes).
    Returns (x_values, y_values) sorted by neighbor_count for plotting.
    """
    by_count = defaultdict(set)
    for neighbor_count, multi_hash in rows:
        if neighbor_count is None:
            continue
        by_count[neighbor_count].add(multi_hash)
    xs = sorted(by_count.keys())
    ys = [len(by_count[x]) for x in xs]
    return xs, ys


def plot_neighbor_count_vs_nodes(rows):
    """
    Plot X = neighbor count, Y = number of nodes (distinct multi_hash).
    """
    xs, ys = compute_neighbor_count_histogram(rows)
    fig = go.Figure(
        data=[go.Bar(x=xs, y=ys, name="nodes")],
        layout=go.Layout(
            xaxis=dict(
                tickfont=dict(size=40),
            ),
            yaxis=dict(
                tickfont=dict(size=40),
            ),
            showlegend=False,
        ),
    )
    out_path = Path(__file__).resolve().parents[2] / "report" / "pics" / "global_peer_neighbour.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(out_path), width=1600, height=800, scale=2)
    # fig.show()


def main():
    rows_raw = fetch_neighbor_peer()
    rows = dedupe_by_multi_hash(rows_raw)
    
    # Plot: X = neighbor count, Y = number of nodes
    plot_neighbor_count_vs_nodes(rows=rows)


if __name__ == "__main__":
    main()
