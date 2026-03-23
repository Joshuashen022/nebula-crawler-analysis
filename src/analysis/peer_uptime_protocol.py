"""
Aggregate crawler_track_duration and peer_actual_uptime per multi_hash, then
count how many highly reliable peers use each protocol (similar to
peer_uptime_country, but grouped by protocol instead of country).
"""
import sys
from pathlib import Path
from collections import defaultdict
from datetime import timedelta

import plotly.express as px

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.sessions.read_uptime_duration import fetch_uptime_duration
from src.dbs.protocol_peer_count import fetch_protocol_peer_count
from src.api.get_remote_data import get_remote_data
        
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


def count_reliable_peers_by_protocol(reliable_peers, protocol_rows):
    """
    Match reliable peers to protocols and count reliable peers per protocol.
    protocol_rows: list of (protocol, multi_hash).
    """
    reliable_ids = {multi_hash for multi_hash, _percentage in reliable_peers}

    # Build mapping multi_hash -> set of protocols (peers can speak multiple protocols).
    mh_to_protocols: dict[str, set[str]] = defaultdict(set)
    for protocol, multi_hash in protocol_rows:
        if not multi_hash or not protocol:
            continue
        mh_to_protocols[multi_hash].add(protocol)

    protocol_counts = defaultdict(int)
    for mh in reliable_ids:
        for protocol in mh_to_protocols.get(mh, []):
            protocol_counts[protocol] += 1

    # Return a plain dict sorted by count desc, then protocol string
    return dict(sorted(protocol_counts.items(), key=lambda x: (-x[1], x[0])))


def plot_reliable_peers_by_protocol(rows, protocol_rows):
    """Build a grouped bar chart for reliable peers by protocol at different thresholds."""
    thresholds = [0.9, 0.8, 0.7]
    plot_rows = []

    for threshold in thresholds:
        reliable_peers, _totals = aggregate_uptime_by_multi_hash(rows, threshold)
        protocol_counts = count_reliable_peers_by_protocol(reliable_peers, protocol_rows)

        for protocol, count in protocol_counts.items():
            # Skip protocols with very few reliable peers
            if count < 50:
                continue
            plot_rows.append(
                {
                    "protocol": protocol,
                    "count": count,
                    "threshold": str(threshold),
                }
            )

    if not plot_rows:
        print("No protocols with sufficient reliable peers at given thresholds.")
        return

    fig = px.bar(
        plot_rows,
        x="protocol",
        y="count",
        color="threshold",
        barmode="group",
        title=None,
    )
    fig.update_layout(
        title=None,
        xaxis=dict(
            title=None,
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
    output_path = project_root / "report" / "pics" / "peer_uptime_protocol.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(output_path), scale=2)
    fig.show()


def get_reliable_protocol_counts(threshold: float = 0.9):
    """
    Print (protocol, count) for peers whose uptime ratio exceeds `threshold`,
    sorted from highest count to lowest.
    """
    rows = fetch_uptime_duration()
    protocol_rows = fetch_protocol_peer_count()

    reliable_peers, _totals = aggregate_uptime_by_multi_hash(rows, threshold)
    protocol_counts = count_reliable_peers_by_protocol(reliable_peers, protocol_rows)
    return protocol_counts


def main():
    """Fetch data and show reliable peers by protocol chart."""
    # rows = fetch_uptime_duration()
    # protocol_rows = fetch_protocol_peer_count()
    # plot_reliable_peers_by_protocol(rows, protocol_rows)
    protocol_counts = get_reliable_protocol_counts(0.9) 
    print("=== Reliable protocol counts at threshold 0.9 ===")
    print("Count\tProtocol")
    print("-" * 20)
    for protocol, count in protocol_counts.items():
        print(f"{count}\t{protocol}")
    print("-" * 20)

def remote_main():
    protocol_counts = get_remote_data("/reliable-protocol-counts")
    print("=== Reliable protocol counts at threshold 0.9 ===")
    print("Count\tProtocol")
    print("-" * 20)
    for protocol, count in protocol_counts.items():
        print(f"{count}\t{protocol}")
    print("-" * 20)

if __name__ == "__main__":
    remote_main()


# local
# 4841    /ipfs/ping/1.0.0
# 4837    /ipfs/id/1.0.0
# 4834    /ipfs/id/push/1.0.0
# 4360    /ipfs/kad/1.0.0
# 4339    /libp2p/autonat/1.0.0
# 4312    /libp2p/circuit/relay/0.2.0/stop
# 4294    /ipfs/bitswap/1.0.0
# 4294    /ipfs/bitswap/1.1.0
# 4291    /ipfs/bitswap
# 4280    /x/
# 4249    /ipfs/bitswap/1.2.0
# 4012    /libp2p/dcutr
# 4000    /ipfs/lan/kad/1.0.0
# 3681    /libp2p/circuit/relay/0.2.0/hop
# 2027    /libp2p/autonat/2/dial-back
# 2027    /libp2p/autonat/2/dial-request
# 1154    /libp2p/circuit/relay/0.1.0
# 1108    /p2p/id/delta/1.0.0
# 344     /meshsub/1.0.0
# 343     /meshsub/1.1.0
# 342     /floodsub/1.0.0
# 341     /sbptp/1.0.0
# 221     /meshsub/1.2.0
# 101     /libp2p/fetch/0.0.1
# 47      /meshsub/1.3.0
# 35      /sfst/1.0.0
# 20      /sfst/2.0.0


# remote
# Count   Protocol
# --------------------
# 2123    /ipfs/ping/1.0.0
# 2119    /ipfs/id/1.0.0
# 2112    /ipfs/id/push/1.0.0
# 1965    /libp2p/circuit/relay/0.2.0/stop
# 1895    /ipfs/bitswap/1.0.0
# 1895    /ipfs/bitswap/1.1.0
# 1891    /ipfs/bitswap/1.2.0
# 1888    /ipfs/bitswap
# 1883    /ipfs/kad/1.0.0
# 1881    /x/
# 1867    /libp2p/autonat/1.0.0
# 1829    /libp2p/dcutr
# 1766    /ipfs/lan/kad/1.0.0
# 1616    /libp2p/circuit/relay/0.2.0/hop
# 1247    /libp2p/autonat/2/dial-back
# 1247    /libp2p/autonat/2/dial-request
# 366     /libp2p/circuit/relay/0.1.0
# 361     /p2p/id/delta/1.0.0
# 210     /floodsub/1.0.0
# 210     /meshsub/1.0.0
# 210     /meshsub/1.1.0
# 179     /meshsub/1.2.0
# 113     /sbptp/1.0.0
# 58      /libp2p/fetch/0.0.1
# 41      /meshsub/1.3.0
# 13      /http/1.1
# 11      /edgevpn/service/0.1
# 8       /sfst/2.0.0