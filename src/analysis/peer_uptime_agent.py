"""
Aggregate crawler_track_duration and peer_actual_uptime per multi_hash, then
count how many highly reliable peers each agent_version has (similar to
peer_uptime_country, but grouped by agent_version instead of country).
"""
import sys
from pathlib import Path
from collections import defaultdict
from datetime import timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.sessions.read_uptime_duration import fetch_uptime_duration
from src.dbs.agent_peer_count import fetch_agent_peer_count
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


def count_reliable_peers_by_agent(reliable_peers, agent_rows):
    """Match reliable peers to agents and count reliable peers per agent_version."""
    reliable_ids = {multi_hash for multi_hash, _percentage in reliable_peers}

    # Build mapping multi_hash -> agent_version
    # agent_rows: list of (agent_version, multi_hash)
    mh_to_agent = {}
    for agent, multi_hash in agent_rows:
        if not multi_hash:
            continue
        if not agent:
            continue
        # If a multi_hash is seen with multiple agents, keep the first one.
        mh_to_agent.setdefault(multi_hash, agent)

    agent_counts = defaultdict(int)
    for mh in reliable_ids:
        agent = mh_to_agent.get(mh)
        if not agent:
            continue
        agent_counts[agent] += 1

    # Return a plain dict sorted by count desc, then agent string
    return dict(sorted(agent_counts.items(), key=lambda x: (-x[1], x[0])))


def plot_reliable_peers_by_agent(rows, agent_rows):
    """Build a grouped bar chart for reliable peers by agent at different thresholds."""
    try:
        import plotly.express as px
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "Plotly is required for plotting. Install it (e.g. `pip install plotly`) "
            "or run `print_reliable_agent_counts()` for console output only."
        ) from e

    thresholds = [0.9, 0.8, 0.7]
    plot_rows = []


    for threshold in thresholds:
        reliable_peers, _totals = aggregate_uptime_by_multi_hash(rows, threshold)
        agent_counts = count_reliable_peers_by_agent(reliable_peers, agent_rows)


        for agent, count in agent_counts.items():
            # Skip agents with very few reliable peers
            if count < 50:
                continue
            plot_rows.append(
                {
                    "agent": agent,
                    "count": count,
                    "threshold": str(threshold),
                }
            )

    if not plot_rows:
        print("No agents with sufficient reliable peers at given thresholds.")
        return

    fig = px.bar(
        plot_rows,
        x="agent",
        y="count",
        color="threshold",
        barmode="group",
        title="Reliable peers by agent at different uptime thresholds",
    )
    fig.update_layout(xaxis_tickangle=-45)
    # Save figure as PNG under report/pics
    project_root = Path(__file__).resolve().parents[2]
    output_path = project_root / "report" / "pics" / "peer_uptime_agent.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(output_path), scale=2)
    fig.show()

def get_reliable_agent_counts(threshold: float = 0.9):
    rows = fetch_uptime_duration()
    agent_rows = fetch_agent_peer_count()
    reliable_peers, _totals = aggregate_uptime_by_multi_hash(rows, threshold)
    agent_counts = count_reliable_peers_by_agent(reliable_peers, agent_rows)
    return agent_counts

def print_reliable_agent_counts(agent_counts, threshold: float = 0.9):
    """
    Print (agent_version, percentage) for peers whose uptime ratio exceeds `threshold`,
    where percentage = count / total_count, sorted from highest count to lowest.
    """

    total_count = sum(agent_counts.values())
    print(f"=== Reliable agent % at threshold {threshold} (count/total_count) ===")
    print("Agent\tPercent")
    print("-" * 20)
    for agent, count in agent_counts.items():
        pct = (count / total_count * 100.0) if total_count else 0.0
        print(f"{agent}\t{pct:.2f}%")
    print("-" * 20)


def main():
    """Fetch data and show reliable peers by agent chart."""
    agent_counts = get_reliable_agent_counts(0.9)
    print_reliable_agent_counts(agent_counts, 0.9)

def remote_main():
    agent_counts = get_remote_data("/reliable-agent-counts")
    print_reliable_agent_counts(agent_counts, 0.9)


if __name__ == "__main__":
    main()

