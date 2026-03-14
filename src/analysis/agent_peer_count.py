"""
Use output from protocol_peer_count.fetch_protocol_peer_count() to compute,
per protocol, how many distinct multi_hash (peer) accounts it contains.
"""
import sys
from pathlib import Path

from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.agent_peer_count import fetch_agent_peer_count


def main():
    rows = fetch_agent_peer_count()
    # protocol -> set of multi_hash for distinct count
    agent_hashes: dict[str, set[str]] = defaultdict(set)
    for agent, multi_hash in rows:
        agent_hashes[agent].add(multi_hash)

    # sort by count descending, then by protocol name
    sorted_agents = sorted(
        agent_hashes.items(),
        key=lambda x: (-len(x[1]), x[0]),
    )

    print("=== Agent distinct multi_hash (peer) count ===\n")
    print(f"{'Agent':<60} {'Distinct multi_hash':>20}")
    print("-" * 82)
    for agent, hashes in sorted_agents:
        print(f"{agent:<60} {len(hashes):>20,}")
    print("-" * 82)
    print(f"{'Total agents':<60} {len(agent_hashes):>20,}")
    print(f"{'Total (agent, multi_hash) pairs':<60} {len(rows):>20,}")


if __name__ == "__main__":
    main()
