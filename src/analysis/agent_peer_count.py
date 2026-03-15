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


# kubo/0.22.0/3f884d3/gala.games                                                914
# kubo/0.37.0/6898472/docker                                                    445
# kubo/0.18.1/675f8bd/docker                                                    431
# go-ipfs/0.8.0/48f94e2                                                         426
# kubo/0.39.0/                                                                  335
# kubo/0.32.1/                                                                  207
# kubo/0.39.0/2896aed/docker                                                    150
# storm                                                                         138
# kubo/0.40.1/desktop                                                           106
# kubo/0.37.0/6898472                                                            95
# kubo/0.40.1/39f8a65/docker                                                     94
# kubo/0.36.0/                                                                   91
# kubo/0.40.1                                                                    91
# kubo/0.28.0/                                                                   83
# kubo/0.33.2/                                                                   63
# kubo/0.25.0/                                                                   60
# kubo/0.24.0/                                                                   50
# kubo/0.22.0/                                                                   49
# kubo/0.39.0/desktop                                                            47
# kubo/0.37.0/                                                                   44
# kubo/0.36.0/37b8411/docker                                                     40