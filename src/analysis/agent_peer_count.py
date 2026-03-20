"""
Use output from protocol_peer_count.fetch_protocol_peer_count() to compute,
per protocol, how many distinct multi_hash (peer) accounts it contains.
"""
import sys
from pathlib import Path

from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.agent_peer_count import fetch_agent_peer_count
from src.api.get_remote_data import get_remote_data

def get_agent_peer_count():
    rows = fetch_agent_peer_count()
    # protocol -> set of multi_hash for distinct count
    agent_hashes: dict[str, set[str]] = defaultdict(set)
    for agent, multi_hash in rows:
        agent_hashes[agent].add(multi_hash)

    total_distinct_pairs = sum(len(hashes) for hashes in agent_hashes.values())

    # sort by count descending, then by protocol name
    sorted_agents = sorted(
        agent_hashes.items(),
        key=lambda x: (-len(x[1]), x[0]),
    )

    # JSON responses can't include `set`, so convert hashes to a stable list.
    # (Tuples are fine for json; sets are not.)
    sorted_agents_json = [(agent, sorted(hashes)) for agent, hashes in sorted_agents]

    result = dict()
    result["sorted_agents"] = sorted_agents_json
    result["total_distinct_pairs"] = total_distinct_pairs
    return result

def remote_main():
    result = get_remote_data("/agent-peer-count")
    sorted_agents = result["sorted_agents"]
    total_distinct_pairs = result["total_distinct_pairs"]
    total_agents = len(result["sorted_agents"])
    print("=== Agent distinct multi_hash (peer) count ===\n")
    print(f"{'Agent':<60} {'Distinct multi_hash':>20} {'% of total':>10}")
    print("-" * 94)
    for agent, hashes in sorted_agents:
        share = (len(hashes) / total_distinct_pairs * 100.0) if total_distinct_pairs else 0.0
        print(f"{agent:<60} {len(hashes):>20,} {share:>9.2f}%")
    print("-" * 94)
    print(f"{'Total agents':<60} {total_agents:>20,}")
    print(f"{'Total distinct (agent, multi_hash) pairs':<60} {total_distinct_pairs:>20,}")


def main():
    result = get_agent_peer_count()
    sorted_agents = result["sorted_agents"]
    total_distinct_pairs = result["total_distinct_pairs"]
    total_agents = len(result["sorted_agents"])
    print("=== Agent distinct multi_hash (peer) count ===\n")
    print(f"{'Agent':<60} {'Distinct multi_hash':>20} {'% of total':>10}")
    print("-" * 94)
    for agent, hashes in sorted_agents:
        share = (len(hashes) / total_distinct_pairs * 100.0) if total_distinct_pairs else 0.0
        print(f"{agent:<60} {len(hashes):>20,} {share:>9.2f}%")
    print("-" * 94)
    print(f"{'Total agents':<60} {total_agents:>20,}")
    print(f"{'Total distinct (agent, multi_hash) pairs':<60} {total_distinct_pairs:>20,}")


if __name__ == "__main__":
    main()


# Agent                                                                   Distinct      percent
# ----------------------------------------------------------------------------------------------
# kubo/0.22.0/3f884d3/gala.games                                              1,007     14.37%
# go-ipfs/0.8.0/48f94e2                                                         683      9.75%
# kubo/0.37.0/6898472/docker                                                    522      7.45%
# kubo/0.18.1/675f8bd/docker                                                    445      6.35%
# kubo/0.39.0/                                                                  411      5.86%
# kubo/0.32.1/                                                                  301      4.30%
# kubo/0.39.0/2896aed/docker                                                    161      2.30%
# storm                                                                         160      2.28%
# kubo/0.40.1/desktop                                                           157      2.24%
# kubo/0.40.1/39f8a65/docker                                                    116      1.66%
# kubo/0.40.1                                                                   115      1.64%
# kubo/0.37.0/6898472                                                           110      1.57%
# kubo/0.28.0/                                                                   98      1.40%
# kubo/0.36.0/                                                                   97      1.38%
# kubo/0.24.0/                                                                   74      1.06%
# kubo/0.33.2/                                                                   64      0.91%
# kubo/0.25.0/                                                                   62      0.88%
# kubo/0.22.0/                                                                   61      0.87%
# kubo/0.39.0/desktop                                                            61      0.87%
# kubo/0.17.0/4485d6b                                                            60      0.86%
# server@d266990fa-dirty                                                         60      0.86%