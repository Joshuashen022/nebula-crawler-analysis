"""
Use output from protocol_peer_count.fetch_protocol_peer_count() to compute,
per protocol, how many distinct multi_hash (peer) accounts it contains.
"""
import sys
from pathlib import Path

from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.protocol_peer_count import fetch_protocol_peer_count

def sort_protocol_peer_count(rows):
    protocol_hashes: dict[str, set[str]] = defaultdict(set)
    for protocol, multi_hash in rows:
        protocol_hashes[protocol].add(multi_hash)

    # sort by count descending, then by protocol name
    sorted_protocols = sorted(
        protocol_hashes.items(),
        key=lambda x: (-len(x[1]), x[0]),
    )
    result = dict()
    count = []
    for protocol, hashes in sorted_protocols:
        count.append((protocol, len(hashes)))
    result["counts"] = count
    result["total"] = len(protocol_hashes)
    result["pair"] = len(rows)
    return result

def main():
    rows = fetch_protocol_peer_count()
    result = sort_protocol_peer_count(rows)

    print("=== Protocol distinct multi_hash (peer) count ===\n")
    print(f"{'Protocol':<60} {'Distinct multi_hash':>20}")
    print("-" * 82)
    for protocol, count in result["counts"]:
        print(f"{protocol:<60} {count:>20,}")
    print("-" * 82)
    print(result["total"])
    print(f"{'Total protocols':<60} {result['total']:>20,}")
    print(f"{'Total (protocol, multi_hash) pairs':<60} {result['pair']:>20,}")



if __name__ == "__main__":
    main()

# Protocol                                                      Distinct multi_hash
# ----------------------------------------------------------------------------------
# /ipfs/ping/1.0.0                                                            5,728
# /ipfs/id/1.0.0                                                              5,721
# /ipfs/id/push/1.0.0                                                         5,716
# /ipfs/kad/1.0.0                                                             5,463
# /libp2p/autonat/1.0.0                                                       5,247
# /libp2p/circuit/relay/0.2.0/stop                                            4,846
# /ipfs/bitswap/1.0.0                                                         4,796
# /ipfs/bitswap/1.1.0                                                         4,796
# /ipfs/bitswap                                                               4,791
# /x/                                                                         4,772
# /ipfs/bitswap/1.2.0                                                         4,756
# /ipfs/lan/kad/1.0.0                                                         4,662
# /libp2p/dcutr                                                               4,569
# /libp2p/circuit/relay/0.2.0/hop                                             4,336
# /libp2p/autonat/2/dial-back                                                 2,454
# /libp2p/autonat/2/dial-request                                              2,454
# /libp2p/circuit/relay/0.1.0                                                 1,384
# /p2p/id/delta/1.0.0                                                         1,338
