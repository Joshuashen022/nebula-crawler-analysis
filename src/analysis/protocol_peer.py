"""
Use output from protocol_peer_count.fetch_protocol_peer_count() to compute,
per protocol, how many distinct multi_hash (peer) accounts it contains.
"""
import sys
from pathlib import Path

from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.protocol_peer_count import fetch_protocol_peer_count
from src.api.get_remote_data import get_remote_data

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

def remote_main():
    result = get_remote_data("/protocol-peer")

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
    remote_main()

# Protocol                                                      Distinct multi_hash
# ----------------------------------------------------------------------------------
# /ipfs/ping/1.0.0                                                            8,255
# /ipfs/id/1.0.0                                                              8,251
# /ipfs/id/push/1.0.0                                                         8,238
# /ipfs/kad/1.0.0                                                             7,250
# /libp2p/autonat/1.0.0                                                       7,100
# /libp2p/circuit/relay/0.2.0/stop                                            6,755
# /ipfs/bitswap/1.0.0                                                         6,426
# /ipfs/bitswap/1.1.0                                                         6,426
# /ipfs/bitswap                                                               6,395
# /ipfs/bitswap/1.2.0                                                         6,394
# /x/                                                                         6,376
# /libp2p/dcutr                                                               6,266
# /ipfs/lan/kad/1.0.0                                                         6,076
# /libp2p/circuit/relay/0.2.0/hop                                             5,459
# /libp2p/autonat/2/dial-back                                                 3,488
# /libp2p/autonat/2/dial-request                                              3,488
# /libp2p/circuit/relay/0.1.0                                                 2,121
# /p2p/id/delta/1.0.0                                                         2,074
# /sbptp/1.0.0                                                                1,149


# Protocol                                                      Distinct multi_hash
# ----------------------------------------------------------------------------------
# /ipfs/ping/1.0.0                                                            6,798
# /ipfs/id/1.0.0                                                              6,795
# /ipfs/id/push/1.0.0                                                         6,762
# /ipfs/kad/1.0.0                                                             6,238
# /libp2p/autonat/1.0.0                                                       6,074
# /libp2p/circuit/relay/0.2.0/stop                                            5,754
# /ipfs/bitswap/1.0.0                                                         5,632
# /ipfs/bitswap/1.1.0                                                         5,632
# /ipfs/bitswap/1.2.0                                                         5,601
# /ipfs/bitswap                                                               5,582
# /x/                                                                         5,560
# /libp2p/dcutr                                                               5,393
# /ipfs/lan/kad/1.0.0                                                         5,345
# /libp2p/circuit/relay/0.2.0/hop                                             4,931
# /libp2p/autonat/2/dial-back                                                 2,936
# /libp2p/autonat/2/dial-request                                              2,935
# /libp2p/circuit/relay/0.1.0                                                 1,611
# /p2p/id/delta/1.0.0                                                         1,564
# /sbptp/1.0.0                                                                  704