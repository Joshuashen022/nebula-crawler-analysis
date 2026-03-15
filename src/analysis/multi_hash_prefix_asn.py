"""
(ASN, node count, rank) from peers × multi_addresses. Rank from database/asn.json (ASRank API).
"""
import sys
from pathlib import Path
from collections import defaultdict
from typing import Optional
import time
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.multi_hash_prefix_asn import fetch_peer_id_prefix_by_asn
from src.api.asn_lookup import lookup_asn


def split_peer_ids_by_asn_count(rows):
    """
    Split (multi_hash, asn) pairs into (asn, total_count, qm_count, d3_count), sort by total desc, NULL last.
    """
    by_asn = defaultdict(set)
    for multi_hash, asn in rows:
        by_asn[asn].add(multi_hash)

    result = []
    for asn, peer_ids in by_asn.items():
        total = len(peer_ids)
        qm_count = sum(1 for h in peer_ids if h and h.startswith("Qm"))
        d3_count = sum(1 for h in peer_ids if h and h.startswith("12D3"))
        result.append((asn, total, qm_count, d3_count))
    result.sort(key=lambda x: (x[0] is None, -x[1]))
    return result


def get_rank(asn) -> Optional[int]:
    """Get ASN rank from DB/API (data.asn.rank). Returns None if asn is None or rank missing."""
    if asn is None:
        return None
    try:
        rec = lookup_asn(str(asn))
        return rec.get("data", {}).get("asn", {}).get("rank")
    except Exception:
        return None


def main():
    rows = fetch_peer_id_prefix_by_asn()
    result = split_peer_ids_by_asn_count(rows)

    print("ASN\t\tcount\t\tQm\t\t12D3\t\trank")
    for asn, count, qm_count, d3_count in result:
        rank = get_rank(asn)
        print(f"{asn}\t\t{count}\t\t{qm_count}\t\t{d3_count}\t\t{rank}")


if __name__ == "__main__":
    main()


# ASN             count           Qm              12D3            rank
# 20473           816             22              794             94
# 24940           541             73              468             633
# 21928           485             0               485             4702
# 51167           403             62              341             12505
# 4134            354             236             118             129
# 17621           339             0               339             903
# 24400           320             0               320             887
# 16276           318             21              297             397
# 14061           305             35              270             4709
# 16509           262             55              207             564
# 36352           233             1               232             937
# 40021           172             27              145             12998
# 22773           131             0               131             111
# 4766            129             103             26              61
# 14618           111             13              98              1266
# 57043           103             0               103             1642
# 8075            100             76              24              2933
# 63949           97              2               95              12426
# 7922            88              3               85              36