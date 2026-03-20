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
try:
    import matplotlib.pyplot as plt  # type: ignore[reportMissingImports]
except Exception:  # pragma: no cover
    plt = None


def split_peer_ids_by_asn_count(rows):
    """
    Split (multi_hash, asn) pairs into (asn, count, qm_count, d3_count), sort by count desc, NULL last.

    total_count is distinct multi_hash across all rows (global unique peers). Same peer in multiple ASNs
    is counted once in total_count but can appear in several ASN buckets.
    """
    total_count = len({h for h, _ in rows if h})
    by_asn = defaultdict(set)
    for multi_hash, asn in rows:
        if multi_hash:
            by_asn[asn].add(multi_hash)

    result = []
    for asn, peer_ids in by_asn.items():
        count = len(peer_ids)
        qm_count = sum(1 for h in peer_ids if h and h.startswith("Qm"))
        d3_count = sum(1 for h in peer_ids if h and h.startswith("12D3"))
        result.append((asn, count, qm_count, d3_count))
    result.sort(key=lambda x: (x[0] is None, -x[1]))
    return result, total_count


def get_rank(asn) -> Optional[int]:
    """Get ASN rank from DB/API (data.asn.rank). Returns None if asn is None or rank missing."""
    if asn is None:
        return None
    try:
        rec = lookup_asn(str(asn))
        return rec.get("data", {}).get("asn", {}).get("rank")
    except Exception:
        return None


def plot_rank_vs_count(rows_with_rank, output_path: str):
    if plt is None:
        raise RuntimeError("matplotlib is required for plotting (pip install matplotlib).")

    xs = [rank for _, count, _, _, rank in rows_with_rank if rank is not None]
    ys = [count for _, count, _, _, rank in rows_with_rank if rank is not None]
    if not xs:
        raise RuntimeError("No ranks available to plot (all rank values were None).")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(xs, ys, s=18, alpha=0.7)
    ax.set_xscale("log")
    ax.set_yscale("log")

    ticks = [10, 10**2, 10**3, 10**4]
    ax.set_xticks(ticks)
    ax.set_xticklabels(["10", r"$10^2$", r"$10^3$", r"$10^4$"])
    ax.set_yticks(ticks)
    ax.set_yticklabels(["10", r"$10^2$", r"$10^3$", r"$10^4$"])
    ax.tick_params(axis="both", which="major", labelsize=20)
    ax.grid(True, which="both", linestyle="--", linewidth=0.6, alpha=0.5)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200)
    fig.show()
    return output_path


def main():
    rows = fetch_peer_id_prefix_by_asn()
    result, total_count = split_peer_ids_by_asn_count(rows)

    print("ASN\t\tcount\t\tcount/total\t\tQm\t\t12D3\t\trank")
    for asn, count, qm_count, d3_count in result:
        if count < 100:
            continue
        rank = get_rank(asn)
        share = (count / total_count) if total_count else 0.0
        print(f"{asn}\t\t{count}\t\t{share:.6f}\t\t{qm_count}\t\t{d3_count}\t\t{rank}")

def main2():
    rows = fetch_peer_id_prefix_by_asn()
    result, total_count = split_peer_ids_by_asn_count(rows)

    # Filter count > 100, attach rank, sort by rank ascending (None last)
    rows_with_rank = [
        (asn, count, qm_count, d3_count, get_rank(asn))
        for asn, count, qm_count, d3_count in result
        if count > 10
    ]
    rows_with_rank.sort(key=lambda x: (x[4] is None, x[4] or 0))

    # print("ASN\t\tcount\t\tcount/total\t\tQm\t\t12D3\t\trank")
    # for asn, count, qm_count, d3_count, rank in rows_with_rank:
    #     share = (count / total_count) if total_count else 0.0
    #     print(f"{asn}\t\t{count}\t\t{share:.6f}\t\t{qm_count}\t\t{d3_count}\t\t{rank}")
    project_root = Path(__file__).resolve().parents[2]
    output_path = project_root / "report" / "pics" / "multi_hash_prefix_asn.png"
    # output_path = str(Path(__file__).with_suffix("")) + "_rank_scatter.png"
    try:
        saved_to = plot_rank_vs_count(rows_with_rank, output_path=output_path)
        print(f"\nSaved scatter plot to: {saved_to}")
    except Exception as e:
        print(f"\nPlot skipped: {e}")


if __name__ == "__main__":
    main2()

# main() output:
# ASN             count           count/total     Qm              12D3            rank
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

# main2() output:
# ASN             count           count/total     Qm              12D3            rank
# 4766            154             126             28              61
# 20473           851             22              829             94
# 22773           135             0               135             111
# 4134            463             323             140             129
# 16276           337             21              316             397
# 16509           285             56              229             564
# 24940           598             73              525             633
# 4812            622             0               622             776
# 24400           321             0               321             887
# 17621           720             0               720             903
# 36352           244             1               243             937
# 14618           127             13              114             1266
# 57043           102             0               102             1642
# 8075            114             85              29              2933
# 21928           499             0               499             4702
# 14061           332             34              298             4709
# 63949           108             2               106             12426
# 51167           408             62              346             12505
# 40021           183             27              156             12998
# None            18010           900             17110           None