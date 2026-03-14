"""
从 peers 表读取所有 multi_hash，统计 Qm 开头、12D3 开头的数量，并检查是否存在其他前缀。
"""
import sys
from pathlib import Path

from collections import Counter

# Allow running as script: python src/analysis/multi_hash_prefix_stats.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.peers.read_multi_hashes import fetch_all_multi_hashes


def get_prefix(multi_hash: str, length: int = 4) -> str:
    """获取 multi_hash 的前缀（默认前 4 个字符，用于区分 12D3 与其它）。"""
    if not multi_hash:
        return ""
    return multi_hash[: min(length, len(multi_hash))]


def main():
    hashes = fetch_all_multi_hashes()
    total = len(hashes)

    count_qm = sum(1 for h in hashes if h and h.startswith("Qm"))
    count_12d3 = sum(1 for h in hashes if h and h.startswith("12D3"))

    # 统计所有前缀（取前 4 个字符，便于区分 12D3 与 Qmxx 等）
    prefix_counter = Counter(get_prefix(h, 4) for h in hashes if h)

    # 重复筛查
    hash_counter = Counter(hashes)
    unique_count = len(hash_counter)
    duplicates = {h: c for h, c in hash_counter.items() if c > 1}
    n_duplicated_hashes = len(duplicates)
    n_duplicate_occurrences = total - unique_count

    print("=== peers.multi_hash 前缀统计 ===\n")
    print(f"总条数: {total:,}")
    print(f"  Qm 开头:  {count_qm:,}")
    print(f"  12D3 开头: {count_12d3:,}")
    print()
    print("--- 重复筛查 ---")
    print(f"去重后数量: {unique_count:,}")
    if n_duplicated_hashes == 0:
        print("无重复 multi_hash。")
    else:
        print(f"存在重复: 共 {n_duplicated_hashes:,} 个 multi_hash 出现多次，多出 {n_duplicate_occurrences:,} 条记录。")
        top_dup = sorted(duplicates.items(), key=lambda x: -x[1])[:5]
        print("重复次数最多的 5 个:")
        for h, c in top_dup:
            snippet = (h[:20] + "…") if len(h) > 20 else h
            print(f"  {snippet!r}: 出现 {c} 次")
    print()

    others = total - count_qm - count_12d3
    if others > 0:
        print(f"其他前缀数量: {others:,}\n")
        print("--- 所有前缀分布（前 4 字符）---")
        for prefix, cnt in prefix_counter.most_common():
            print(f"  {prefix!r}: {cnt:,}")
    else:
        print("未发现 Qm、12D3 以外的其他前缀。")


if __name__ == "__main__":
    main()

# === peers.multi_hash 前缀统计 ===

# 总条数: 17,238
#   Qm 开头:  1,269
#   12D3 开头: 15,969

# --- 重复筛查 ---
# 去重后数量: 17,238
# 无重复 multi_hash。