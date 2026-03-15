"""
Read output from read_multi_hashes_create_time.fetch_all_multi_hashes(),
then count multi_hash (peer) records per time bucket: start_time + k * step_length.
Default step_length is 1 hour (3600 seconds).
Display in UTC+8 (e.g. 2026-03-12 13:54:54+08) to avoid timezone confusion.
"""
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.peers.read_multi_hashes_create_time import fetch_all_multi_hashes

TZ_UTC8 = timezone(timedelta(hours=8))


def _to_timestamp(t) -> int:
    if hasattr(t, "timestamp"):
        return int(t.timestamp())
    return int(t)


def fetch_multi_hash_count_by_create_time(
    rows: list[tuple[str, object]],
    start_time: int,
    step_length_seconds: int = 3600,
):
    """
    start_time: Unix timestamp (seconds); the beginning of the first bucket (earliest time).
    step_length_seconds: Length of each bucket in seconds (default 3600 = 1 hour).
    """
    bucket_count: dict[int, int] = {}
    for _multi_hash, created_at in rows:
        ts = _to_timestamp(created_at)
        if ts < start_time:
            continue
        bucket_id = (ts - start_time) // step_length_seconds
        bucket_count[bucket_id] = bucket_count.get(bucket_id, 0) + 1
    return bucket_count
    

def main():
    """
    start_time: Unix timestamp (seconds). If None, use the min created_at in the data.
    step_length_seconds: Length of each bucket in seconds (default 3600 = 1 hour).
    """
    rows = fetch_all_multi_hashes() # 1773294894
    start_time = 1741743294# 2026-03-12 13:54:54.811238+08 -> 1741743294 
    start_str = datetime.fromtimestamp(start_time, tz=TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S+08")
    print(f"Using start_time: {start_time} ({start_str})")
    step_length_seconds = 3600
    
    bucket_count = fetch_multi_hash_count_by_create_time(rows, start_time, step_length_seconds)
    
    step_hours = step_length_seconds / 3600
    print(f"\n=== multi_hash count by create_time (step = {step_length_seconds}s = {step_hours:.2f}h) ===\n")
    print(f"{'Bucket start (+08)':<28} {'Bucket':>8} {'Count':>12}")
    print("-" * 50)

    for bucket_id in sorted(bucket_count.keys()):
        bucket_start_ts = start_time + bucket_id * step_length_seconds
        bucket_start_str = datetime.fromtimestamp(bucket_start_ts, tz=TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S+08")
        count = bucket_count[bucket_id]
        print(f"{bucket_start_str:<28} {bucket_id:>8} {count:>12,}")
    print("-" * 50)
    print(f"{'Total buckets':<28} {'':>8} {sum(bucket_count.values()):>12,}")


if __name__ == "__main__":
    main()
