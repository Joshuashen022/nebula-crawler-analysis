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
from src.dbs.peers.read_multi_hashes_update_time import fetch_all_multi_hashes
from src.api.get_remote_data import get_remote_data

TZ_UTC8 = timezone(timedelta(hours=8))
step_length_seconds = 3600

def _to_timestamp(t) -> int:
    if hasattr(t, "timestamp"):
        return int(t.timestamp())
    return int(t)


def get_multi_hash_count_by_update_duration():
    """
    start_time: Unix timestamp (seconds); the beginning of the first bucket (earliest time).
    step_length_seconds: Length of each bucket in seconds (default 3600 = 1 hour).
    """
    rows = fetch_all_multi_hashes()
    bucket_count: dict[int, int] = {}
    for _multi_hash, updated_at, created_at in rows:
        ts = _to_timestamp(updated_at) - _to_timestamp(created_at)
        bucket_id = ts // step_length_seconds
        bucket_count[bucket_id] = bucket_count.get(bucket_id, 0) + 1
    return bucket_count

def print_multi_hash_count_by_update_duration(bucket_count):

    step_hours = step_length_seconds / 3600
    print(f"\n=== multi_hash count by update_duration (step = {step_length_seconds}s = {step_hours:.2f}h) ===\n")
    print(f"{'Bucket':>8} \t {'Count':>12}")
    print("-" * 50)

    for bucket_id in sorted(bucket_count.keys()):
        # bucket_duration = bucket_id * step_length_seconds
        count = bucket_count[bucket_id]
        print(f"{bucket_id} hours \t {count:>12,}")
    print("-" * 50)
    print(f"{'Total buckets':<28} {'':>8} {sum(bucket_count.values()):>12,}")

def remote_main():
    bucket_count = get_remote_data("/multi-hash-count-by-update-duration")
    print_multi_hash_count_by_update_duration(bucket_count)

def main():
    """
    start_time: Unix timestamp (seconds). If None, use the min created_at in the data.
    step_length_seconds: Length of each bucket in seconds (default 3600 = 1 hour).
    """
    bucket_count = get_multi_hash_count_by_update_duration()
    print_multi_hash_count_by_update_duration(bucket_count)


if __name__ == "__main__":
    remote_main()
