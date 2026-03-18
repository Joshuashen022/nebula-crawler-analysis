from datetime import datetime, timedelta
from typing import Iterable, Any

# import config

# DB_HOST = config.DB_HOST
DB_HOST = "localhost"


def get_conn():
    """Database connection matching read_peers / read_multi_addresses."""
    import psycopg2

    return psycopg2.connect(
        host=DB_HOST,
        port=5432,
        dbname="nebula_local",
        user="joshua",
        password="",
    )

# Example output:
# ('12D3KooWMaAqLanHTgwxsRCMKp9jKvvTBpwkuNvKS67z4RWBGeEN', datetime.datetime(2026, 3, 16, 11, 24, 8, 151118, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.datetime(2026, 3, 16, 11, 36, 30, 94913, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.timedelta(seconds=741, microseconds=943795), DateTimeTZRange(datetime.datetime(2026, 3, 16, 11, 24, 8, 151118, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.datetime(2026, 3, 16, 11, 36, 30, 93024, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), '[)'), datetime.timedelta(seconds=741, microseconds=941906))
# ('12D3KooWMT1J2WKjDxLB7v95QKrH6EYTH8rDr2nmyv3NKrnqLa9Y', datetime.datetime(2026, 3, 16, 11, 24, 7, 984447, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.datetime(2026, 3, 16, 11, 35, 46, 371184, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.timedelta(seconds=698, microseconds=386737), DateTimeTZRange(datetime.datetime(2026, 3, 16, 11, 24, 7, 984447, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.datetime(2026, 3, 16, 11, 35, 46, 370655, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), '[)'), datetime.timedelta(seconds=698, microseconds=386208))
# ('12D3KooWKtdLTqJp2qD9ZWxVcPQKgjA9ujmVg1XVxLBvUfZmcWqG', datetime.datetime(2026, 3, 16, 11, 24, 6, 1061, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.datetime(2026, 3, 16, 11, 33, 49, 140622, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.timedelta(seconds=583, microseconds=139561), DateTimeTZRange(datetime.datetime(2026, 3, 16, 11, 24, 6, 1061, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), datetime.datetime(2026, 3, 16, 11, 33, 49, 130927, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800))), '[)'), datetime.timedelta(seconds=583, microseconds=129866))
def fetch_uptime_duration():
    """
    Get uptime duration from sessions; join via peers to get multi_hash.
    Returns list of (multi_hash, first_successful_visit, updated_at, crawler_track_duration, uptime, peer_actual_uptime).
    """
    from psycopg2.extras import RealDictCursor

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT 
                    p.multi_hash,
                    s.first_successful_visit,
                    s.updated_at,
                    (s.updated_at - s.first_successful_visit) AS crawler_track_duration,
                    s.uptime,
                    (upper(s.uptime) - lower(s.uptime)) AS peer_actual_uptime
                FROM sessions s
                JOIN peers p ON s.peer_id = p.id
                ORDER BY s.first_successful_visit DESC;
                """
            )
            rows = cur.fetchall()
        return [(row["multi_hash"], row["first_successful_visit"], row["updated_at"], row["crawler_track_duration"], row["uptime"], row["peer_actual_uptime"]) for row in rows]
    finally:
        conn.close()

# deprecated, problem with sessions
def merge_time_windows(
    windows: Iterable[tuple[datetime, datetime]],
    *,
    join_touching: bool = True,
) -> list[tuple[datetime, datetime]]:
    """
    Merge overlapping (and optionally touching) time windows.

    Example:
      (12:00~13:00) + (12:10~13:10) => (12:00~13:10)
      (12:00~13:00) + (14:00~15:00) => two windows
      adding (12:50~14:10) => (12:00~15:00)
    """
    normalized: list[tuple[datetime, datetime]] = []
    for start, end in windows:
        if start is None or end is None:
            continue
        if end < start:
            start, end = end, start
        normalized.append((start, end))

    if not normalized:
        return []

    normalized.sort(key=lambda w: w[0])
    merged: list[tuple[datetime, datetime]] = [normalized[0]]

    for start, end in normalized[1:]:
        cur_start, cur_end = merged[-1]
        overlaps = start <= cur_end if join_touching else start < cur_end
        if overlaps:
            if end > cur_end:
                merged[-1] = (cur_start, end)
        else:
            merged.append((start, end))

    return merged


# deprecated, problem with sessions
def calculate_crawler_total_up_windows(
    sessions: Iterable[Any],
    *,
    start_field: str = "first_successful_visit",
    end_field: str = "updated_at",
    join_touching: bool = True,
) -> tuple[list[tuple[datetime, datetime]], timedelta]:
    """
    Calculate the crawler's total "up" windows by unioning each session's
    alive window (start_field, end_field).

    - `sessions` may contain objects with attributes, or dict-like rows.
    - Returns (merged_windows, total_duration).
    """

    def get_value(s: Any, key: str):
        if isinstance(s, dict):
            return s.get(key)
        return getattr(s, key, None)

    windows: list[tuple[datetime, datetime]] = []
    for s in sessions:
        start = get_value(s, start_field)
        end = get_value(s, end_field)
        if start is None or end is None:
            continue
        windows.append((start, end))

    merged = merge_time_windows(windows, join_touching=join_touching)
    total = sum((end - start for start, end in merged), timedelta(0))
    return merged, total


if __name__ == "__main__":
    rows = fetch_uptime_duration()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))

    # following code is deprecated, problem with sessions
    if rows:
        print("sample row:", rows[0])

    # Each row is:
    # (multi_hash, first_successful_visit, updated_at, crawler_track_duration, uptime, peer_actual_uptime)
    windows = [(r[1], r[2]) for r in rows if r[1] is not None and r[2] is not None]
    merged = merge_time_windows(windows)
    total = sum((end - start for start, end in merged), timedelta(0))

    print("merged_windows:", merged)
    print("total_merged_uptime:", total)

