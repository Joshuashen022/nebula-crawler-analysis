import psycopg2
from psycopg2.extras import RealDictCursor

# import config

# DB_HOST = config.DB_HOST
DB_HOST = "localhost"


def get_conn():
    """Database connection matching read_peers / read_multi_addresses."""
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


if __name__ == "__main__":
    rows = fetch_uptime_duration()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))

