
import psycopg2
from psycopg2.extras import RealDictCursor

# import config

# DB_HOST = config.DB_HOST
DB_HOST = "localhost"

def fetch_all_multi_hashes():
    """from peers read all multi_hash。"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=5432,
        dbname="nebula_local",
        user="joshua",
        password="",
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT multi_hash
                FROM peers;
                """
            )
            rows = cur.fetchall()
        return [row["multi_hash"] for row in rows if row.get("multi_hash")]
    finally:
        conn.close()

