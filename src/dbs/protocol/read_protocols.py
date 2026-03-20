import psycopg2
from psycopg2.extras import RealDictCursor
import config

DB_HOST = config.DB_HOST

def fetch_protocols():
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
                SELECT id, protocol
                FROM protocols
                ORDER BY id;
                """
            )
            rows = cur.fetchall()

    finally:
        conn.close()
    return rows


if __name__ == "__main__":
    fetch_protocols()

