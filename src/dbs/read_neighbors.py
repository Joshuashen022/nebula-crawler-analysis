import psycopg2
from psycopg2.extras import RealDictCursor


def main():
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="nebula_local",
        user="joshua",
        password="",
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    crawl_id,
                    peer_id,
                    neighbor_ids,
                    error_bits
                FROM neighbors
                ORDER BY crawl_id, peer_id
                LIMIT 10;
                """
            )
            rows = cur.fetchall()

        for row in rows:
            print(row)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

