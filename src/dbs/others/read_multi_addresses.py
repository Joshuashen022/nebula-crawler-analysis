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
                    id,
                    asn,
                    is_cloud,
                    is_relay,
                    is_public,
                    addr,
                    has_many_addrs,
                    resolved,
                    country,
                    continent,
                    maddr,
                    updated_at,
                    created_at
                FROM multi_addresses
                ORDER BY id
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

