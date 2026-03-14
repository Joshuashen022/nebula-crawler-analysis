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
                    multi_address_id,
                    asn,
                    is_cloud,
                    updated_at,
                    created_at,
                    country,
                    continent,
                    address
                FROM ip_addresses
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

