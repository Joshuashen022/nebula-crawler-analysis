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
                    peer_id,
                    crawl_id,
                    session_id,
                    agent_version_id,
                    protocols_set_id,
                    type,
                    connect_error,
                    crawl_error,
                    visit_started_at,
                    visit_ended_at,
                    created_at,
                    dial_duration,
                    connect_duration,
                    crawl_duration,
                    multi_address_ids,
                    peer_properties
                FROM visits
                ORDER BY id, visit_started_at
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

