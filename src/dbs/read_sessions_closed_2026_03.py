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
                    first_successful_visit,
                    last_successful_visit,
                    next_visit_due_at,
                    first_failed_visit,
                    last_failed_visit,
                    last_visited_at,
                    updated_at,
                    created_at,
                    successful_visits_count,
                    recovered_count,
                    state,
                    failed_visits_count,
                    finish_reason,
                    uptime
                FROM sessions_closed_2026_03
                ORDER BY id, last_visited_at
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

