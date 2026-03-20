import psycopg2
from psycopg2.extras import RealDictCursor

import config

DB_HOST = config.DB_HOST
# example:
# ('QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt', 1773294894)
# ('QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ', 1773294895)
# ('12D3KooWHmouV6chMaddmw57Xix4hyJKmM4NxDEo2tHpZKhtd3TZ', 1773380086)
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
                SELECT multi_hash, created_at
                FROM peers
                WHERE multi_hash IS NOT NULL 
                AND TRIM(BOTH FROM multi_hash) <> '';
                """
            )
            rows = cur.fetchall()
        return [(row["multi_hash"], int(row["created_at"].timestamp())) for row in rows if row.get("multi_hash")]
    finally:
        conn.close()


if __name__ == "__main__":
    rows = fetch_all_multi_hashes()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))

