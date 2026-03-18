import psycopg2
from psycopg2.extras import RealDictCursor

import config

DB_HOST = config.DB_HOST
# DB_HOST = "localhost"


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
# ('12D3KooW9pPHa87JXvant21YMxEjUgeHNhXWU6tXtRDmHuVdFTos', None)
# ('12D3KooW9pPUBnqbkTEQhUNKzs2R3ZYeeTVHg542Phawj4sPWQg3', 7018)
# ('12D3KooW9pPUBnqbkTEQhUNKzs2R3ZYeeTVHg542Phawj4sPWQg3', 197540)
def fetch_peer_id_prefix_by_asn():
    """
    Get peerId (multi_hash) from peers; join via peers_x_multi_addresses to multi_addresses for asn.
    Returns list of (multi_hash, asn).
    """

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    p.multi_hash,
                    m.asn
                FROM peers p
                INNER JOIN peers_x_multi_addresses px ON p.id = px.peer_id
                INNER JOIN multi_addresses m ON px.multi_address_id = m.id
                WHERE p.multi_hash IS NOT NULL AND p.multi_hash != ''
                """
            )
            rows = cur.fetchall()
        return [(row["multi_hash"], row["asn"]) for row in rows]
    finally:
        conn.close()


if __name__ == "__main__":
    rows = fetch_peer_id_prefix_by_asn()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))