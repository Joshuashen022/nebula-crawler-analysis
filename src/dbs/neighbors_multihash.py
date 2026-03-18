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
# (1, 'Qma2Ajf8Kyja1DBQU2JXsuLjK4oidNadSeQW6tuhA39Wu2')
# (1, 'QmaigxZegou4ZZ1JTXq17GvPpni9TQGzkibfuXHU4syNhE')
# (2, '12D3KooWD8t5kwAeGfkbbk7Ap5Q9MZo65MkvtdqU3HsAFpLBKCHx')
# (221, 'QmWC4gFKjNgZRJuoftyTGAhYAc5252nM7rQ4ehA9PrfPaP')
def fetch_neighbor_peer():
    """
    Get peerId (multi_hash) from peers; join via peers_x_multi_addresses to multi_addresses for country.
    Returns list of (agent_version, multi_hash).
    """

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    array_length(n.neighbor_ids, 1) AS neighbor_count,
                    p.multi_hash
                FROM 
                    neighbors n
                JOIN peers p ON p.id = n.peer_id
                """
            )
            rows = cur.fetchall()
        return [(row["neighbor_count"], row["multi_hash"]) for row in rows]
    finally:
        conn.close()


def dedupe_by_multi_hash(rows):
    """Deduplicate by multi_hash, keeping the row with max neighbor_count per multi_hash."""
    best = {}
    for neighbor_count, multi_hash in rows:
        if multi_hash not in best or neighbor_count > best[multi_hash][0]:
            best[multi_hash] = (neighbor_count, multi_hash)
    return list(best.values())


if __name__ == "__main__":
    rows = dedupe_by_multi_hash(fetch_neighbor_peer())
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(rows[len(rows) - 1])
    print(len(rows))


