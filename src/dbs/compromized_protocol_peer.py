

import psycopg2
from psycopg2.extras import RealDictCursor

# import config

# DB_HOST = config.DB_HOST
DB_HOST = "localhost"


def get_conn():
    """Database connection matching read_peers / read_multi_addresses."""
    return psycopg2.connect(
        host=DB_HOST,
        port=5432,
        dbname="nebula_local",
        user="joshua",
        password="",
    )
# compromised protocols
# 20	/sbptp/1.0.0
# 21	/sfst/2.0.0
# 22	/sfst/1.0.0
# 124	/sbst/1.0.0

# Example output:
# ('kubo/0.28.0/e7f0f340c', '12D3KooWBcKJSCmKvakuD1w6XVRKJcMPtR61SyfF4TQnPKLjHP9V')
# ('go-ipfs/0.13.1/', '12D3KooWL56t2x25773n7BDqEPaUuQ8ZbUa2wMVSx9JLQdFn9Y2Y')
# ('kubo/0.18.1/675f8bd/docker', '12D3KooWSjGF3yMJhovuWD21VyBvWcdiVrzLJJ6nah2epVrfcnvw')
def fetch_compromized_protocol_peer():
    """
    Get peerId (multi_hash) from peers; join via peers_x_multi_addresses to multi_addresses for country.
    Returns list of (multi_hash, protocols_set_id, protocol_ids).
    """

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT peers.multi_hash, protocols_sets.id, protocols_sets.protocol_ids
                FROM protocols_sets
                JOIN peers ON protocols_sets.id = peers.protocols_set_id
                WHERE protocol_ids && ARRAY[20, 21, 22, 124]::int[]
                """
            )
            rows = cur.fetchall()
        return [(row["multi_hash"], row["id"], row["protocol_ids"]) for row in rows]
    finally:
        conn.close()

if __name__ == "__main__":
    rows = fetch_compromized_protocol_peer()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))

