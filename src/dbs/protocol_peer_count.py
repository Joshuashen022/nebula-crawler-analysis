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
# ('/p2p/id/delta/1.0.0', 'QmPsTfuRWt9QM7FdauNGofSx8N64avHimd29WSNQEFxLzp')
# ('/libp2p/circuit/relay/0.2.0/stop', '12D3KooWKp9dfaoZobUbjV864wPEotaFrCrPJahzVtJbQ5PfqCZB')
# ('/ipfs/kad/1.0.0', '12D3KooWNas7F1UqWWLxNpyZM7wW2CSUjnXCKsQfKtH1LT1neWSn')
def fetch_protocol_peer_count():
    """
    Get peerId (multi_hash) from peers; join via peers_x_multi_addresses to multi_addresses for country.
    Returns list of (protocol, multi_hash).
    """

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    p.protocol,
                    pr.multi_hash
                FROM 
                    protocols p
                JOIN 
                    (SELECT id, unnest(protocol_ids) AS protocol_id FROM protocols_sets) ps 
                    ON p.id = ps.protocol_id
                JOIN 
                    peers pr ON pr.protocols_set_id = ps.id
                WHERE 
                    p.protocol IS NOT NULL 
                    AND TRIM(p.protocol) <> ''
                """
            )
            rows = cur.fetchall()
        return [(row["protocol"], row["multi_hash"]) for row in rows]
    finally:
        conn.close()

if __name__ == "__main__":
    rows = fetch_protocol_peer_count()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))

