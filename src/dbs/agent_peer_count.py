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

# Example output:
# ('kubo/0.28.0/e7f0f340c', '12D3KooWBcKJSCmKvakuD1w6XVRKJcMPtR61SyfF4TQnPKLjHP9V')
# ('go-ipfs/0.13.1/', '12D3KooWL56t2x25773n7BDqEPaUuQ8ZbUa2wMVSx9JLQdFn9Y2Y')
# ('kubo/0.18.1/675f8bd/docker', '12D3KooWSjGF3yMJhovuWD21VyBvWcdiVrzLJJ6nah2epVrfcnvw')
def fetch_agent_peer_count():
    """
    从 peers 取 peerId (multi_hash)，通过 peers_x_multi_addresses 关联 multi_addresses 得到 country，
    返回 list of (multi_hash, country)。
    """

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    a.agent_version,
                    pr.multi_hash
                FROM 
                    agent_versions a
                JOIN 
                    peers pr ON pr.agent_version_id = a.id
                """
            )
            rows = cur.fetchall()
        return [(row["agent_version"], row["multi_hash"]) for row in rows]
    finally:
        conn.close()

if __name__ == "__main__":
    rows = fetch_agent_peer_count()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))

