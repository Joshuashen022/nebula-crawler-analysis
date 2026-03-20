import psycopg2
from psycopg2.extras import RealDictCursor
import config

DB_HOST = config.DB_HOST

def fetch_protocols():
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
                SELECT id, protocol
                FROM protocols
                ORDER BY id;
                """
            )
            rows = cur.fetchall()
        return [(row["id"], row["protocol"]) for row in rows]
    finally:
        conn.close()


if __name__ == "__main__":
    rows = fetch_protocols()
    print(rows[0])
    print(rows[1])
    print(rows[2])
    print(len(rows))

#         {
#             "id": 39,
#             "protocol": "/sbptp/1.0.0"
#         },
#         {
#             "id": 40,
#             "protocol": "/sfst/1.0.0"
#         },
#         {
#             "id": 43,
#             "protocol": "/sfst/2.0.0"
#         },
#         {
#             "id": 122,
#             "protocol": "/sbst/1.0.0"
#         },
# {
#     "ok": true,
#     "service": "protocols",
#     "data": [
#         {
#             "id": 1,
#             "protocol": "/ipfs/bitswap"
#         },
#         {
#             "id": 2,
#             "protocol": "/ipfs/bitswap/1.0.0"
#         },
#         {
#             "id": 3,
#             "protocol": "/ipfs/bitswap/1.1.0"
#         },
#         {
#             "id": 4,
#             "protocol": "/ipfs/bitswap/1.2.0"
#         },
#         {
#             "id": 5,
#             "protocol": "/ipfs/id/1.0.0"
#         },
#         {
#             "id": 6,
#             "protocol": "/ipfs/id/push/1.0.0"
#         },
#         {
#             "id": 7,
#             "protocol": "/ipfs/kad/1.0.0"
#         },
#         {
#             "id": 8,
#             "protocol": "/ipfs/lan/kad/1.0.0"
#         },
#         {
#             "id": 9,
#             "protocol": "/ipfs/ping/1.0.0"
#         },
#         {
#             "id": 10,
#             "protocol": "/p2p/id/delta/1.0.0"
#         },
#         {
#             "id": 11,
#             "protocol": "/libp2p/circuit/relay/0.2.0/hop"
#         },
#         {
#             "id": 12,
#             "protocol": "/libp2p/autonat/1.0.0"
#         },
#         {
#             "id": 13,
#             "protocol": "/libp2p/circuit/relay/0.1.0"
#         },
#         {
#             "id": 14,
#             "protocol": "/libp2p/circuit/relay/0.2.0/stop"
#         },
#         {
#             "id": 15,
#             "protocol": "/libp2p/autonat/2/dial-back"
#         },
#         {
#             "id": 16,
#             "protocol": "/libp2p/autonat/2/dial-request"
#         },
#         {
#             "id": 17,
#             "protocol": "/x/"
#         },
#         {
#             "id": 18,
#             "protocol": "/libp2p/dcutr"
#         },
#         {
#             "id": 19,
#             "protocol": "/floodsub/1.0.0"
#         },
#         {
#             "id": 20,
#             "protocol": "/meshsub/1.0.0"
#         },
#         {
#             "id": 21,
#             "protocol": "/meshsub/1.1.0"
#         },
#         {
#             "id": 22,
#             "protocol": "/libp2p/fetch/0.0.1"
#         },
#         {
#             "id": 23,
#             "protocol": "/meshsub/1.2.0"
#         },
#         {
#             "id": 24,
#             "protocol": "/address-exchange/1.0.1"
#         },
#         {
#             "id": 25,
#             "protocol": "/controlplane/v1"
#         },
#         {
#             "id": 26,
#             "protocol": "/http/1.1"
#         },
#         {
#             "id": 27,
#             "protocol": "/dataplane/v1"
#         },
#         {
#             "id": 28,
#             "protocol": "/meshsub/1.3.0"
#         },
#         {
#             "id": 29,
#             "protocol": "/edgevpn/service/0.1"
#         },
#         {
#             "id": 30,
#             "protocol": "/hyprspace/0.0.1"
#         },
#         {
#             "id": 31,
#             "protocol": "harmony/epochsync/mainnet/0/1.0.0"
#         },
#         {
#             "id": 32,
#             "protocol": "harmony/sync/mainnet/0/1.0.0"
#         },
#         {
#             "id": 33,
#             "protocol": "harmony/sync/mainnet/1/1.0.0"
#         },
#         {
#             "id": 34,
#             "protocol": "/meshproxy/circuit/1.0.0"
#         },
#         {
#             "id": 35,
#             "protocol": "/fsp2p/vpn/0.0.1"
#         },
#         {
#             "id": 36,
#             "protocol": "/tunsgo/hostpex/1.0.0"
#         },
#         {
#             "id": 37,
#             "protocol": "/tunsgo/pex/1.0.0"
#         },
#         {
#             "id": 38,
#             "protocol": "/tunsgo/urlproxy/1.0.0"
#         },

#         {
#             "id": 41,
#             "protocol": "/libp2p/autonat/v1.0.0"
#         },
#         {
#             "id": 42,
#             "protocol": "/http"
#         },

#         {
#             "id": 44,
#             "protocol": "/ipfs/bitswap/1.2.0/ipfs/bitswap"
#         },
#         {
#             "id": 45,
#             "protocol": "/ipfs/bitswap/1.2.0/ipfs/bitswap/1.0.0"
#         },
#         {
#             "id": 46,
#             "protocol": "/ipfs/bitswap/1.2.0/ipfs/bitswap/1.1.0"
#         },
#         {
#             "id": 47,
#             "protocol": "/ipfs/bitswap/1.2.0/ipfs/bitswap/1.2.0"
#         },
#         {
#             "id": 48,
#             "protocol": "/koinos/peerrpc/1.0.0"
#         },
#         {
#             "id": 49,
#             "protocol": "/oos-dataplane/1.0.0"
#         },
#         {
#             "id": 50,
#             "protocol": "/p2p-msg-bus/peer-addr-request/1.0.0"
#         },
#         {
#             "id": 51,
#             "protocol": "/p2p-playground/deploy/1.0.0"
#         },
#         {
#             "id": 52,
#             "protocol": "/p2p-playground/list/1.0.0"
#         },
#         {
#             "id": 53,
#             "protocol": "/p2p-playground/logs/1.0.0"
#         },
#         {
#             "id": 54,
#             "protocol": "/p2p-playground/membership/1.0.0"
#         },
#         {
#             "id": 55,
#             "protocol": "/p2p-playground/transfer/1.0.0"
#         },
#         {
#             "id": 56,
#             "protocol": "/bitcoin/alert-system/1.0.0"
#         },
#         {
#             "id": 57,
#             "protocol": "/xpc-p2p/dht/1.0.0/kad/1.0.0"
#         },
#         {
#             "id": 58,
#             "protocol": "/xpc/clearance-gossip/1.0.0"
#         },
#         {
#             "id": 59,
#             "protocol": "/xpc/cold-sync/1.0.0"
#         },
#         {
#             "id": 60,
#             "protocol": "/xpc/merkle-branch-sync/1.0.0"
#         },
#         {
#             "id": 61,
#             "protocol": "/xpc/merkle-root-gossip/1.0.0"
#         },
#         {
#             "id": 62,
#             "protocol": "/xpc/missing-data-sync/1.0.0"
#         },
#         {
#             "id": 63,
#             "protocol": "/xpc/pending-message-gossip/1.0.0"
#         },
#         {
#             "id": 64,
#             "protocol": "/ipfs/graphsync/2.0.0"
#         },
#         {
#             "id": 65,
#             "protocol": "harmony/epochsync/testnet/0/1.0.0"
#         },
#         {
#             "id": 66,
#             "protocol": "harmony/sync/testnet/0/1.0.0"
#         },
#         {
#             "id": 67,
#             "protocol": "/strecoin/1.0.0"
#         },
#         {
#             "id": 68,
#             "protocol": "/piconet/content/1.0.0"
#         },
#         {
#             "id": 69,
#             "protocol": "/piconet/gameproxy/1.0.0"
#         },
#         {
#             "id": 70,
#             "protocol": "/p2pmessage/1.0.0"
#         },
#         {
#             "id": 71,
#             "protocol": "/proxy-example/0.0.1"
#         },
#         {
#             "id": 72,
#             "protocol": "/holler/1.0.0"
#         },
#         {
#             "id": 73,
#             "protocol": "/ipfs/graphsync/1.0.0"
#         },
#         {
#             "id": 74,
#             "protocol": "/webrtc-signaling/0.0.1"
#         },
#         {
#             "id": 75,
#             "protocol": "harmony/sync/testnet/1/1.0.0"
#         },
#         {
#             "id": 76,
#             "protocol": "/shsk/1.0.0"
#         },
#         {
#             "id": 77,
#             "protocol": "/doogle/antientropy/1.0.0"
#         },
#         {
#             "id": 78,
#             "protocol": "/doogle/crawl/1.0.0"
#         },
#         {
#             "id": 79,
#             "protocol": "/doogle/fleet/heartbeat/1.0.0"
#         },
#         {
#             "id": 80,
#             "protocol": "/doogle/index/1.0.0"
#         },
#         {
#             "id": 81,
#             "protocol": "/doogle/replicate/1.0.0"
#         },
#         {
#             "id": 82,
#             "protocol": "/doogle/search/1.0.0"
#         },
#         {
#             "id": 83,
#             "protocol": "/doogle/shard/1.0.0"
#         },
#         {
#             "id": 84,
#             "protocol": "/blackbox/auth/1.0.0"
#         },
#         {
#             "id": 85,
#             "protocol": "/blackbox/chat/1.0.0"
#         },
#         {
#             "id": 86,
#             "protocol": "/blackbox/files/1.0.0"
#         },
#         {
#             "id": 87,
#             "protocol": "/dc/thread/0.0.1"
#         },
#         {
#             "id": 88,
#             "protocol": "/dc/transfer/1.0.0"
#         },
#         {
#             "id": 89,
#             "protocol": "/hyprspace/pex/0.0.1"
#         },
#         {
#             "id": 90,
#             "protocol": "/hyprspace/service/0.0.1"
#         },
#         {
#             "id": 91,
#             "protocol": "/strelayp/1.0.0"
#         },
#         {
#             "id": 92,
#             "protocol": "/skychat/bootstrap-identity/1.0.0"
#         },
#         {
#             "id": 93,
#             "protocol": "/skychat/peer-exchange/1.0.0"
#         },
#         {
#             "id": 94,
#             "protocol": "/chat/1.1.0"
#         },
#         {
#             "id": 95,
#             "protocol": "/telemetry/telemetry/0.3.0"
#         },
#         {
#             "id": 96,
#             "protocol": "/skychat/room-history/1.0.0"
#         },
#         {
#             "id": 97,
#             "protocol": "/skychat/voice-signal/1.0.0"
#         },
#         {
#             "id": 98,
#             "protocol": "/arcana/mpc-sig/1.0.0"
#         },
#         {
#             "id": 99,
#             "protocol": "harmony/epochsync/partner/0/1.0.0"
#         },
#         {
#             "id": 100,
#             "protocol": "harmony/sync/partner/0/1.0.0"
#         },
#         {
#             "id": 101,
#             "protocol": "harmony/sync/partner/1/1.0.0"
#         },
#         {
#             "id": 102,
#             "protocol": "/kilo/message/1.0.0"
#         },
#         {
#             "id": 103,
#             "protocol": "/fil/datatransfer/1.2.0"
#         },
#         {
#             "id": 104,
#             "protocol": "/fil/storage/transfer/1.0.0"
#         },
#         {
#             "id": 105,
#             "protocol": "/legs/head/indexer/ingest/mainnet/0.0.1"
#         },
#         {
#             "id": 106,
#             "protocol": "/fil/kad/testnetnet/kad/1.0.0"
#         },
#         {
#             "id": 107,
#             "protocol": "/freepath/msg/1.0.0"
#         },
#         {
#             "id": 108,
#             "protocol": "/meshproxy/peer-exchange/1.0.0"
#         },
#         {
#             "id": 109,
#             "protocol": "/meshproxy/socks5-tunnel/1.0.0"
#         },
#         {
#             "id": 110,
#             "protocol": "/yggdrasil/peering/1.0.0"
#         },
#         {
#             "id": 111,
#             "protocol": "/meshproxy/raw-tunnel-e2e/1.0.0"
#         },
#         {
#             "id": 112,
#             "protocol": "/ipfscluster/1.0/rpc"
#         },
#         {
#             "id": 113,
#             "protocol": "/fyles/self-contact-invite/0.0.1"
#         },
#         {
#             "id": 114,
#             "protocol": "/fyles/contact-invite/0.0.1"
#         },
#         {
#             "id": 115,
#             "protocol": "/file-push/0.0.3"
#         },
#         {
#             "id": 116,
#             "protocol": "/session-establishment/0.0.2"
#         },
#         {
#             "id": 117,
#             "protocol": "/hootvex/vpn-data/1.0.0"
#         },
#         {
#             "id": 118,
#             "protocol": "/hootvex/wg-keyex/1.0.0"
#         },
#         {
#             "id": 119,
#             "protocol": "/hootvex/circuit/1.0.0"
#         },
#         {
#             "id": 120,
#             "protocol": "/bitcoin-testnet/alert-system/0.0.1"
#         },
#         {
#             "id": 121,
#             "protocol": "/dvpn/dvpnmessage/1.0.0"
#         },

#         {
#             "id": 123,
#             "protocol": "/edgevpn/0.1"
#         },
#         {
#             "id": 124,
#             "protocol": "/hootvex/vpn-tunnel/1.0.0"
#         },
#         {
#             "id": 125,
#             "protocol": "/meshproxy/chat/ack/1.0.0"
#         },
#         {
#             "id": 126,
#             "protocol": "/meshproxy/chat/msg/1.0.0"
#         },
#         {
#             "id": 127,
#             "protocol": "/meshproxy/chat/relay-e2e/1.0.0"
#         },
#         {
#             "id": 128,
#             "protocol": "/meshproxy/chat/request/1.0.0"
#         },
#         {
#             "id": 129,
#             "protocol": "/shadowmesh/content/1.0.0"
#         },
#         {
#             "id": 130,
#             "protocol": "/meshproxy/chat/sync/1.0.0"
#         },
#         {
#             "id": 131,
#             "protocol": "/meshproxy/group/control/1.0.0"
#         },
#         {
#             "id": 132,
#             "protocol": "/meshproxy/group/msg/1.0.0"
#         },
#         {
#             "id": 133,
#             "protocol": "/meshproxy/group/sync/1.0.0"
#         },
#         {
#             "id": 134,
#             "protocol": "/f2p-forward/0.0.2/control"
#         },
#         {
#             "id": 135,
#             "protocol": "/f2p-forward/0.0.2/data"
#         },
#         {
#             "id": 136,
#             "protocol": "/f2p-forward/0.0.2/data+zstd"
#         },
#         {
#             "id": 137,
#             "protocol": "/f2p-forward/0.0.2/holepunch"
#         },
#         {
#             "id": 138,
#             "protocol": "/robot/cmd/1.0.0"
#         },
#         {
#             "id": 139,
#             "protocol": "/robot/joints/1.0.0"
#         },
#         {
#             "id": 140,
#             "protocol": "/robot/video/1.0.0"
#         },
#         {
#             "id": 141,
#             "protocol": "/meshproxy/chat/request/1.0.1"
#         },
#         {
#             "id": 142,
#             "protocol": "/gpu-service/1.0.0"
#         },
#         {
#             "id": 143,
#             "protocol": "/picoSpaces/1.0.0"
#         },
#         {
#             "id": 144,
#             "protocol": "/gpu-market/listings/1.0.0"
#         },
#         {
#             "id": 145,
#             "protocol": "/private-chat/1.0.0"
#         },
#         {
#             "id": 146,
#             "protocol": "/redware/auth/1.0.0"
#         },
#         {
#             "id": 147,
#             "protocol": "/redware/chat/1.0.0"
#         },
#         {
#             "id": 148,
#             "protocol": "/redware/invite/1.0.0"
#         },
#         {
#             "id": 149,
#             "protocol": "/redware/media/1.0.0"
#         },
#         {
#             "id": 150,
#             "protocol": "/redware/sync/1.0.0"
#         }
#     ]
# }