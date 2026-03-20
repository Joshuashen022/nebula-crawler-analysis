import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.api.get_remote_data import get_remote_data
from src.dbs.protocol.read_protocols import fetch_protocols
def protocol_compare():
    remote_result = get_remote_data("/protocol-peer")
    remote_dict = dict(remote_result['counts'])
    print(len(remote_dict))
    # local_result = fetch_protocols()
    local_result = dict(fetch_protocols())
    print(len(local_result))
    # print(local_result[0])
    # print(local_result[1])
    # print(local_result[2])
    # print(len(local_result))
    

if __name__ == "__main__":
    protocol_compare()