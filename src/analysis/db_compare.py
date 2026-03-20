import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.api.get_remote_data import get_remote_data
from src.dbs.protocol.read_protocols import fetch_protocols
def protocol_compare():
    remote_result = get_remote_data("/dbs/protocols")
    remote_dict = {}
    for row in remote_result:
        remote_dict[row[1]] = row[0]
    
    local_result = fetch_protocols()
    local_dict = {}
    for row in local_result:
        local_dict[row[1]] = row[0]

    for key, _ in remote_dict.items():
        if key not in local_dict:
            print(key)
    

if __name__ == "__main__":
    protocol_compare()