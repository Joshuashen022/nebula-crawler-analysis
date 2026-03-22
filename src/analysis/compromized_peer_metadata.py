"""
Combine compromised peers with agent/asn/country metadata.

Output shape:
    { peerId (multi_hash): (agent_version, asn, country) }
"""

import json
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.api.get_remote_data import get_remote_data
from src.dbs.agent_peer_count import fetch_agent_peer_count
from src.dbs.compromized_protocol_peer import fetch_compromized_protocol_peer
from src.dbs.multi_hash_prefix_asn import fetch_peer_id_prefix_by_asn
from src.dbs.multi_hash_prefix_country import fetch_peer_id_prefix_by_country


PeerMeta = Tuple[Optional[str], Optional[int], Optional[str]]


def _first_non_null(existing, candidate):
    return existing if existing is not None else candidate


def _build_agent_map(rows: Iterable[Tuple[str, str]]) -> Dict[str, str]:
    """
    rows: (agent_version, peerId)
    """
    out: Dict[str, str] = {}
    for agent_version, peer_id in rows:
        if not peer_id:
            continue
        # If duplicates exist, keep the first non-empty agent version.
        if peer_id not in out and agent_version:
            out[peer_id] = agent_version
    return out


def _build_asn_map(rows: Iterable[Tuple[str, Optional[int]]]) -> Dict[str, Optional[int]]:
    """
    rows: (peerId, asn)
    """
    out: Dict[str, Optional[int]] = {}
    for peer_id, asn in rows:
        if not peer_id:
            continue
        out[peer_id] = _first_non_null(out.get(peer_id), asn)
    return out


def _build_country_map(rows: Iterable[Tuple[str, Optional[str]]]) -> Dict[str, Optional[str]]:
    """
    rows: (peerId, country)
    """
    out: Dict[str, Optional[str]] = {}
    for peer_id, country in rows:
        if not peer_id:
            continue
        out[peer_id] = _first_non_null(out.get(peer_id), country)
    return out


def build_compromized_peer_metadata() -> Dict[str, PeerMeta]:
    """
    Returns:
        dict: peerId -> (agent_version, asn, country)

    Note:
        The dict is restricted to peers returned by `fetch_compromized_protocol_peer()`.
    """
    compromized_rows = fetch_compromized_protocol_peer()
    compromized_peer_ids = {peer_id for (peer_id, _set_id, _protocol_ids) in compromized_rows if peer_id}

    agent_map = _build_agent_map(fetch_agent_peer_count())
    asn_map = _build_asn_map(fetch_peer_id_prefix_by_asn())
    country_map = _build_country_map(fetch_peer_id_prefix_by_country())

    out: Dict[str, PeerMeta] = {}
    for peer_id in compromized_peer_ids:
        out[peer_id] = (
            agent_map.get(peer_id),
            asn_map.get(peer_id),
            country_map.get(peer_id),
        )
    return out


def main():
    data = build_compromized_peer_metadata()
    print("compromized_peers:", len(data))
    result = dict()
    for peer_id, (agent_version, asn, country) in data.items():
        result[agent_version] = result.get(agent_version, 0) + 1

    sorted_result = sorted(result.items(), key=lambda x: x[1], reverse=True)
    for agent_version, count in sorted_result:
        print(f"{agent_version:<60} {count:>20,}")


def remote_main():
    data = get_remote_data("/compromized-peer-metadata")
    print("compromized_peers:", len(data))
    # Print a few samples deterministically.

    result = dict()
    for peer_id, (agent_version, asn, country) in data.items():
        result[agent_version] = result.get(agent_version, 0) + 1

    sorted_result = sorted(result.items(), key=lambda x: x[1], reverse=True)
    for agent_version, count in sorted_result:
        print(f"{agent_version:<60} {count:>20,}")

if __name__ == "__main__":
    remote_main()

