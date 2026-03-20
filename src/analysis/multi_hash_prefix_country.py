"""
Read all multi_hash from peers table; count those starting with Qm vs 12D3 and check for other prefixes.
Per country (from multi_addresses), count peerIds (peers.multi_hash) starting with Qm / 12D3.
"""
import sys
from pathlib import Path

from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.multi_hash_prefix_country import fetch_peer_id_prefix_by_country
from src.api.get_remote_data import get_remote_data

# Example output:
# None: country is not selected
# ('12D3KooW9pykygUHigHGbN123J26Uq91PmBEw4C8n1ScqPzLQcdf', 'US')
# ('12D3KooW9q6VFjzk9k95j9CdZU9QGCx495HxsGxqU8u6FsD1G75R', 'US')
# ('12D3KooW9pPUBnqbkTEQhUNKzs2R3ZYeeTVHg542Phawj4sPWQg3', 'DE', 'US')
# ('12D3KooW9pVusgEF3YPqnmyXAQ5fNXe9AGcd3hs9BWsgbTGzaUyK', 'SG', 'US')
def split_peer_ids_by_country_count(rows):
    """
    Split (multi_hash, country) pairs into:
    - single_country: list of (multi_hash, country) for peerIds that appear in exactly one country
    - multi_country: list of (multi_hash, country1, country2, ...) for peerIds that appear in more than one country
    """
    by_peer = defaultdict(set)
    for multi_hash, country in rows:
        if country is None:
            continue
        by_peer[multi_hash].add(country)

    single_country = []
    multi_country = []
    for multi_hash, countries in by_peer.items():
        countries_tuple = tuple(sorted(countries))
        if len(countries_tuple) == 1:
            single_country.append((multi_hash, countries_tuple[0]))
        else:
            multi_country.append((multi_hash,) + countries_tuple)
    return single_country, multi_country

def get_peer_id_prefix_by_country():
    rows = fetch_peer_id_prefix_by_country()
    by_country = defaultdict(lambda: {"Qm": 0, "12D3": 0})
    for multi_hash, country in rows:
        if country is None:
            continue
        if multi_hash.startswith("Qm"):
            by_country[country]["Qm"] += 1
        elif multi_hash.startswith("12D3"):
            by_country[country]["12D3"] += 1

    # Extra analysis: countries with Total > 100, sorted by Qm / Total descending
    filtered_countries = []
    for country in by_country:
        qm = by_country[country]["Qm"]
        d3 = by_country[country]["12D3"]
        total = qm + d3
        ratio = qm / total
        filtered_countries.append((country, qm, d3, total, ratio))

    return filtered_countries

def print_peer_id_prefix_by_country(filtered_countries):
    if filtered_countries:
        print()
        print("Countries with Total > 100, sorted by Qm/Total desc")
        print(f"{'Country':<6} {'Qm':>10} {'12D3':>10} {'Total':>10} {'Qm/Total':>10}")
        print("-" * 60)
        for country, qm, d3, total, ratio in sorted(
            filtered_countries, key=lambda x: x[4], reverse=True
        ):
            print(f"{country:<6} {qm:>10,} {d3:>10,} {total:>10,} {ratio*100:>9.2f}%")

def main():
    """
    Per-country counts of peerIds (multi_hash from peers) starting with Qm or 12D3.
    Uses join via peers_x_multi_addresses (read_peers + read_multi_addresses).
    """
    # Extra analysis: countries with Total > 100, sorted by Qm / Total descending
    filtered_countries = get_peer_id_prefix_by_country()
    print_peer_id_prefix_by_country(filtered_countries)

def remote_main():
    filtered_countries = get_remote_data("/multi-hash-prefix-country")
    print_peer_id_prefix_by_country(filtered_countries)

if __name__ == "__main__":
    remote_main()


# Countries with Total > 100, sorted by Qm/Total desc
# Country         Qm       12D3      Total   Qm/Total
# ------------------------------------------------------------
# TW             79         61        140     56.43%
# HK             81         69        150     54.00%
# KR            125        168        293     42.66%
# SG             32        205        237     13.50%
# FI             45        307        352     12.78%
# RU             25        209        234     10.68%
# CN            435      4,100      4,535      9.59%
# DE             76        720        796      9.55%
# US            281      2,754      3,035      9.26%
# FR             76        773        849      8.95%
# JP             11        147        158      6.96%
# IN             11        158        169      6.51%
# GB             25        407        432      5.79%
# ES              8        136        144      5.56%
# NL             12        206        218      5.50%
# CA             11        270        281      3.91%