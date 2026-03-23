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
            if total > 100:
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


# Country         Qm       12D3      Total   Qm/Total
# ------------------------------------------------------------
# TW            109         70        179     60.89%
# HK            112         88        200     56.00%
# KR            196        193        389     50.39%
# UA             24         98        122     19.67%
# RU             49        279        328     14.94%
# SE             15         87        102     14.71%
# CH             14        100        114     12.28%
# US            456      3,883      4,339     10.51%
# PL             12        105        117     10.26%
# SG             31        277        308     10.06%
# FI             48        500        548      8.76%
# DE             83        974      1,057      7.85%
# AU              8         98        106      7.55%
# FR             75      1,031      1,106      6.78%
# JP             13        187        200      6.50%
# ES             16        243        259      6.18%
# CN            829     12,599     13,428      6.17%
# CA             17        369        386      4.40%
# IN             12        268        280      4.29%
# NL             14        321        335      4.18%
# GB             26        992      1,018      2.55%
# TH              1        139        140      0.71%

# remote
# Country         Qm       12D3      Total   Qm/Total
# ------------------------------------------------------------
# TW            103         66        169     60.95%
# HK            108         74        182     59.34%
# KR            206        175        381     54.07%
# UA             25        105        130     19.23%
# SG             32        218        250     12.80%
# CH             12         89        101     11.88%
# RU             37        289        326     11.35%
# FI             43        406        449      9.58%
# US            333      3,312      3,645      9.14%
# DE             78        790        868      8.99%
# JP             15        166        181      8.29%
# AU              8         93        101      7.92%
# FR             74        881        955      7.75%
# CN            626      8,712      9,338      6.70%
# ES             15        211        226      6.64%
# CA             21        323        344      6.10%
# IN             13        224        237      5.49%
# PL              4         97        101      3.96%
# NL             12        292        304      3.95%
# GB             27        861        888      3.04%
# TH              0        109        109      0.00%