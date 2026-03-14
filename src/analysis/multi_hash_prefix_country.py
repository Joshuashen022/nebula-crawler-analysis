"""
从 peers 表读取所有 multi_hash，统计 Qm 开头、12D3 开头的数量，并检查是否存在其他前缀。
按国家（country，来自 multi_addresses）统计 peerId（peers.multi_hash）以 Qm / 12D3 开头的数量。
"""
import sys
from pathlib import Path

from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.multi_hash_prefix_country import fetch_peer_id_prefix_by_country

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


def main():
    """
    按国家统计：来自 peers 的 peerId (multi_hash) 以 Qm、12D3 开头的数量。
    使用 read_peers + read_multi_addresses 的关联（peers_x_multi_addresses）。
    """
    rows = fetch_peer_id_prefix_by_country()
    # per country: {"Qm": count, "12D3": count}
    by_country = defaultdict(lambda: {"Qm": 0, "12D3": 0})
    for multi_hash, country in rows:
        if country is None:
            continue
        if multi_hash.startswith("Qm"):
            by_country[country]["Qm"] += 1
        elif multi_hash.startswith("12D3"):
            by_country[country]["12D3"] += 1

    # sort by total (Qm + 12D3) descending
    sorted_countries = sorted(
        by_country.keys(),
        key=lambda c: by_country[c]["Qm"] + by_country[c]["12D3"],
        reverse=True,
    )
    print(f"{'Country':<6} {'Qm':>10} {'12D3':>10} {'Total':>10}")
    print("-" * 40)
    for country in sorted_countries:
        qm = by_country[country]["Qm"]
        d3 = by_country[country]["12D3"]
        print(f"{country:<6} {qm:>10,} {d3:>10,} {qm + d3:>10,}")
    total_qm = sum(by_country[c]["Qm"] for c in by_country)
    total_d3 = sum(by_country[c]["12D3"] for c in by_country)
    print("-" * 40)
    print(f"{'Total':<6} {total_qm:>10,} {total_d3:>10,} {total_qm + total_d3:>10,}")



if __name__ == "__main__":
    main()


# Country         Qm       12D3      Total
# ----------------------------------------
# US            194      2,171      2,365
# FR             70        601        671
# DE             73        488        561
# GB             23        250        273
# CN            131        141        272
# FI             40        231        271
# CA             10        165        175
# SG             28        142        170
# KR             44        124        168
# NL             11        153        164
# JP             11        116        127
# RU             10         98        108
# ES              5         85         90
# IN              8         63         71
# HK             22         41         63
# TW             22         39         61
# SE             10         50         60
# AU              5         53         58
# TH              0         57         57
# PL             10         45         55
# CH             10         44         54
# AT              3         40         43
# UA              6         23         29
# BR              7         22         29
# AE              0         29         29
# IT              1         27         28
# PT              3         21         24
# MX              8         16         24
# VN              2         20         22
# AR              0         21         21
# ID              1         19         20
# IE              3         17         20
# LT              4         14         18
# TR              1         12         13
# LV              3          8         11
# BE              0         11         11
# CZ              0         10         10
# NO              2          7          9
# HU              2          7          9
# RO              0          9          9
# RS              0          8          8
# GR              0          8          8
# BG              3          5          8
# DK              0          7          7
# IL              1          6          7
# MY              0          7          7
# NZ              0          5          5
# ZA              1          4          5
# PA              3          2          5
# CL              1          3          4
# IS              0          4          4
# BD              0          4          4
# SI              0          3          3
# SK              0          3          3
# CO              1          2          3
# GE              1          2          3
# MT              0          2          2
# RE              0          2          2
# EE              0          2          2
# PK              0          2          2
# LU              0          2          2
# KW              0          2          2
# KZ              0          2          2
# SC              0          2          2
# IM              0          1          1
# PR              0          1          1
# MN              0          1          1
# MD              0          1          1
# VG              0          1          1
# HR              0          1          1
# CW              0          1          1
# AQ              0          1          1
# CY              0          1          1
# BO              0          1          1
# KH              0          1          1
# DO              0          1          1
# BY              0          1          1
# MO              0          1          1
# AM              1          0          1
# AL              0          1          1
# TN              0          1          1
# PH              0          1          1
# VE              1          0          1
# ----------------------------------------
# Total         796      5,596      6,392