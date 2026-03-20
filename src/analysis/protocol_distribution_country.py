"""
Analyze protocol distribution by country: for each nation, compute the top 3
most popular protocols and display as a grouped bar chart (similar to
peer_uptime_country.py).

Uses:
- Protocol data from protocol_peer_count.fetch_protocol_peer_count()
- Geography data from multi_hash_prefix_country.fetch_peer_id_prefix_by_country()
"""
import sys
from pathlib import Path
from collections import defaultdict
from typing import Optional

import plotly.express as px

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.protocol_peer_count import fetch_protocol_peer_count
from src.dbs.multi_hash_prefix_country import fetch_peer_id_prefix_by_country
from src.api.get_remote_data import get_remote_data


def build_protocol_country_counts(protocol_rows, peer_country_rows):
    """
    Join protocol+multi_hash with multi_hash+country.
    Returns (country, protocol) -> count of distinct multi_hash.
    """
    # multi_hash -> set of countries (a peer can have addresses in multiple countries)
    mh_to_countries: dict[str, set[str]] = defaultdict(set)
    for multi_hash, country in peer_country_rows:
        if country:
            mh_to_countries[multi_hash].add(country)

    # (country, protocol) -> set of multi_hash for distinct count
    country_protocol_hashes: dict[tuple[str, str], set[str]] = defaultdict(set)
    for protocol, multi_hash in protocol_rows:
        if not protocol or not multi_hash:
            continue
        for country in mh_to_countries.get(multi_hash, []):
            country_protocol_hashes[(country, protocol)].add(multi_hash)

    # Convert to counts
    return {
        (country, protocol): len(hashes)
        for (country, protocol), hashes in country_protocol_hashes.items()
    }


# def get_top3_protocols_per_country(counts, min_peers: int = 50, max_countries: Optional[int] = None):
#     """
#     For each country, get the top 3 protocols by peer count.
#     Only include countries with at least min_peers total.
#     If max_countries is set, limit to that many countries (by total peer count).
#     Returns list of dicts for plotting: country, rank, protocol, count.
#     """
#     # Aggregate by country first to filter
#     country_totals = defaultdict(int)
#     for (country, protocol), count in counts.items():
#         country_totals[country] += count

#     # Per-country protocol counts
#     country_protocols: dict[str, list[tuple[str, int]]] = defaultdict(list)
#     for (country, protocol), count in counts.items():
#         if country_totals[country] >= min_peers:
#             country_protocols[country].append((protocol, count))

#     # Sort countries by total desc, optionally limit
#     sorted_countries = sorted(
#         country_protocols.keys(),
#         key=lambda c: (-country_totals[c], c),
#     )
#     if max_countries is not None:
#         sorted_countries = sorted_countries[:max_countries]

#     # Sort each country's protocols by count desc, take top 3
#     plot_rows = []
#     for country in sorted_countries:
#         sorted_protocols = sorted(
#             country_protocols[country],
#             key=lambda x: (-x[1], x[0]),
#         )[:3]
#         for rank, (protocol, count) in enumerate(sorted_protocols, 1):
#             # Shorten protocol for display (e.g. /ipfs/kad/1.0.0 -> kad)
#             short_name = protocol.split("/")[-2] if "/" in protocol else protocol
#             if not short_name:
#                 short_name = protocol[:20] + "…" if len(protocol) > 20 else protocol
#             plot_rows.append(
#                 {
#                     "country": country,
#                     "count": count,
#                     "rank": f"Top {rank}",
#                     "protocol": protocol,
#                     "short_protocol": short_name,
#                 }
#             )

#     return plot_rows


def plot_protocol_distribution_by_country(plot_rows):
    """Build a grouped bar chart for top 3 protocols per country."""
    if not plot_rows:
        print("No data to plot.")
        return

    fig = px.bar(
        plot_rows,
        x="country",
        y="count",
        color="rank",
        barmode="group",
        title="Top 3 protocols by country (peer count)",
        hover_data={"protocol": True, "short_protocol": False},
    )
    fig.update_layout(
        xaxis_title="Country",
        yaxis_title="Count",
        legend_title="Rank",
    )
    out_path = Path(__file__).resolve().parents[2] / "report" / "pics" / "protocol_distribution_country.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_image(str(out_path), width=1400, height=700, scale=2)
        print(f"Saved chart to {out_path}")
    except Exception as e:
        print(f"Could not save image ({e}); displaying interactively.")
    fig.show()


def _protocol_distribution_for_country(
    counts,
    country: str,
    top_n: int = 20,
    min_count: int = 1,
):
    """
    Build rows for plotting protocol distribution in one country.
    Returns list of dicts: country, protocol, short_protocol, count.
    """
    if not country:
        return []

    country_norm = country.strip().upper()
    rows: list[dict] = []
    for (c, protocol), count in counts.items():
        if (c or "").strip().upper() != country_norm:
            continue
        if not protocol:
            continue
        if count < min_count:
            continue
        short_name = protocol.split("/")[-2] if "/" in protocol else protocol
        if not short_name:
            short_name = protocol[:20] + "…" if len(protocol) > 20 else protocol
        rows.append(
            {
                "country": c,
                "protocol": protocol,
                "short_protocol": short_name,
                "count": count,
            }
        )

    rows.sort(key=lambda r: (-r["count"], r["protocol"]))
    if top_n is not None:
        rows = rows[:top_n]
    return rows


def plot_protocol_distribution_for_country(
    country_rows,
    country: str,
    *,
    out_path: Optional[Path] = None,
    title: Optional[str] = None,
):
    """Plot protocol distribution for a single country as a bar chart."""
    if not country_rows:
        print(f"No data to plot for country={country!r}.")
        return

    fig = px.bar(
        country_rows,
        x="short_protocol",
        y="count",
        title=title or f"Protocol distribution for {country}",
        hover_data={"protocol": True, "short_protocol": False},
    )

    fig.update_layout(
        title=None,
        font=dict(size=18),
        xaxis=dict(title=None, title_font=dict(size=30), tickfont=dict(size=30), tickangle=-35),
        yaxis=dict(title=None, title_font=dict(size=30), tickfont=dict(size=30)),
        legend=dict(title=None, font=dict(size=16), title_font=dict(size=16)),
    )

    if out_path is None:
        safe_country = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in country.strip())
        out_path = (
            Path(__file__).resolve().parents[2]
            / "report"
            / "pics"
            / f"protocol_distribution_{safe_country}.png"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_image(str(out_path), width=1400, height=700, scale=2)
        print(f"Saved chart to {out_path}")
    except Exception as e:
        print(f"Could not save image ({e}); displaying interactively.")
    fig.show()


def _country_distribution_for_protocol(
    counts,
    protocol: str,
    top_n: int = 40,
    min_count: int = 1,
):
    """
    Build rows for plotting the distribution of one protocol across countries.
    Returns list of dicts: protocol, country, count.
    """
    if not protocol:
        return []

    protocol_norm = protocol.strip()
    rows: list[dict] = []
    for (country, p), count in counts.items():
        if (p or "").strip() != protocol_norm:
            continue
        if not country:
            continue
        if count < min_count:
            continue
        rows.append({"protocol": protocol_norm, "country": country, "count": count})

    rows.sort(key=lambda r: (-r["count"], r["country"]))
    if top_n is not None:
        rows = rows[:top_n]
    return rows


def plot_country_distribution_for_protocol(
    protocol_rows,
    protocol: str,
    *,
    out_path: Optional[Path] = None,
    title: Optional[str] = None,
):
    """Plot distribution of a protocol across countries as a bar chart."""
    if not protocol_rows:
        print(f"No data to plot for protocol={protocol!r}.")
        return

    fig = px.bar(
        protocol_rows,
        x="country",
        y="count",
        title=title or f"Protocol distribution by country: {protocol}",
        hover_data={"protocol": True},
    )
    fig.update_layout(
        title=None,
        font=dict(size=18),
        xaxis=dict(title=None, title_font=dict(size=30), tickfont=dict(size=30), tickangle=-35),
        yaxis=dict(title=None, title_font=dict(size=30), tickfont=dict(size=30)),
        legend=dict(title=None, font=dict(size=16), title_font=dict(size=16)),
    )

    if out_path is None:
        safe_protocol = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in protocol.strip())
        out_path = (
            Path(__file__).resolve().parents[2]
            / "report"
            / "pics"
            / f"protocol_country_distribution_{safe_protocol}.png"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_image(str(out_path), width=1400, height=700, scale=2)
        print(f"Saved chart to {out_path}")
    except Exception as e:
        print(f"Could not save image ({e}); displaying interactively.")
    fig.show()


def get_protocol_distribution_for_country(country: str, top_n: int = 30):
    protocol_rows = fetch_protocol_peer_count()
    peer_country_rows = fetch_peer_id_prefix_by_country()

    counts = build_protocol_country_counts(protocol_rows, peer_country_rows)

    cn_rows = _protocol_distribution_for_country(counts, country, top_n=top_n)
    return cn_rows

def get_country_distribution_for_protocol(
    protocol: str,
    *,
    top_n: int = 40,
    min_count: int = 1,
):
    """
    One-shot helper: fetch data, build counts, then plot a single protocol's
    distribution across countries.
    """
    protocol_rows = fetch_protocol_peer_count()
    peer_country_rows = fetch_peer_id_prefix_by_country()
    counts = build_protocol_country_counts(protocol_rows, peer_country_rows)

    rows = _country_distribution_for_protocol(
        counts,
        protocol,
        top_n=top_n,
        min_count=min_count,
    )
    return rows


def main():
    # protocol distribution for a single country
    rows = get_protocol_distribution_for_country("US", top_n=30)
    plot_protocol_distribution_for_country(rows, "US")

    # single protocol distribution across countries
    rows =get_country_distribution_for_protocol("/sbptp/1.0.0", top_n=40)
    plot_country_distribution_for_protocol(rows,"/sbptp/1.0.0",)

def remote_main():
    rows = get_remote_data("/protocol-distribution-country?country=US", top_n=30)
    plot_protocol_distribution_for_country(rows, "US")
    rows =get_remote_data("/country-distribution-protocol?protocol=sbptp/1.0.0", top_n=40)
    plot_country_distribution_for_protocol(rows,"sbptp/1.0.0",)


if __name__ == "__main__":
    remote_main()
