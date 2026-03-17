from collections import Counter

import plotly.express as px
import sys
from pathlib import Path

from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.multi_hash_prefix_country import fetch_peer_id_prefix_by_country

iso2_to_iso3 = {
    "US": "USA", "DE": "DEU", "FR": "FRA", "FI": "FIN", "CA": "CAN",
    "GB": "GBR", "ES": "ESP", "NL": "NLD", "CN": "CHN", "VN": "VNM",
    "RU": "RUS", "SG": "SGP", "AE": "ARE", "AR": "ARG", "AT": "AUT",
    "PT": "PRT", "PL": "POL", "JP": "JPN", "KR": "KOR", "AU": "AUS",
    "CH": "CHE", "SE": "SWE", "HK": "HKG", "IT": "ITA", "TW": "TWN",
    "CZ": "CZE", "TH": "THA", "MX": "MEX", "IN": "IND", "BE": "BEL",
    "NO": "NOR", "ID": "IDN", "IE": "IRL", "BR": "BRA", "NZ": "NZL",
    "DK": "DNK", "DO": "DOM", "LT": "LTU", "UA": "UKR", "SI": "SVN",
    "BG": "BGR", "MY": "MYS", "AL": "ALB", "GR": "GRC", "RS": "SRB",
    "SK": "SVK", "IS": "ISL", "GE": "GEO", "PR": "PRI", "IL": "ISR",
    "RO": "ROU", "LU": "LUX", "LV": "LVA", "TR": "TUR", "KW": "KWT",
    "HR": "HRV", "EE": "EST", "HU": "HUN", "CY": "CYP", "ZA": "ZAF",
    "CL": "CHL", "SC": "SYC", "PA": "PAN", "IM": "IMN", "CW": "CUW",
    "PK": "PAK", "MD": "MDA", "TT": "TTO", "CO": "COL", "BD": "BGD",
    "PH": "PHL", "KH": "KHM", "VG": "VGB", "AQ": "ATA", "MN": "MNG",
    "BY": "BLR", "UY": "URY", "IQ": "IRQ", "MT": "MLT"
}

BRACKET_ORDER = ["0–9", "10–99", "100–999", "1k–9,999", "10k+"]


def _bracket_count(value: int) -> str:
    if value < 10:
        return "0–9"
    if value < 100:
        return "10–99"
    if value < 1000:
        return "100–999"
    if value < 10000:
        return "1k–9,999"
    return "10k+"


def fetch_geographical_data():
    """Return country/count/bracket data from peer_id prefix by country (ISO2 from fetch_peer_id_prefix_by_country)."""
    rows = fetch_peer_id_prefix_by_country()  # list of (multi_hash, country), country in ISO2
    country_counts = Counter()
    for _peer_id, country in rows:
        if country:
            country_counts[country] += 1
    sorted_items = country_counts.most_common()
    countries = [c for c, _ in sorted_items]
    counts = [n for _, n in sorted_items]
    brackets = [_bracket_count(count) for count in counts]
    countries_iso3 = [iso2_to_iso3.get(c, c) for c in countries]

    if not countries_iso3:
        return {"country": [], "count": [], "bracket": []}

    data = {
        "country": countries_iso3,
        "count": counts,
        "bracket": brackets,
    }
    return data


def print_geographical_analysis(data: dict) -> None:
    """Print summary, Top 15, and bracket distribution (same format as API analysis)."""
    countries = data["country"]
    counts = data["count"]
    brackets = data.get("bracket") or []

    total_addresses = sum(counts)
    n_countries = len(countries)

    print("=== Geographical API Analysis ===\n")
    print(f"Countries/regions: {n_countries}")
    print(f"Total multi-addresses: {total_addresses:,}\n")

    print("--- Top 15 by count ---")
    for i, (c, n) in enumerate(zip(countries, counts), 1):
        if i > 15:
            break
        bracket = brackets[i - 1] if i <= len(brackets) else ""
        print(f"  {i:2}. {c}: {n:,}  ({bracket})")

    if brackets:
        print("\n--- Bracket distribution ---")
        bracket_counts = Counter(brackets)
        for b in BRACKET_ORDER:
            if b in bracket_counts:
                print(f"  {b}: {bracket_counts[b]} countries/regions")


def main():
    data = fetch_geographical_data()
    if not data["country"]:
        return

    print_geographical_analysis(data)

    fig = px.choropleth(
        data,
        locations="country",
        color="bracket",
        locationmode="ISO-3",
        hover_name="country",
        hover_data={"count": True, "bracket": True},
        category_orders={"bracket": BRACKET_ORDER},
        color_discrete_map={
            "0–9": "#ffffcc",   # pale yellow
            "10–99": "#a1dab4",  # light green
            "100–999": "#41b6c4",  # teal
            "1k–9,999": "#225ea8",  # blue
            "10k+": "#081d58",   # dark blue
        },
    )
    fig.update_layout(
        legend_title_text="",
        legend=dict(
            x=1.0,
            y=0.5,
            xanchor="left",
            yanchor="middle",
            font=dict(size=50),
            itemsizing="constant",
            itemwidth=36,
        ),
        margin=dict(r=220),
    )
    out_path = Path(__file__).resolve().parents[2] / "report" / "pics" / "global_geographical.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(out_path), width=1600, height=800, scale=2)
    # fig.show()


if __name__ == "__main__":
    main()

