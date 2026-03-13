import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.express as px

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

def _bucket_count(value: int) -> str:
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
    """Return country/count/bucket data for multi_addresses."""
    conn = psycopg2.connect(
        host="db",
        port=5432,
        dbname="nebula_local",
        user="joshua",
        password="",
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    country,
                    COUNT(*) AS count
                FROM multi_addresses
                GROUP BY country
                ORDER BY count DESC;
                """
            )
            rows = cur.fetchall()

        countries = [row["country"] for row in rows if row["country"]]
        counts = [row["count"] for row in rows if row["country"]]
        buckets = [_bucket_count(count) for count in counts]
        countries_iso3 = [iso2_to_iso3.get(c, c) for c in countries]

        if not countries_iso3:
            return {"country": [], "count": [], "bucket": []}

        data = {
            "country": countries_iso3,
            "count": counts,
            "bucket": buckets,
        }
        return data
    finally:
        conn.close()


def main():
    data = fetch_geographical_data()
    if not data["country"]:
        return

    fig = px.choropleth(
        data,
        locations="country",
        color="bucket",
        locationmode="ISO-3",
        title="Multi addresses per country (bucketed)",
        hover_name="country",
        hover_data={"count": True, "bucket": True},
        category_orders={
            "bucket": ["0–9", "10–99", "100–999", "1k–9,999", "10k+"]
        },
        color_discrete_map={
            "0–9": "#f7fbff",
            "10–99": "#c6dbef",
            "100–999": "#6baed6",
            "1k–9,999": "#2171b5",
            "10k+": "#08306b",
        },
    )
    fig.show()


if __name__ == "__main__":
    main()

