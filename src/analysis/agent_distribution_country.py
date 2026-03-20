"""
Analyze agent distribution by country.

This mirrors the logic of `protocol_distribution_country.py`, but uses
agent versions instead of protocols:

- Agent data from `agent_peer_count.fetch_agent_peer_count()`
  -> (agent_version, multi_hash)
- Geography data from `multi_hash_prefix_country.fetch_peer_id_prefix_by_country()`
  -> (multi_hash, country)
"""
import sys
from pathlib import Path
from collections import defaultdict
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.agent_peer_count import fetch_agent_peer_count
from src.dbs.multi_hash_prefix_country import fetch_peer_id_prefix_by_country
from src.api.get_remote_data import get_remote_data


def build_agent_country_counts(agent_rows, peer_country_rows):
    """
    Join agent+multi_hash with multi_hash+country.
    Returns (country, agent) -> count of distinct multi_hash.
    """
    # multi_hash -> set of countries (a peer can have addresses in multiple countries)
    mh_to_countries: dict[str, set[str]] = defaultdict(set)
    for multi_hash, country in peer_country_rows:
        if country:
            mh_to_countries[multi_hash].add(country)

    # (country, agent) -> set of multi_hash for distinct count
    country_agent_hashes: dict[tuple[str, str], set[str]] = defaultdict(set)
    for agent, multi_hash in agent_rows:
        if not agent or not multi_hash:
            continue
        for country in mh_to_countries.get(multi_hash, []):
            country_agent_hashes[(country, agent)].add(multi_hash)

    # Convert to counts
    return {
        (country, agent): len(hashes)
        for (country, agent), hashes in country_agent_hashes.items()
    }


def build_country_agent_presence(agent_rows, peer_country_rows):
    """
    For each country, compute:
    - num_with_agent: number of distinct multi_hash that have an agent_version
    - num_without_agent: number of distinct multi_hash that do not have any agent_version
    Returns dict: country -> dict(with_agent=..., without_agent=..., total=...)
    """
    # All peers by country
    country_to_all_mh: dict[str, set[str]] = defaultdict(set)
    for multi_hash, country in peer_country_rows:
        if not country or not multi_hash:
            continue
        country_to_all_mh[country].add(multi_hash)

    # Peers that have an agent
    mh_with_agent: set[str] = set()
    for agent, multi_hash in agent_rows:
        if not multi_hash:
            continue
        mh_with_agent.add(multi_hash)

    country_stats: dict[str, dict[str, int]] = {}
    for country, all_mh in country_to_all_mh.items():
        with_agent = len(all_mh & mh_with_agent)
        total = len(all_mh)
        without_agent = total - with_agent
        country_stats[country] = {
            "with_agent": with_agent,
            "without_agent": without_agent,
            "total": total,
        }

    return country_stats


def print_country_agent_presence(country_stats, *, min_total: int = 1, max_countries: Optional[int] = None):
    """
    Print, to the command line, per-country counts of peers with and without agent.
    """
    # Filter by minimum total peers if requested
    rows = [
        (country, stats)
        for country, stats in country_stats.items()
        if stats["total"] >= min_total
    ]

    # Sort by total descending, then country code
    rows.sort(key=lambda x: (-x[1]["total"], x[0]))

    if max_countries is not None:
        rows = rows[:max_countries]

    header = f"{'Country':<8} {'Total':>12} {'With agent':>12} {'Without agent':>15}"
    sep = "-" * len(header)
    print("=== Per-country agent presence ===")
    print(header)
    print(sep)
    for country, stats in rows:
        print(
            f"{country:<8} "
            f"{stats['total']:>12,} "
            f"{stats['with_agent']:>12,} "
            f"{stats['without_agent']:>15,}"
        )
    print(sep)
    print(f"Total countries: {len(rows):,}")


def _agent_distribution_for_country(
    counts,
    country: str
):
    """
    Build rows for plotting agent distribution in one country.
    Returns list of dicts: country, agent, short_agent, count.
    """
    if not country:
        return []

    country_norm = country.strip().upper()
    rows: list[dict] = []
    for (c, agent), count in counts.items():
        if (c or "").strip().upper() != country_norm:
            continue
        if not agent:
            continue

        # Derive a shorter label for display, similar to `agent_peer_count.py` examples
        short_name = agent
        if len(short_name) > 50:
            short_name = short_name[:47] + "..."

        rows.append(
            {
                "country": c,
                "agent": agent,
                "short_agent": short_name,
                "count": count,
            }
        )

    rows.sort(key=lambda r: (-r["count"], r["agent"]))
    return rows


def plot_agent_distribution_for_country(
    country_rows,
    country: str,
    *,
    out_path: Optional[Path] = None,
    title: Optional[str] = None,
):
    """Plot agent distribution for a single country as a bar chart."""
    import plotly.express as px

    if not country_rows:
        print(f"No data to plot for country={country!r}.")
        return

    fig = px.bar(
        country_rows,
        x="short_agent",
        y="count",
        title=title or f"Agent distribution for {country}",
        hover_data={"agent": True, "short_agent": False},
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
            / f"agent_distribution_{safe_country}.png"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_image(str(out_path), width=1400, height=700, scale=2)
        print(f"Saved chart to {out_path}")
    except Exception as e:
        print(f"Could not save image ({e}); displaying interactively.")
    fig.show()


def _country_distribution_for_agent(
    counts,
    agent: str,
    top_n: int = 40,
    min_count: int = 1,
):
    """
    Build rows for plotting the distribution of one agent across countries.
    Returns list of dicts: agent, country, count.
    """
    if not agent:
        return []

    agent_norm = agent.strip()
    rows: list[dict] = []
    for (country, a), count in counts.items():
        if (a or "").strip() != agent_norm:
            continue
        if not country:
            continue
        if count < min_count:
            continue
        rows.append({"agent": agent_norm, "country": country, "count": count})

    rows.sort(key=lambda r: (-r["count"], r["country"]))
    if top_n is not None:
        rows = rows[:top_n]
    return rows


def _country_share_for_agent(counts,agent: str):
    """
    对于给定 agent，计算其在每个国家中占该国「总 agent 数量」的百分比。

    - counts: 来自 build_agent_country_counts()，(country, agent) -> peer 数量
    - agent: 目标 agent 版本字符串
    - min_country_total_agents: 只展示该国总 agent 数量 >= 此阈值的国家
    - top_n: 只保留前 top_n 个国家（按百分比排序）
    - min_count: 该 agent 在该国最少 peer 数量

    返回 list[dict]，包含:
    - country
    - agent
    - count: 该 agent 在该国的 peer 数量
    - total_agents: 该国所有 agent 的总 peer 数量
    - share: 该 agent 在该国总 agent 数量中的占比（0-100，float）
    """
    if not agent:
        return []

    agent_norm = agent.strip()

    # 先按国家聚合，得到每个国家的 agent 总数量
    country_totals: dict[str, int] = defaultdict(int)
    for (country, a), count in counts.items():
        if not country or not a:
            continue
        country_totals[country] += count

    rows: list[dict] = []
    for (country, a), count in counts.items():
        if (a or "").strip() != agent_norm:
            continue
        if not country:
            continue

        total_agents = country_totals.get(country, 0)

        share = 100.0 * count / total_agents
        rows.append(
            {
                "country": country,
                "agent": agent_norm,
                "count": count,
                "total_agents": total_agents,
                "share": share,
            }
        )

    # 按占比从大到小排序，其次按国家代码
    rows.sort(key=lambda r: (-r["share"], r["country"]))

    return rows


def calculate_country_top_agent_share(counts):
    """
    For each country, calculate:
    - top_agent_count: the maximum peer count among agents in that country
    - top_agent_share: percentage of that country's total agent peer counts

    Returns list[dict] with:
    - country
    - top_agent_count
    - total_agents
    - top_agent_share (0-100 float)
    """
    # Country total (sum over agents)
    country_totals: dict[str, int] = defaultdict(int)
    # Country top agent info
    country_max_count: dict[str, int] = defaultdict(int)
    country_max_agent: dict[str, str] = {}

    for (country, agent), count in counts.items():
        if not country or not agent:
            continue
        if not count:
            continue
        country_totals[country] += count
        prev_count = country_max_count[country]
        prev_agent = country_max_agent.get(country)
        # Deterministic tie-break: if counts equal, pick lexicographically smallest agent string.
        if count > prev_count or (count == prev_count and (prev_agent is None or agent < prev_agent)):
            country_max_count[country] = count
            country_max_agent[country] = agent

    rows: list[dict] = []
    for country, total in country_totals.items():
        top_count = country_max_count.get(country, 0)
        top_agent = country_max_agent.get(country, "")
        share = 100.0 * top_count / total if total else 0.0
        rows.append(
            {
                "country": country,
                "top_agent": top_agent,
                "top_agent_count": top_count,
                "total_agents": total,
                "top_agent_share": share,
            }
        )

    rows.sort(key=lambda r: (-r["top_agent_count"], -r["top_agent_share"], r["country"]))
    return rows


def print_country_top_agent_share(
    rows,
    *,
    max_countries: Optional[int] = None,
):
    """
    Print per-country top agent count and its share of that country's total agents.
    """
    if not rows:
        print("No country top-agent rows to display.")
        return

    if max_countries is not None:
        rows = rows[:max_countries]

    header = f"{'Country':<8} {'Top agent':<52} {'Top count':>12} {'Top share':>10}"
    sep = "-" * len(header)
    print("=== Per-country top agent share ===")
    print(header)
    print(sep)
    for r in rows:
        agent = (r.get("top_agent") or "").strip()
        if len(agent) > 52:
            agent = agent[:49] + "..."
        print(
            f"{r['country']:<8} "
            f"{agent:<52} "
            f"{r['top_agent_count']:>12,} "
            f"{r['top_agent_share']:>9.2f}%"
        )
    print(sep)
    print(f"Total countries: {len(rows):,}")


def plot_country_distribution_for_agent(
    agent_rows,
    agent: str,
    *,
    out_path: Optional[Path] = None,
    title: Optional[str] = None,
):
    """Plot distribution of an agent across countries as a bar chart."""
    import plotly.express as px

    if not agent_rows:
        print(f"No data to plot for agent={agent!r}.")
        return

    fig = px.bar(
        agent_rows,
        x="country",
        y="count",
        title=title or f"Agent distribution by country: {agent}",
        hover_data={"agent": True},
    )
    fig.update_layout(
        title=None,
        font=dict(size=18),
        xaxis=dict(title=None, title_font=dict(size=30), tickfont=dict(size=30), tickangle=-35),
        yaxis=dict(title=None, title_font=dict(size=30), tickfont=dict(size=30)),
        legend=dict(title=None, font=dict(size=16), title_font=dict(size=16)),
    )

    if out_path is None:
        safe_agent = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in agent.strip())
        out_path = (
            Path(__file__).resolve().parents[2]
            / "report"
            / "pics"
            / f"agent_country_distribution_{safe_agent}.png"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_image(str(out_path), width=1400, height=700, scale=2)
        print(f"Saved chart to {out_path}")
    except Exception as e:
        print(f"Could not save image ({e}); displaying interactively.")
    fig.show()


def plot_country_share_for_agent(
    agent_share_rows,
    agent: str,
    *,
    out_path: Optional[Path] = None,
    title: Optional[str] = None,
):
    """
    绘制：同一 agent 在不同国家中占本国总 agent 数量百分比的柱状图。
    y 轴为百分比，hover 中展示绝对数量信息。
    """
    import plotly.express as px

    if not agent_share_rows:
        print(f"No data to plot (share) for agent={agent!r}.")
        return

    fig = px.bar(
        agent_share_rows,
        x="country",
        y="share",
        title=title or f"Agent share by country (percentage of total agents): {agent}",
        hover_data={
            "agent": True,
            "count": True,
            "total_agents": True,
            "share": True,
        },
    )
    fig.update_layout(
        title=None,
        font=dict(size=18),
        xaxis=dict(title=None, title_font=dict(size=30), tickfont=dict(size=30), tickangle=-35),
        yaxis=dict(title="Share of agents (%)", title_font=dict(size=30), tickfont=dict(size=30)),
        legend=dict(title=None, font=dict(size=16), title_font=dict(size=16)),
    )

    if out_path is None:
        safe_agent = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in agent.strip())
        out_path = (
            Path(__file__).resolve().parents[2]
            / "report"
            / "pics"
            / f"agent_country_share_{safe_agent}.png"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_image(str(out_path), width=1400, height=700, scale=2)
        print(f"Saved chart to {out_path}")
    except Exception as e:
        print(f"Could not save image ({e}); displaying interactively.")
    fig.show()


def get_country_with_without_agent():
    agent_rows = fetch_agent_peer_count()
    peer_country_rows = fetch_peer_id_prefix_by_country()
    return build_country_agent_presence(agent_rows, peer_country_rows)

def get_agent_distribution_for_country(country: str):
    agent_rows = fetch_agent_peer_count()
    peer_country_rows = fetch_peer_id_prefix_by_country()
    counts = build_agent_country_counts(agent_rows, peer_country_rows)
    return _agent_distribution_for_country(counts, country)

def get_country_distribution_for_agent(agent: str):
    agent_rows = fetch_agent_peer_count()
    peer_country_rows = fetch_peer_id_prefix_by_country()
    counts = build_agent_country_counts(agent_rows, peer_country_rows)
    rows = _country_distribution_for_agent(counts, agent)
    return rows

def get_country_top_agent_share():
    agent_rows = fetch_agent_peer_count()
    peer_country_rows = fetch_peer_id_prefix_by_country()
    counts = build_agent_country_counts(agent_rows, peer_country_rows)
    return calculate_country_top_agent_share(counts)

def get_country_share_for_agent(agent: str):
    agent_rows = fetch_agent_peer_count()
    peer_country_rows = fetch_peer_id_prefix_by_country()
    counts = build_agent_country_counts(agent_rows, peer_country_rows)
    rows = _country_share_for_agent(counts, agent)
    return rows

def main():
    agent = "kubo/0.18.1/675f8bd/docker"
    country = "US"
    # CLI: show per-country counts of peers with / without agent
    country_stats = get_country_with_without_agent()
    print_country_agent_presence(country_stats, min_total=10)

    # Example: agent distribution for a single country
    cn_rows = get_agent_distribution_for_country(country)
    plot_agent_distribution_for_country(cn_rows, country)

    # Per-country: top agent count and its share of that country's total agents
    top_rows = get_country_top_agent_share()
    print_country_top_agent_share(top_rows, max_countries=10)

    # Example: single agent distribution across countries
    rows = get_country_distribution_for_agent(agent)
    plot_country_distribution_for_agent(rows, agent)

    # Example: agent share by country (percentage of total agents of all country)
    rows = get_country_share_for_agent(agent)
    plot_country_share_for_agent(rows, agent)

def remote_main():
    agent = "kubo/0.18.1/675f8bd/docker"
    country = "US"
    # CLI: show per-country counts of peers with / without agent
    country_stats = get_remote_data("/agent-country-with-without")
    print_country_agent_presence(country_stats, min_total=10)

    # Example: agent distribution for a single country
    cn_rows = get_remote_data("/agent-distribution-country?country=US")
    plot_agent_distribution_for_country(cn_rows, country)

    # Per-country: top agent count and its share of that country's total agents
    top_rows = get_remote_data("/agent-country-top-share")
    print_country_top_agent_share(top_rows, max_countries=10)

    # Example: single agent distribution across countries
    rows = get_remote_data("/agent-distribution-country?agent=kubo/0.18.1/675f8bd/docker")
    plot_country_distribution_for_agent(rows, agent)

    # Example: agent share by country (percentage of total agents of all country)
    rows = get_remote_data("/agent-country-share?agent=kubo/0.18.1/675f8bd/docker")
    plot_country_share_for_agent(rows, agent)

if __name__ == "__main__":
    remote_main()

# Country         Total   With agent   Without agent
# --------------------------------------------------
# CN              6,155          366           5,789
# US              3,269        2,463             806
# FR                925          732             193
# DE                866          607             259
# GB                483          291             192
# FI                366          311              55
# KR                310          244              66
# CA                307          247              60
# NL                270          199              71
# SG                262          189              73
# RU                235          148              87
# IN                170           75              95
# JP                168          123              45
# ES                162           85              77
# HK                140           88              52
# TW                128           68              60
# TH                114           60              54


# === Per-country top agent share ===
# Country  Top agent                                               Top count  Top share
# -------------------------------------------------------------------------------------
# US       kubo/0.22.0/3f884d3/gala.games                                551     22.36%
# CN       go-ipfs/0.8.0/48f94e2                                         233     64.01%
# FR       kubo/0.22.0/3f884d3/gala.games                                146     19.89%
# KR       go-ipfs/0.8.0/48f94e2                                          98     40.00%
# GB       kubo/0.39.0/                                                   47     16.67%
# DE       kubo/0.22.0/3f884d3/gala.games                                 40      6.58%
# FI       kubo/0.25.0/                                                   37     11.71%
# CA       kubo/0.37.0/6898472/docker                                     33     13.31%
# JP       kubo/0.22.0/3f884d3/gala.games                                 30     25.64%
# SG       kubo/0.22.0/3f884d3/gala.games                                 30     16.30%
# NL       kubo/0.22.0/3f884d3/gala.games                                 30     14.85%
# TW       go-ipfs/0.8.0/48f94e2                                          23     34.33%
# IN       kubo/0.32.1/                                                   23     31.51%
# ES       kubo/0.37.0/6898472/docker                                     23     26.14%