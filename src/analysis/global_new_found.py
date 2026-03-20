"""
Read output from read_multi_hashes_create_time.fetch_all_multi_hashes(),
then count multi_hash (peer) records per time bucket: start_time + k * step_length.
Default step_length is 1 hour (3600 seconds).
Display in UTC+8 (e.g. 2026-03-12 13:54:54+08) to avoid timezone confusion.
"""
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Union

import plotly.express as px
import plotly.graph_objects as go


sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.dbs.peers.read_multi_hashes_create_time import fetch_all_multi_hashes
from src.api.get_remote_data import get_remote_data

TZ_UTC8 = timezone(timedelta(hours=8))
start_time = 1741743294  # 2026-03-12 13:54:54.811238+08 -> 1741743294
step_length_seconds = 3600

def _to_timestamp(t) -> int:
    if hasattr(t, "timestamp"):
        return int(t.timestamp())
    return int(t)


def fetch_multi_hash_count_by_create_time(
    rows: list[tuple[str, object]],
    start_time: int=1741743294,
    step_length_seconds: int = 3600,
):
    """
    start_time: Unix timestamp (seconds); the beginning of the first bucket (earliest time).
    step_length_seconds: Length of each bucket in seconds (default 3600 = 1 hour).
    """
    bucket_count: dict[int, int] = {}
    for _multi_hash, created_at in rows:
        ts = _to_timestamp(created_at)
        if ts < start_time:
            continue
        bucket_id = (ts - start_time) // step_length_seconds
        bucket_count[bucket_id] = bucket_count.get(bucket_id, 0) + 1
    return bucket_count


def plot_multi_hash_count_by_create_time(
    rows: Optional[list[tuple[str, object]]] = None,
    start_time: int = 1741743294,
    step_length_seconds: int = 3600,
    output_path: Optional[Union[str, Path]] = None,
) -> go.Figure:
    """
    Plot a line chart: x = bucket start (+08), y = Count.
    Uses fetch_multi_hash_count_by_create_time to get bucket counts.
    """
    if rows is None:
        rows = fetch_all_multi_hashes()
    bucket_count = fetch_multi_hash_count_by_create_time(
        rows, start_time, step_length_seconds
    )
    xs = []
    ys = []
    for bucket_id in sorted(bucket_count.keys()):
        bucket_start_ts = start_time + bucket_id * step_length_seconds
        bucket_start_str = datetime.fromtimestamp(
            bucket_start_ts, tz=TZ_UTC8
        ).strftime("%Y-%m-%d %H:%M:%S+08")
        xs.append(bucket_start_str)
        ys.append(bucket_count[bucket_id])
    # Exclude the first point; plot from the second bucket onward
    fig = px.line(
        x=xs[1:],
        y=ys[1:],
        # labels={"x": "Timestamp (UTC+8)", "y": ""},
    )
    fig.update_traces(mode="lines+markers")
    fig.update_layout(
        xaxis_title=None,
        yaxis_title=None,
        xaxis_tickangle=-45,
        margin=dict(l=160, r=60, t=60, b=100),
        font=dict(size=42),  # global default font
        xaxis=dict(
            # title_font=dict(size=48),
            tickfont=dict(size=40),
        ),
        yaxis=dict(
            # title_font=dict(size=48),
            tickfont=dict(size=40),
        ),
        legend=dict(
            font=dict(size=20),
        ),
    )
    if output_path is not None:
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # If user passes an HTML path, keep interactive output; otherwise save static image for LaTeX/PDF.
        if out_path.suffix.lower() == ".html":
            fig.write_html(str(out_path))
        else:
            # Requires the 'kaleido' package to be installed.
            fig.write_image(str(out_path), width=1600, height=800, scale=2)
    return fig


def main():
    """
    start_time: Unix timestamp (seconds). If None, use the min created_at in the data.
    step_length_seconds: Length of each bucket in seconds (default 3600 = 1 hour).
    """
    rows = fetch_all_multi_hashes()  # 1773294894
    
    bucket_count = fetch_multi_hash_count_by_create_time(rows)
    
    print(f"\n=== multi_hash count by create_time (step = 1 hour) ===\n")
    print(f"{'Bucket start (+08)':<28} {'Bucket':>8} {'Count':>12}")
    print("-" * 50)

    for bucket_id in sorted(bucket_count.keys()):
        bucket_start_ts = start_time + bucket_id * step_length_seconds
        bucket_start_str = datetime.fromtimestamp(bucket_start_ts, tz=TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S+08")
        count = bucket_count[bucket_id]
        print(f"{bucket_start_str:<28} {bucket_id:>8} {count:>12,}")
    print("-" * 50)
    print(f"{'Total buckets':<28} {'':>8} {sum(bucket_count.values()):>12,}")

    # Optional: generate line chart (x = bucket start +08, y = Count)
    fig = plot_multi_hash_count_by_create_time(
        rows=rows,
        start_time=start_time,
        step_length_seconds=step_length_seconds,
        output_path=Path(__file__).resolve().parents[2]
        / "report"
        / "pics"
        / "global_new_found.png",
    )
    fig.show()

def remote_main():
    """
    start_time: Unix timestamp (seconds). If None, use the min created_at in the data.
    step_length_seconds: Length of each bucket in seconds (default 3600 = 1 hour).
    """
    
    rows = get_remote_data("/global-new-found")
    bucket_count = fetch_multi_hash_count_by_create_time(rows)
    
    print(f"\n=== multi_hash count by create_time (step = 1 hour) ===\n")
    print(f"{'Bucket start (+08)':<28} {'Bucket':>8} {'Count':>12}")
    print("-" * 50)

    for bucket_id in sorted(bucket_count.keys()):
        bucket_start_ts = start_time + bucket_id * step_length_seconds
        bucket_start_str = datetime.fromtimestamp(bucket_start_ts, tz=TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S+08")
        count = bucket_count[bucket_id]
        print(f"{bucket_start_str:<28} {bucket_id:>8} {count:>12,}")
    print("-" * 50)
    print(f"{'Total buckets':<28} {'':>8} {sum(bucket_count.values()):>12,}")

    # Optional: generate line chart (x = bucket start +08, y = Count)
    fig = plot_multi_hash_count_by_create_time(
        rows=rows,
        start_time=start_time,
        step_length_seconds=step_length_seconds,
        output_path=Path(__file__).resolve().parents[2]
        / "report"
        / "pics"
        / "global_new_found.png",
    )
    fig.show()


if __name__ == "__main__":
    remote_main()
