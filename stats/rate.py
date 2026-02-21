"""
stats/rate.py
=============
Rate-related statistics:
  - percentile_rate()
  - most_prevailing_rate_on_ca()
  - create_rate_ranges()
  - get_floor_wise_90p_rate()
"""

import numpy as np
import pandas as pd


def percentile_rate(
    df: pd.DataFrame,
    property_type: str,
    column_name: str = "rate_on_net_ca",
) -> float:
    """
    Custom 90th-percentile matching MMA chart methodology.
    Linear interpolation between the two bracketing values.
    """
    segment = df[(df["property_type"] == property_type) & (df[column_name] > 0)]
    data = sorted(segment[column_name].tolist())
    n = len(data)
    if n == 0:
        return 0.0
    if n == 1:
        return data[0]
    index = (90 / 100) * (n - 1) + 1
    v1    = data[int(index) - 1]
    v2    = data[int(index)]
    diff  = round(index - int(index), 1)
    return round(v1 + diff * (v2 - v1), 2)


def most_prevailing_rate_on_ca(df: pd.DataFrame, property_type: str):
    """±5% band around the 90th-percentile rate on net carpet area."""
    segment = df[
        (df["property_type"] == property_type) & (df["rate_on_net_ca"] > 0)
    ]
    if segment.empty:
        return 0
    p90 = percentile_rate(segment, property_type)
    return f"{int(p90 * 0.95)}-{int(p90 * 1.05)}"


def _build_rate_bins(mean: int, interval: int = 1000):
    """7 rate-range buckets centred on mean, compatible with pd.cut."""
    pts    = [mean + o * interval for o in range(-3, 4)]
    edges  = [-np.inf] + pts + [np.inf]
    labels = (
        [f"<{pts[0]}"]
        + [f"{pts[i]}-{pts[i+1]}" for i in range(len(pts) - 1)]
        + [f">{pts[-1]}"]
    )
    return edges, labels


def create_rate_ranges(df: pd.DataFrame, property_type: str) -> dict:
    """Sold-unit counts per rate-range bucket — property-type wise."""
    valid = df[df["rate_on_net_ca"] > 0]
    if valid.empty:
        return {}

    rate_mean = valid["rate_on_net_ca"].mean()
    mean = 0 if pd.isna(rate_mean) else int(round(rate_mean, -2))

    segment = df[
        (df["property_type"] == property_type) & (df["rate_on_net_ca"] > 0)
    ].copy()
    if segment.empty:
        return {}

    edges, labels = _build_rate_bins(mean)
    segment["rate_range"] = pd.cut(
        segment["rate_on_net_ca"], bins=edges, labels=labels, right=False
    )
    result = segment["rate_range"].value_counts().to_dict()
    return {k: v for k, v in result.items() if pd.notna(v) and v != 0}


def get_floor_wise_90p_rate(df: pd.DataFrame, floor_interval: int = 5) -> dict:
    """
    90th-percentile rate_on_net_ca grouped into floor ranges.
    Returns {'0-5': 8500.0, '5-10': 9200.0, ...}
    Only buckets with data are returned. Rows with missing floor_no are ignored.
    """
    df = df[(df["rate_on_net_ca"] > 0) & (df["floor_no"].notna())].copy()
    if df.empty:
        return {}

    max_floor = int(df["floor_no"].max())
    upper     = ((max_floor // floor_interval) + 1) * floor_interval
    edges     = list(range(0, upper + floor_interval, floor_interval))
    labels    = [f"{edges[i]}-{edges[i+1]}" for i in range(len(edges) - 1)]

    df["floor_range"] = pd.cut(df["floor_no"], bins=edges, labels=labels, right=False)

    result = (
        df.groupby("floor_range", observed=False)["rate_on_net_ca"]
        .quantile(0.90)
        .round(2)
        .to_dict()
    )
    return {str(k): float(v) for k, v in result.items() if pd.notna(v)}


def floor_wise_wrapper(df: pd.DataFrame, segment_val) -> dict:
    """
    Thin wrapper for apply_and_merge compatibility.
    segment_val is already isolated by the groupby — just forwarded.
    """
    return get_floor_wise_90p_rate(df)