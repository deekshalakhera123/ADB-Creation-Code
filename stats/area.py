"""
stats/area.py
=============
Area-range statistics:
  - get_dynamic_area_ranges_generic()
  - create_area_range_stats()
  - Public aliases for property-type wise and BHK wise
"""

import pandas as pd


def _area_bins(mean: int, interval: int = 200):
    """6 area-range buckets around mean, compatible with pd.cut."""
    pts    = [mean + o * interval for o in range(-2, 3)]
    edges  = [float("-inf")] + pts + [float("inf")]
    labels = (
        [f"<{pts[0]}"]
        + [f"{pts[i]}-{pts[i+1]}" for i in range(len(pts) - 1)]
        + [f">{pts[-1]}"]
    )
    return edges, labels


def get_dynamic_area_ranges_generic(
    df: pd.DataFrame,
    target_col: str = "carpet_sqft",
    interval: int = 200,
) -> pd.Series:
    """Vectorised area-range assignment via pd.cut."""
    df = df[df["carpet_sqft"] > 0]
    valid = df[df[target_col] != 0][target_col]
    if valid.empty:
        return pd.Series([None] * len(df), index=df.index)
    rate_mean = valid.mean()
    mean = 0 if pd.isna(rate_mean) else int(round(rate_mean, -2))
    edges, labels = _area_bins(mean, interval)
    return pd.cut(df[target_col], bins=edges, labels=labels, right=False)


def _filter_and_assign_area_ranges(
    df: pd.DataFrame, filter_col: str, filter_val
) -> pd.DataFrame:
    """Filter, drop zero-carpet rows, attach 'area_range'. Returns empty df on failure."""
    if df.empty:
        return df
    df_f = df[df[filter_col] == filter_val].copy() if filter_col in df.columns else df.copy()
    if df_f.empty:
        return df_f
    df_f = df_f[df_f["carpet_sqft"] > 0]
    df_f["area_range"] = get_dynamic_area_ranges_generic(df_f, "carpet_sqft")
    return df_f


def create_area_range_stats(
    df: pd.DataFrame,
    filter_col: str,
    filter_val,
    agg: str = "count",
    value_col: str = "agreement_price",
) -> dict:
    """
    Generic area-range statistics engine.
    agg : 'count' | 'sum' | 'mean'
    Empty buckets (NaN / zero) are stripped from output.
    """
    df_f = _filter_and_assign_area_ranges(df, filter_col, filter_val)
    if df_f.empty:
        return {}

    if agg == "count":
        result = df_f["area_range"].value_counts().to_dict()
    elif agg == "sum":
        result = df_f.groupby("area_range", observed=False)[value_col].sum().to_dict()
    elif agg == "mean":
        result = df_f.groupby("area_range", observed=False)[value_col].mean().round(2).to_dict()
    else:
        raise ValueError(f"Unsupported agg '{agg}'. Choose: 'count', 'sum', 'mean'.")

    return {k: v for k, v in result.items() if pd.notna(v) and v != 0}


# ── Property-type wise aliases ────────────────────────────────────────────────

def create_area_ranges(df, property_type):
    """Sold units count — property-type wise."""
    return create_area_range_stats(df, "property_type", property_type, agg="count")

def create_area_ranges_carpet(df, property_type):
    """Carpet area consumed (sum) — property-type wise."""
    return create_area_range_stats(df, "property_type", property_type, agg="sum", value_col="carpet_sqft")

def create_area_ranges_sales(df, property_type):
    """Total agreement price (sum) — property-type wise."""
    return create_area_range_stats(df, "property_type", property_type, agg="sum", value_col="agreement_price")

def create_area_ranges_avg_sales(df, property_type):
    """Average agreement price — property-type wise."""
    return create_area_range_stats(df, "property_type", property_type, agg="mean", value_col="agreement_price")


# ── BHK wise aliases ──────────────────────────────────────────────────────────

def create_area_ranges_unit_sold(df, BHK):
    """Sold units count — BHK wise."""
    return create_area_range_stats(df, "bhk", BHK, agg="count")

def create_area_ranges_unit_carpet(df, BHK):
    """Carpet area consumed (sum) — BHK wise."""
    return create_area_range_stats(df, "bhk", BHK, agg="sum", value_col="carpet_sqft")

def create_area_ranges_unit_sales(df, BHK):
    """Total agreement price (sum) — BHK wise."""
    return create_area_range_stats(df, "bhk", BHK, agg="sum", value_col="agreement_price")

def create_area_ranges_unit_avg_sales(df, BHK):
    """Average agreement price — BHK wise."""
    return create_area_range_stats(df, "bhk", BHK, agg="mean", value_col="agreement_price")