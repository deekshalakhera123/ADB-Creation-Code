"""
stats/area.py
=============
Area-range statistics engine.

Supports:
    - Mean-based bins (project level)
    - User-defined min/max bins (location/city level)
    - Configurable interval
"""

import pandas as pd
from config import AREA_STEP


# ============================================================
# BIN BUILDERS
# ============================================================

def _area_bins_mean(mean: int, interval: int = AREA_STEP):
    """6 area-range buckets around mean."""
    pts = [mean + o * interval for o in range(-2, 3)]
    edges = [float("-inf")] + pts + [float("inf")]
    labels = (
        [f"<{pts[0]}"]
        + [f"{pts[i]}-{pts[i+1]}" for i in range(len(pts) - 1)]
        + [f">{pts[-1]}"]
    )
    return edges, labels


def _area_bins_fixed(
    min_val: float,
    max_val: float,
    interval: int = AREA_STEP,
):
    """
    Build bins strictly between user-provided min and max.
    """

    if min_val >= max_val:
        raise ValueError("min_val must be less than max_val")

    start = int(min_val)
    end = int(max_val)

    edges = list(range(start, end + interval, interval))

    # Ensure minimum two edges
    if len(edges) < 2:
        edges = [start, start + interval]

    labels = [
        f"{edges[i]}-{edges[i+1]}"
        for i in range(len(edges) - 1)
    ]

    return edges, labels


# ============================================================
# RANGE ASSIGNMENT
# ============================================================

def get_area_ranges_generic(
    df: pd.DataFrame,
    target_col: str = "carpet_sqft",
    interval: int = AREA_STEP,
    bin_strategy: str = "mean",   # "mean" | "fixed"
    min_val: float = None,
    max_val: float = None,
) -> pd.Series:
    """
    Vectorised area-range assignment using pd.cut.

    bin_strategy:
        "mean"   → bins around mean
        "fixed"  → user-defined min/max bins
    """

    df = df[df[target_col] > 0]

    valid = df[target_col]
    if valid.empty:
        return pd.Series([None] * len(df), index=df.index)

    if bin_strategy == "mean":

        area_mean = valid.mean()
        mean = 0 if pd.isna(area_mean) else int(round(area_mean, -2))
        edges, labels = _area_bins_mean(mean, interval)

    elif bin_strategy == "fixed":

        if min_val is None or max_val is None:
            raise ValueError(
                "min_val and max_val must be provided for fixed bin strategy"
            )

        edges, labels = _area_bins_fixed(min_val, max_val, interval)

    else:
        raise ValueError("Invalid bin_strategy. Use 'mean' or 'fixed'.")

    return pd.cut(
        df[target_col],
        bins=edges,
        labels=labels,
        right=False,
        include_lowest=True,
    )


# ============================================================
# FILTER + ASSIGN
# ============================================================

def _filter_and_assign_area_ranges(
    df: pd.DataFrame,
    filter_col: str,
    filter_val,
    interval: int = AREA_STEP,
    bin_strategy: str = "mean",
    min_val: float = None,
    max_val: float = None,
) -> pd.DataFrame:
    """
    Filter dataframe and attach 'area_range'.
    """

    if df.empty:
        return df

    df_f = (
        df[df[filter_col] == filter_val].copy()
        if filter_col in df.columns
        else df.copy()
    )

    if df_f.empty:
        return df_f

    df_f = df_f[df_f["carpet_sqft"] > 0]

    df_f["area_range"] = get_area_ranges_generic(
        df_f,
        target_col="carpet_sqft",
        interval=interval,
        bin_strategy=bin_strategy,
        min_val=min_val,
        max_val=max_val,
    )

    return df_f


# ============================================================
# GENERIC STATS ENGINE
# ============================================================

def create_area_range_stats(
    df: pd.DataFrame,
    filter_col: str,
    filter_val,
    agg: str = "count",
    value_col: str = "agreement_price",
    interval: int = AREA_STEP,
    bin_strategy: str = "mean",
    min_val: float = None,
    max_val: float = None,
) -> dict:
    """
    Generic area-range statistics engine.

    agg:
        'count' | 'sum' | 'mean'
    """

    df_f = _filter_and_assign_area_ranges(
        df,
        filter_col,
        filter_val,
        interval=interval,
        bin_strategy=bin_strategy,
        min_val=min_val,
        max_val=max_val,
    )

    if df_f.empty:
        return {}

    if agg == "count":
        result = df_f["area_range"].value_counts().to_dict()

    elif agg == "sum":
        result = (
            df_f.groupby("area_range", observed=False)[value_col]
            .sum()
            .to_dict()
        )

    elif agg == "mean":
        result = (
            df_f.groupby("area_range", observed=False)[value_col]
            .mean()
            .round(2)
            .to_dict()
        )

    else:
        raise ValueError(
            f"Unsupported agg '{agg}'. Choose: 'count', 'sum', 'mean'."
        )

    return {k: v for k, v in result.items() if pd.notna(v) and v != 0}


# ============================================================
# PROPERTY-TYPE WISE WRAPPERS
# ============================================================

def create_area_ranges(
    df,
    property_type,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "property_type",
        property_type,
        agg="count",
        **kwargs,
    )


def create_area_ranges_sales(
    df,
    property_type,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "property_type",
        property_type,
        agg="sum",
        value_col="agreement_price",
        **kwargs,
    )


def create_area_ranges_avg_sales(
    df,
    property_type,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "property_type",
        property_type,
        agg="mean",
        value_col="agreement_price",
        **kwargs,
    )


def create_area_ranges_area(
    df,
    property_type,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "property_type",
        property_type,
        agg="sum",
        value_col="carpet_sqft",
        **kwargs,
    )

# ============================================================
# BHK WISE WRAPPERS
# ============================================================

def create_area_ranges_unit_sold(
    df,
    bhk,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "bhk_br",
        bhk,
        agg="count",
        **kwargs,
    )


def create_area_ranges_unit_sales(
    df,
    bhk,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "bhk_br",
        bhk,
        agg="sum",
        value_col="agreement_price",
        **kwargs,
    )


def create_area_ranges_unit_avg_sales(
    df,
    bhk,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "bhk_br",
        bhk,
        agg="mean",
        value_col="agreement_price",
        **kwargs,
    )

def create_area_ranges_unit_carpet_area_consumed(
    df,
    bhk,
    **kwargs,
):
    return create_area_range_stats(
        df,
        "bhk_br",
        bhk,
        agg="sum",
        value_col="carpet_sqft",
        **kwargs,
    )