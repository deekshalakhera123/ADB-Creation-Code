"""
stats/price.py
==============
Price-range statistics engine.

Supports:
    - Mean-based bounds (project level)
    - User-defined min/max bounds (location/city level)
    - Configurable step size
"""

import pandas as pd
from config import PRICE_STEP


# ============================================================
# FORMATTER
# ============================================================

def format_price(value: float) -> str:
    """Convert a raw INR amount to a Cr / L / K label."""
    if value >= 1e7:
        return f"{value / 1e7:.2f} Cr"
    elif value >= 1e5:
        return f"{value / 1e5:.2f} L"
    return f"{value / 1e3:.2f} K"


# ============================================================
# BOUND STRATEGIES
# ============================================================

def _compute_price_bounds_mean(df: pd.DataFrame, step: int):
    """
    Compute bounds as:
        rounded mean ± 2 * step
    """
    prices = df["agreement_price"].astype(float)
    avg = round(prices.mean() / step) * step

    lower = max(step, avg - 2 * step)
    upper = avg + 2 * step

    return lower, upper


def _compute_price_bounds_fixed(min_val: float, max_val: float):
    """
    Use user-provided bounds.
    """
    if min_val is None or max_val is None:
        raise ValueError("min_val and max_val must be provided for fixed strategy")

    if min_val >= max_val:
        raise ValueError("min_val must be less than max_val")

    return float(min_val), float(max_val)


# ============================================================
# RANGE ASSIGNMENT
# ============================================================

def _assign_price_range(
    value: float,
    min_r: float,
    max_r: float,
    step: int,
) -> str:

    if value < min_r:
        return f"< {format_price(min_r)}"

    if value > max_r:
        return f"> {format_price(max_r)}"

    start = min_r
    while start <= max_r:
        end = start + step
        if start <= value <= end:
            return f"{format_price(start)} - {format_price(end)}"
        start += step

    return f"> {format_price(max_r)}"


# ============================================================
# SUMMARY BUILDER
# ============================================================

def _summarise_price_ranges(df: pd.DataFrame) -> dict:
    summary = (
        df.groupby("agreement_price_range", observed=False)
        .agg(
            unit_sold=("agreement_price", "size"),
            total_sales=("agreement_price", "sum"),
            carpet_area_consumed=("carpet_sqft", "sum"),
        )
        .reset_index()
    )

    idx = summary.set_index("agreement_price_range")

    return {
        "unit_sold": idx["unit_sold"].to_dict(),
        "total_sales": idx["total_sales"].to_dict(),
        "carpet_area_consumed_in_sqft": idx["carpet_area_consumed"].to_dict(),
    }


# ============================================================
# GENERIC ENGINE
# ============================================================

def calculate_price_range_stats(
    df: pd.DataFrame,
    filter_col: str,
    filter_val,
    step: int = PRICE_STEP,
    bound_strategy: str = "mean",   # "mean" | "fixed"
    min_val: float = None,
    max_val: float = None,
) -> dict:
    """
    Generic price-range statistics engine.

    bound_strategy:
        "mean"  → mean ± 2 * step
        "fixed" → user-defined min/max bounds
    """

    group = df[df["agreement_price"] > 0].copy()
    if group.empty:
        return {}

    # ---- Apply filter ----
    if filter_col in group.columns:
        group = group[group[filter_col] == filter_val].copy()

    if group.empty:
        return {}
    
    # ---- Determine bounds ----
    if bound_strategy == "mean":
        min_r, max_r = _compute_price_bounds_mean(group, step)

    elif bound_strategy == "fixed":
        min_r, max_r = _compute_price_bounds_fixed(min_val, max_val)

    else:
        raise ValueError("Invalid bound_strategy. Use 'mean' or 'fixed'.")

    # ---- Assign ranges ----
    group["agreement_price_range"] = group["agreement_price"].apply(
        _assign_price_range,
        args=(min_r, max_r, step),
    )

    return _summarise_price_ranges(group)


# ============================================================
# PROPERTY-TYPE WISE WRAPPERS
# ============================================================

def calculate_property_type_price_range(
    df: pd.DataFrame,
    property_type: str,
    **kwargs,
) -> dict:
    return calculate_price_range_stats(
        df,
        "property_type",
        property_type,
        **kwargs,
    )


# ============================================================
# BHK WISE WRAPPERS
# ============================================================

def calculate_bhk_price_range(
    df: pd.DataFrame,
    bhk: str,
    **kwargs,
) -> dict:
    return calculate_price_range_stats(
        df,
        "bhk",
        bhk,
        **kwargs,
    )