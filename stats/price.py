"""
stats/price.py
==============
Price-range statistics:
  - format_price()
  - calculate_price_range_stats()
  - calculate_property_type_price_range()
  - calculate_bhk_price_range()
"""

import pandas as pd

from config import PRICE_STEP


def format_price(value: float) -> str:
    """Convert a raw INR amount to a Cr / L / K label."""
    if value >= 1e7:
        return f"{value / 1e7:.2f} Cr"
    elif value >= 1e5:
        return f"{value / 1e5:.2f} L"
    return f"{value / 1e3:.2f} K"


def _compute_price_bounds(df: pd.DataFrame, step: int = PRICE_STEP):
    """Round-mean ± 2 steps; lower bound minimum = 1 step."""
    avg = round(df["agreement_price"].astype(float).mean() / step) * step
    return max(step, avg - 2 * step), avg + 2 * step


def _assign_price_range(
    value: float, min_r: float, max_r: float, step: int = PRICE_STEP
) -> str:
    if value < min_r:
        return f"< {format_price(min_r)}"
    if value > max_r:
        return f"> {format_price(max_r)}"
    for start in range(int(min_r), int(max_r) + 1, step):
        if start <= value <= start + step:
            return f"{format_price(start)} - {format_price(start + step)}"
    return f"> {format_price(max_r)}"


def _summarise_price_ranges(df: pd.DataFrame) -> dict:
    summary = (
        df.groupby("agreement_price_range")
        .agg(
            unit_sold=("agreement_price", "size"),
            total_sales=("agreement_price", "sum"),
            carpet_area_consumed=("carpet_sqft", "sum"),
        )
        .reset_index()
    )
    idx = summary.set_index("agreement_price_range")
    return {
        "unit_sold":                    idx["unit_sold"].to_dict(),
        "total_sales":                  idx["total_sales"].to_dict(),
        "carpet_area_consumed_in_sqft": idx["carpet_area_consumed"].to_dict(),
    }


def calculate_price_range_stats(
    df: pd.DataFrame, filter_col: str, filter_val
) -> dict:
    """
    Generic price-range statistics engine.
    Bounds are derived from the full positive-price group so buckets
    reflect the overall distribution, not just one segment.
    """
    group = df[df["agreement_price"] > 0].copy()
    if group.empty:
        return {}
    min_r, max_r = _compute_price_bounds(group)
    if filter_col in group.columns:
        group = group[group[filter_col] == filter_val].copy()
    if group.empty:
        return {}
    group["agreement_price_range"] = group["agreement_price"].apply(
        _assign_price_range, args=(min_r, max_r)
    )
    return _summarise_price_ranges(group)


def calculate_property_type_price_range(df: pd.DataFrame, property_type: str) -> dict:
    """Price-range stats — property-type wise."""
    return calculate_price_range_stats(df, "property_type", property_type)


def calculate_bhk_price_range(df: pd.DataFrame, BHK: str) -> dict:
    """Price-range stats — BHK wise."""
    return calculate_price_range_stats(df, "bhk", BHK)