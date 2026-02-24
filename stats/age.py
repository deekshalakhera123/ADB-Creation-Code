"""
stats/age.py
============
Age-range statistics engine.

Logic:
    - Fixed range: min=25, max=55, interval=5
    - Catch-all buckets: <25 and >55 for outliers
    - Metrics: unit_sold, total_agreement_price, carpet_area_consumed
    - Supports: property_type wise and BHK wise
"""

import numpy as np
import pandas as pd


# ============================================================
# BIN BUILDER
# ============================================================

def _build_age_bins(
    min_age: int = 25,
    max_age: int = 55,
    interval: int = 5,
):
    """
    Build age bins from min to max with catch-all edges.

    Default produces:
        <25 | 25-30 | 30-35 | 35-40 | 40-45 | 45-50 | 50-55 | >55
    """
    inner = list(range(min_age, max_age + interval, interval))

    edges = [-np.inf] + inner + [np.inf]
    labels = (
        [f"<{inner[0]}"]
        + [f"{inner[i]}-{inner[i+1]}" for i in range(len(inner) - 1)]
        + [f">{inner[-1]}"]
    )
    return edges, labels


# ============================================================
# AGE EXPLODER
# ============================================================

def _explode_ages(df: pd.DataFrame) -> pd.DataFrame:
    """
    The 'age' column is a list of buyer ages per transaction.
    Explode it so each age gets its own row, inheriting all
    other columns from the parent transaction.
    Rows with empty / NaN age lists are dropped.
    """
    df = df.copy()
    df = df[df["age"].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    df = df.explode("age")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df = df[df["age"].notna()]
    return df


# ============================================================
# SUMMARY BUILDER
# ============================================================

def _summarise_age_ranges(df: pd.DataFrame) -> dict:
    """
    For a df that already has an 'age_range' column,
    return unit_sold, total_agreement_price, carpet_area_consumed per bucket.
    """
    summary = (
        df.groupby("age_range", observed=False)
        .agg(
            unit_sold                 =("agreement_price", "size"),
            total_agreement_price     =("agreement_price", "sum"),
            carpet_area_consumed_sqft =("carpet_sqft",     "sum"),
        )
        .reset_index()
    )
    idx = summary.set_index("age_range")
    return {
        "unit_sold":                    idx["unit_sold"].to_dict(),
        "total_agreement_price":        idx["total_agreement_price"].to_dict(),
        "carpet_area_consumed_in_sqft": idx["carpet_area_consumed_sqft"].to_dict(),
    }
# # for percentage

# def _summarise_age_ranges(df: pd.DataFrame) -> dict:
#     """
#     For a df that already has an 'age_range' column,
#     return unit_sold %, total_agreement_price %, carpet_area_consumed % per bucket.
#     All values are percentage share of their respective totals.
#     """
#     summary = (
#         df.groupby("age_range", observed=False)
#         .agg(
#             unit_sold                 =("agreement_price", "size"),
#             total_agreement_price     =("agreement_price", "sum"),
#             carpet_area_consumed_sqft =("carpet_sqft",     "sum"),
#         )
#         .reset_index()
#     )

#     # ── Compute totals for percentage base ────────────────────
#     total_units   = summary["unit_sold"].sum()
#     total_price   = summary["total_agreement_price"].sum()
#     total_carpet  = summary["carpet_area_consumed_sqft"].sum()

#     # ── Convert to percentage, round to 2 decimal places ─────
#     summary["unit_sold_pct"]                 = (summary["unit_sold"]                 / total_units  * 100).round(2) if total_units  > 0 else 0
#     summary["total_agreement_price_pct"]     = (summary["total_agreement_price"]     / total_price  * 100).round(2) if total_price  > 0 else 0
#     summary["carpet_area_consumed_sqft_pct"] = (summary["carpet_area_consumed_sqft"] / total_carpet * 100).round(2) if total_carpet > 0 else 0

#     idx = summary.set_index("age_range")

#     return {
#         "unit_sold_pct":                    idx["unit_sold_pct"].to_dict(),
#         "total_agreement_price_pct":        idx["total_agreement_price_pct"].to_dict(),
#         "carpet_area_consumed_in_sqft_pct": idx["carpet_area_consumed_sqft_pct"].to_dict(),
#     }

# ============================================================
# GENERIC ENGINE
# ============================================================

def create_age_range_stats(
    df: pd.DataFrame,
    filter_col: str,
    filter_val,
    min_age: int = 25,
    max_age: int = 55,
    interval: int = 5,
) -> dict:
    """
    Generic age-range statistics engine.

    Parameters
    ----------
    filter_col : 'property_type' or 'bhk'
    filter_val : e.g. 'Flat' or '2 Bhk'
    min_age    : lower bound of defined range (default 25)
    max_age    : upper bound of defined range (default 55)
    interval   : bucket width in years (default 5)

    Returns
    -------
    {
        'unit_sold':                    {'<25': 10, '25-30': 34, ...},
        'total_agreement_price':        {'<25': 5000000, ...},
        'carpet_area_consumed_in_sqft': {'<25': 1200.5, ...},
    }
    """
    if df.empty:
        return {}

    # Step 1 — filter to segment
    if filter_col in df.columns:
        df = df[df[filter_col] == filter_val].copy()
    if df.empty:
        return {}

    # Step 2 — explode age lists into individual rows
    df = _explode_ages(df)
    if df.empty:
        return {}

    # Step 3 — build bins and assign
    edges, labels = _build_age_bins(min_age, max_age, interval)

    df["age_range"] = pd.cut(
        df["age"],
        bins=edges,
        labels=labels,
        right=False,
        include_lowest=True,
    )

    # Step 4 — summarise
    result = _summarise_age_ranges(df)

    # Strip zero / NaN buckets from each sub-dict
    return {
        metric: {k: v for k, v in bucket.items() if pd.notna(v) and v != 0}
        for metric, bucket in result.items()
    }


# ============================================================
# PROPERTY-TYPE WRAPPER
# ============================================================

def create_age_range_stats_by_property_type(
    df: pd.DataFrame,
    property_type: str,
    **kwargs,
) -> dict:
    """
    Age-range stats (sold / total price / carpet area) 
    for a given property type.
    """
    return create_age_range_stats(df, "property_type", property_type, **kwargs)


# ============================================================
# BHK WRAPPER
# ============================================================

def create_age_range_stats_by_bhk(
    df: pd.DataFrame,
    bhk: str,
    **kwargs,
) -> dict:
    """
    Age-range stats (sold / total price / carpet area) 
    for a given BHK type.
    """
    return create_age_range_stats(df, "bhk", bhk, **kwargs)