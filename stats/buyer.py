"""
stats/buyer.py
==============
Buyer / pincode statistics:
  - get_pincode_stats()
  - get_project_pincode_stats()
  - generate_top10_buyer_project()
"""

import pandas as pd


def get_pincode_stats(df: pd.DataFrame, price_col: str = "agreement_price") -> dict:
    """
    Per-pincode transaction statistics for the given DataFrame slice.

    Returns
    -------
    {
        pincode: [
            no_of_transactions,
            pct_of_total_transactions,
            total_agreement_price,
            avg_agreement_price,
        ],
        ...
    }
    """
    if df.empty:
        return {}

    total = len(df)
    stats = (
        df.groupby("buyer_pincode")[price_col]
        .agg(
            no_of_transactions="count",
            total_agreement_price="sum",
            avg_agreement_price="mean",
        )
        .round(2)
        .reset_index()
    )
    stats["pct_of_total_transactions"] = (
        (stats["no_of_transactions"] / total * 100).round(2)
    )
    return {
        int(row["buyer_pincode"]): [
            int(row["no_of_transactions"]),
            float(row["pct_of_total_transactions"]),
            float(row["total_agreement_price"]),
            float(row["avg_agreement_price"]),
        ]
        for _, row in stats.iterrows()
    }


def get_project_pincode_stats(
    df: pd.DataFrame,
    group_cols: list,
    price_col: str = "agreement_price",
) -> pd.DataFrame:
    """
    Pincode stats per project group.
    Returns one row per project with a 'pincode_stats' dict column.
    """
    if df.empty:
        return pd.DataFrame(columns=group_cols + ["pincode_stats"])
    return (
        df.groupby(group_cols)
        .apply(lambda g: get_pincode_stats(g, price_col))
        .reset_index()
        .rename(columns={0: "pincode_stats"})
    )


def generate_top10_buyer_project(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """Top-10 buyer pincodes per group, returned as a dict column."""
    grouped = (
        df.groupby(group_cols + ["buyer_pincode"])["buyer_pincode"]
        .count()
        .reset_index(name="count")
        .sort_values(group_cols + ["count"], ascending=[True] * len(group_cols) + [False])
    )
    top10 = grouped.groupby(group_cols).head(10)
    return (
        top10.groupby(group_cols)
        .apply(lambda x: {row["buyer_pincode"]: row["count"] for _, row in x.iterrows()})
        .reset_index(name="top10_project_buyer")
    )