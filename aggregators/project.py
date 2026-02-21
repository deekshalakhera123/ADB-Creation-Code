"""
aggregators/project.py
======================
Project-wise aggregation pipeline.
Now flexible for:
- Project
- Project × Year (YoY)
- Project × Year × Quarter (QoQ)
"""

import pandas as pd

from aggregators.base import (
    build_masks,
    create_pivot,
    apply_and_merge,
    process_price_ranges,
    get_project_type,
)

from stats.rate import (
    most_prevailing_rate_on_ca,
    create_rate_ranges,
    floor_wise_wrapper,
)

from stats.area import (
    create_area_ranges,
    create_area_ranges_sales,
    create_area_ranges_avg_sales,
    create_area_ranges_unit_sold,
    create_area_ranges_unit_sales,
    create_area_ranges_unit_avg_sales,
)

from stats.price import (
    calculate_property_type_price_range,
    calculate_bhk_price_range,
)

from stats.buyer import get_project_pincode_stats


# ============================================================
# FLEXIBLE AGGREGATION ENGINE
# ============================================================

def build_project_aggregation(
    dataframe: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Generic aggregation engine.
    """

    # 🔹 Dynamic grouping
    PROJ_COLS  = group_cols
    PT_GROUPS  = group_cols + ["property_type"]
    BHK_GROUPS = group_cols + ["bhk"]

    m = build_masks(dataframe, base_col="project_name")

    print("=== Analysis Masks Summary ===")
    print(f"Base (all transactions)  : {m['base'].sum()}")
    print(f"Valid price              : {(m['base'] & m['valid_price']).sum()}")
    print(f"Valid area               : {(m['base'] & m['valid_area']).sum()}")
    print(f"Valid rate               : {(m['base'] & m['valid_rate']).sum()}")

    # ── Convenience slices ───────────────────────────────────
    base_df         = dataframe[m["base"]]
    rate_mask       = m["base"] & m["valid_rate"]
    bhk_mask        = m["bhk_base"]
    bhk_rate_mask   = bhk_mask & m["valid_rate"]

    # ========================================================
    # BASE AGGREGATION
    # ========================================================

    all_project_wise = (
        base_df
        .groupby(PROJ_COLS)
        .agg(
            igr_village             =("igr_rera_village_mapped", "first"),
            city                    =("city",                    "first"),
            total_sales             =("agreement_price",         "sum"),
            total_carpet_area       =("net_carpet_area_sqmt",    "sum"),
            total_transactions      =("document_no",             "count"),
            max_floor               =("floor_no",                "max"),
            recent_transaction_date =("transaction_date",        "max"),
            project_type            =("project_type",            get_project_type),
        )
        .reset_index()
    )

    print(f"\nAggregated rows: {len(all_project_wise)}")

    # ========================================================
    # PROPERTY TYPE & BHK PIVOTS
    # ========================================================

    pivots = [
        # Property-type
        (dataframe[m["base"]],                    PT_GROUPS,  "document_no",         "count", "_sold_igr"),
        (dataframe[m["base"]],                    PT_GROUPS,  "agreement_price",     "sum",   "_total_agreement_price"),
        (dataframe[m["base"] & m["valid_price"]], PT_GROUPS,  "agreement_price",     "mean",  "_avg_agreement_price"),
        (dataframe[m["base"]],                    PT_GROUPS,  "net_carpet_area_sqmt","sum",   "_carpet_area_consumed_in_sqmtr_igr"),

        # BHK
        (dataframe[bhk_mask],                    BHK_GROUPS, "document_no",         "count", "_sold_igr"),
        (dataframe[bhk_mask],                    BHK_GROUPS, "agreement_price",     "sum",   "_total_agreement_price"),
        (dataframe[bhk_mask & m["valid_price"]], BHK_GROUPS, "agreement_price",     "mean",  "_avg_agreement_price"),
        (dataframe[bhk_mask],                    BHK_GROUPS, "net_carpet_area_sqmt","sum",   "_carpet_area_consumed_in_sqmtr_igr"),
    ]

    for src, gcols, vcol, agg, sfx in pivots:
        all_project_wise = all_project_wise.merge(
            create_pivot(src, gcols, vcol, agg, sfx),
            on=PROJ_COLS,
            how="left",
        )

    # ========================================================
    # RATE CALCULATIONS — PROPERTY TYPE
    # ========================================================

    if rate_mask.sum() > 0:
        rate_df = dataframe[rate_mask]

        # Net Carpet Area Rate
        proj_rate_nca = (
            rate_df
            .groupby(PT_GROUPS)["rate_on_net_ca"]
            .agg(
                wt_avg_rate_nca="mean",
                p50_rate_nca=lambda x: x.quantile(0.5),
                p75_rate_nca=lambda x: x.quantile(0.75),
                p90_rate_nca=lambda x: x.quantile(0.9),
            )
            .reset_index()
            .pivot(index=PROJ_COLS, columns="property_type")
        )
        proj_rate_nca.columns = [f"{c[1]}_{c[0]}" for c in proj_rate_nca.columns]
        all_project_wise = all_project_wise.merge(
            proj_rate_nca.reset_index(),
            on=PROJ_COLS,
            how="left",
        )

        # Saleable Area Rate
        proj_rate_sa = (
            rate_df
            .groupby(PT_GROUPS)["rate_on_sa"]
            .agg(
                wt_avg_rate_sa="mean",
                p50_rate_sa=lambda x: x.quantile(0.5),
                p75_rate_sa=lambda x: x.quantile(0.75),
                p90_rate_sa=lambda x: x.quantile(0.9),
            )
            .reset_index()
            .pivot(index=PROJ_COLS, columns="property_type")
        )
        proj_rate_sa.columns = [f"{c[1]}_{c[0]}" for c in proj_rate_sa.columns]
        all_project_wise = all_project_wise.merge(
            proj_rate_sa.reset_index(),
            on=PROJ_COLS,
            how="left",
        )

        # Floor-wise 90P Rate
        all_project_wise = all_project_wise.merge(
            apply_and_merge(rate_df, PT_GROUPS, floor_wise_wrapper, "_floor_wise_90p_rate"),
            on=PROJ_COLS,
            how="left",
        )

        for func, sfx in [
            (most_prevailing_rate_on_ca, "_most_prevailing_rate_range"),
            (create_rate_ranges,         "_total_unit_sold_in_rate_range"),
        ]:
            all_project_wise = all_project_wise.merge(
                apply_and_merge(rate_df, PT_GROUPS, func, sfx),
                on=PROJ_COLS,
                how="left",
            )

    # ========================================================
    # RATE CALCULATIONS — BHK
    # ========================================================

    if bhk_rate_mask.sum() > 0:
        bhk_rate = (
            dataframe[bhk_rate_mask]
            .groupby(BHK_GROUPS)["rate_on_net_ca"]
            .agg(
                wt_avg_rate_nca="mean",
                p50_rate_nca=lambda x: x.quantile(0.5),
                p75_rate_nca=lambda x: x.quantile(0.75),
                p90_rate_nca=lambda x: x.quantile(0.9),
            )
            .reset_index()
            .round(2)
            .pivot(index=PROJ_COLS, columns="bhk")
        )
        bhk_rate.columns = [f"{c[1]} - {c[0]}" for c in bhk_rate.columns]
        all_project_wise = all_project_wise.merge(
            bhk_rate.reset_index(),
            on=PROJ_COLS,
            how="left",
        )

    # ========================================================
    # AREA RANGES — PROPERTY TYPE
    # ========================================================

    area_mask       = m["base"] & m["valid_area"]
    area_price_mask = m["base"] & m["valid_price"] & m["valid_area"]

    if area_mask.sum() > 0:
        all_project_wise = all_project_wise.merge(
            apply_and_merge(
                dataframe[area_mask],
                PT_GROUPS,
                create_area_ranges,
                "_total_unit_sold_in_area_range",
            ),
            on=PROJ_COLS,
            how="left",
        )

    if area_price_mask.sum() > 0:
        for func, sfx in [
            (create_area_ranges_sales,     "_total_agreement_price_in_area_range"),
            (create_area_ranges_avg_sales, "_avg_agreement_price_in_area_range"),
        ]:
            all_project_wise = all_project_wise.merge(
                apply_and_merge(dataframe[area_price_mask], PT_GROUPS, func, sfx),
                on=PROJ_COLS,
                how="left",
            )

    # ========================================================
    # AREA RANGES — BHK
    # ========================================================

    bhk_carpet_mask = bhk_mask & m["valid_carpet"]

    if bhk_carpet_mask.sum() > 0:
        all_project_wise = all_project_wise.merge(
            apply_and_merge(
                dataframe[bhk_carpet_mask],
                BHK_GROUPS,
                create_area_ranges_unit_sold,
                "_total_unit_sold_in_area_range",
            ),
            on=PROJ_COLS,
            how="left",
        )

        bhk_carpet_price_mask = bhk_carpet_mask & m["valid_price"]

        if bhk_carpet_price_mask.sum() > 0:
            for func, sfx in [
                (create_area_ranges_unit_sales,     "_total_agreement_price_in_area_range"),
                (create_area_ranges_unit_avg_sales, "_avg_agreement_price_in_area_range"),
            ]:
                all_project_wise = all_project_wise.merge(
                    apply_and_merge(
                        dataframe[bhk_carpet_price_mask],
                        BHK_GROUPS,
                        func,
                        sfx,
                    ),
                    on=PROJ_COLS,
                    how="left",
                )

    # ========================================================
    # AVG CARPET AREA — BHK
    # ========================================================

    if bhk_carpet_mask.sum() > 0:
        avg_carpet = (
            dataframe
            .groupby(BHK_GROUPS)["carpet_sqft"]
            .mean()
            .round(2)
            .unstack()
            .add_suffix("_avg_carpet_area_in_sqft")
            .reset_index()
        )
        all_project_wise = all_project_wise.merge(
            avg_carpet,
            on=PROJ_COLS,
            how="left",
        )

    # ========================================================
    # PRICE RANGES
    # ========================================================

    price_mask = m["base"] & m["valid_price"]

    if price_mask.sum() > 0:
        all_project_wise = all_project_wise.merge(
            process_price_ranges(
                dataframe[price_mask],
                PT_GROUPS,
                calculate_property_type_price_range,
                PROJ_COLS,
            ),
            on=PROJ_COLS,
            how="left",
        )

    if (bhk_mask & m["valid_price"]).sum() > 0:
        all_project_wise = all_project_wise.merge(
            process_price_ranges(
                dataframe[bhk_mask & m["valid_price"]],
                BHK_GROUPS,
                calculate_bhk_price_range,
                PROJ_COLS,
            ),
            on=PROJ_COLS,
            how="left",
        )

    # ========================================================
    # PINCODE STATS
    # ========================================================

    if price_mask.sum() > 0:
        all_project_wise = all_project_wise.merge(
            get_project_pincode_stats(
                dataframe[price_mask],
                group_cols=PROJ_COLS,
            ),
            on=PROJ_COLS,
            how="left",
        )

    # ---- Final cleanup ----
    all_project_wise = all_project_wise.loc[:, ~all_project_wise.columns.duplicated()]
    all_project_wise.columns = all_project_wise.columns.str.lower()

    print(f"\n=== Final Output ===")
    print(f"Shape: {all_project_wise.shape}")

    return all_project_wise


# ============================================================
# WRAPPERS
# ============================================================

def build_project_wise(df: pd.DataFrame) -> pd.DataFrame:
    return build_project_aggregation(df, ["index", "project_name"])


def build_yoy_project_wise(df: pd.DataFrame) -> pd.DataFrame:
    base = build_project_aggregation(df, ["index", "project_name", "year"])
    base = base.sort_values(["index", "project_name", "year"])
    return base

def build_qoq_project_wise(df: pd.DataFrame) -> pd.DataFrame:
    base = build_project_aggregation(df, ["index", "project_name", "quarter"])
    base = base.sort_values(["index", "project_name", "quarter"])
    return base