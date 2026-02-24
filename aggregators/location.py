"""
aggregators/location.py
=======================
Location-wise aggregation pipeline.
Flexible for:
- Location
- Location × Year (YoY)
- Location × Year × Quarter (QoQ)
"""

import pandas as pd

from aggregators.base import (
    build_masks,
    create_pivot,
    apply_and_merge,
    process_price_ranges,
    process_rate_ranges,
    process_age_ranges,
    get_project_type,
)

from stats.rate import (
    most_prevailing_rate_on_ca,
    create_rate_ranges,
    floor_wise_wrapper,
    create_rate_range_stats_by_property_type,
    create_rate_range_stats_by_bhk,
)

from stats.area import (
    create_area_ranges,
    create_area_ranges_sales,
    create_area_ranges_avg_sales,
    create_area_ranges_area,
    create_area_ranges_unit_sold,
    create_area_ranges_unit_sales,
    create_area_ranges_unit_avg_sales,
    create_area_ranges_unit_carpet_area_consumed,
)

from stats.price import (
    calculate_property_type_price_range,
    calculate_bhk_price_range,
)

from stats.buyer import get_project_pincode_stats

from stats.age import (
    create_age_range_stats_by_property_type,
    create_age_range_stats_by_bhk,
)


# ============================================================
# FLEXIBLE AGGREGATION ENGINE
# ============================================================

def build_location_aggregation(
    dataframe: pd.DataFrame,
    group_cols: list,
    base_col: str,
    prevailing_percentile: int = 80,
    prevailing_band: float = 0.10,
    rate_min: float = 2000,
    rate_max: float = 40000,
    price_min: float = 5000000,
    price_max: float = 20000000,
    price_step: int = 200000,
    area_min: float = 200,
    area_max: float = 6200,
    area_interval: int = 200,
) -> pd.DataFrame:
    """
    Location-wise aggregation engine.
    All range-based stats use fixed bin strategy with user-supplied bounds.
    """

    PT_GROUPS  = group_cols + ["property_type"]
    BHK_GROUPS = group_cols + ["bhk"]

    m = build_masks(dataframe, base_col="igr_rera_village_mapped")

    print("=== Analysis Masks Summary ===")
    print(f"Base (all transactions)  : {m['base'].sum()}")
    print(f"Valid price              : {(m['base'] & m['valid_price']).sum()}")
    print(f"Valid area               : {(m['base'] & m['valid_area']).sum()}")
    print(f"Valid rate               : {(m['base'] & m['valid_rate']).sum()}")

    base_df       = dataframe[m["base"]]
    rate_mask     = m["base"] & m["valid_rate"]
    bhk_mask      = m["bhk_base"]
    bhk_rate_mask = bhk_mask & m["valid_rate"]

    # ========================================================
    # BASE AGGREGATION
    # ========================================================

    location_wise_summary = (
        base_df
        .groupby(group_cols)
        .agg(
            city                           =("city",               "first"),
            total_sales                    =("agreement_price",    "sum"),
            total_carpet_area_consumed_igr =("net_carpet_area_sqmt","sum"),
            total_transactions             =("document_no",        "count"),
            max_floor                      =("floor_no",           "max"),
            recent_transaction_date        =("transaction_date",   "max"),
            project_type                   =("project_type",       get_project_type),
        )
        .reset_index()
    )

    print(f"\nAggregated rows: {len(location_wise_summary)}")

    # ========================================================
    # PROPERTY TYPE & BHK PIVOTS
    # ========================================================

    pivots = [
        # Property-type
        (dataframe[m["base"]],                    PT_GROUPS,  "document_no",          "count", "_sold_igr"),
        (dataframe[m["base"]],                    PT_GROUPS,  "agreement_price",      "sum",   "_total_agreement_price"),
        (dataframe[m["base"] & m["valid_price"]], PT_GROUPS,  "agreement_price",      "mean",  "_avg_agreement_price"),
        (dataframe[m["base"]],                    PT_GROUPS,  "net_carpet_area_sqmt", "sum",   "_carpet_area_consumed_in_sqmtr_igr"),
        # BHK
        (dataframe[bhk_mask],                     BHK_GROUPS, "document_no",          "count", "_sold_igr"),
        (dataframe[bhk_mask],                     BHK_GROUPS, "agreement_price",      "sum",   "_total_agreement_price"),
        (dataframe[bhk_mask & m["valid_price"]],  BHK_GROUPS, "agreement_price",      "mean",  "_avg_agreement_price"),
        (dataframe[bhk_mask],                     BHK_GROUPS, "net_carpet_area_sqmt", "sum",   "_carpet_area_consumed_in_sqmtr_igr"),
    ]

    for src, gcols, vcol, agg, sfx in pivots:
        location_wise_summary = location_wise_summary.merge(
            create_pivot(src, gcols, vcol, agg, sfx),
            on=group_cols,
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
            .pivot(index=group_cols, columns="property_type")
        )
        proj_rate_nca.columns = [f"{c[1]}_{c[0]}" for c in proj_rate_nca.columns]
        location_wise_summary = location_wise_summary.merge(
            proj_rate_nca.reset_index(), on=group_cols, how="left",
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
            .pivot(index=group_cols, columns="property_type")
        )
        proj_rate_sa.columns = [f"{c[1]}_{c[0]}" for c in proj_rate_sa.columns]
        location_wise_summary = location_wise_summary.merge(
            proj_rate_sa.reset_index(), on=group_cols, how="left",
        )

        # Floor-wise 90P Rate
        location_wise_summary = location_wise_summary.merge(
            apply_and_merge(rate_df, PT_GROUPS, floor_wise_wrapper, "_floor_wise_90p_rate"),
            on=group_cols, how="left",
        )

        # Prevailing rate
        location_wise_summary = location_wise_summary.merge(
            apply_and_merge(
                rate_df,
                PT_GROUPS,
                lambda g, seg: most_prevailing_rate_on_ca(
                    g, seg,
                    percentile=prevailing_percentile,
                    band_pct=prevailing_band,
                ),
                "_most_prevailing_rate_range",
            ),
            on=group_cols, how="left",
        )

        # Unit sold in rate range (count only)
        location_wise_summary = location_wise_summary.merge(
            apply_and_merge(
                rate_df,
                PT_GROUPS,
                lambda g, seg: create_rate_ranges(
                    g, seg,
                    bin_strategy="fixed",
                    interval=1000,
                    min_val=rate_min,
                    max_val=rate_max,
                ),
                "_total_unit_sold_in_rate_range",
            ),
            on=group_cols, how="left",
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
            .pivot(index=group_cols, columns="bhk")
        )
        bhk_rate.columns = [f"{c[1]} - {c[0]}" for c in bhk_rate.columns]
        location_wise_summary = location_wise_summary.merge(
            bhk_rate.reset_index(), on=group_cols, how="left",
        )

    # ========================================================
    # AREA RANGES — PROPERTY TYPE
    # ========================================================

    area_mask       = m["base"] & m["valid_area"]
    area_price_mask = m["base"] & m["valid_price"] & m["valid_area"]

    if area_mask.sum() > 0:
        # Unit sold in area range
        location_wise_summary = location_wise_summary.merge(
            apply_and_merge(
                dataframe[area_mask],
                PT_GROUPS,
                lambda g, seg: create_area_ranges(
                    g, seg,
                    bin_strategy="fixed",
                    interval=area_interval,
                    min_val=area_min,
                    max_val=area_max,
                ),
                "_total_unit_sold_in_area_range",
            ),
            on=group_cols, how="left",
        )

        # Carpet area consumed in area range
        location_wise_summary = location_wise_summary.merge(
            apply_and_merge(
                dataframe[area_mask],
                PT_GROUPS,
                lambda g, seg: create_area_ranges_area(
                    g, seg,
                    bin_strategy="fixed",
                    interval=area_interval,
                    min_val=area_min,
                    max_val=area_max,
                ),
                "_total_carpet_area_consumed_in_area_range_sqft",
            ),
            on=group_cols, how="left",
        )

    if area_price_mask.sum() > 0:
        for func, sfx in [
            (create_area_ranges_sales,     "_total_agreement_price_in_area_range"),
            (create_area_ranges_avg_sales, "_avg_agreement_price_in_area_range"),
        ]:
            location_wise_summary = location_wise_summary.merge(
                apply_and_merge(
                    dataframe[area_price_mask],
                    PT_GROUPS,
                    lambda g, seg, f=func: f(
                        g, seg,
                        bin_strategy="fixed",
                        interval=area_interval,
                        min_val=area_min,
                        max_val=area_max,
                    ),
                    sfx,
                ),
                on=group_cols, how="left",
            )

    # ========================================================
    # AREA RANGES — BHK
    # ========================================================

    bhk_carpet_mask = bhk_mask & m["valid_carpet"]

    if bhk_carpet_mask.sum() > 0:
        # Unit sold in area range
        location_wise_summary = location_wise_summary.merge(
            apply_and_merge(
                dataframe[bhk_carpet_mask],
                BHK_GROUPS,
                lambda g, seg: create_area_ranges_unit_sold(
                    g, seg,
                    bin_strategy="fixed",
                    interval=area_interval,
                    min_val=area_min,
                    max_val=area_max,
                ),
                "_total_unit_sold_in_area_range",
            ),
            on=group_cols, how="left",
        )

        # Carpet area consumed in area range
        location_wise_summary = location_wise_summary.merge(
            apply_and_merge(
                dataframe[bhk_carpet_mask],
                BHK_GROUPS,
                lambda g, seg: create_area_ranges_unit_carpet_area_consumed(
                    g, seg,
                    bin_strategy="fixed",
                    interval=area_interval,
                    min_val=area_min,
                    max_val=area_max,
                ),
                "_total_carpet_area_consumed_in_area_range_sqft",
            ),
            on=group_cols, how="left",
        )

        bhk_carpet_price_mask = bhk_carpet_mask & m["valid_price"]

        if bhk_carpet_price_mask.sum() > 0:
            for func, sfx in [
                (create_area_ranges_unit_sales,     "_total_agreement_price_in_area_range"),
                (create_area_ranges_unit_avg_sales, "_avg_agreement_price_in_area_range"),
            ]:
                location_wise_summary = location_wise_summary.merge(
                    apply_and_merge(
                        dataframe[bhk_carpet_price_mask],
                        BHK_GROUPS,
                        lambda g, seg, f=func: f(
                            g, seg,
                            bin_strategy="fixed",
                            interval=area_interval,
                            min_val=area_min,
                            max_val=area_max,
                        ),
                        sfx,
                    ),
                    on=group_cols, how="left",
                )

    # ========================================================
    # AVG CARPET AREA — BHK
    # ========================================================

    if bhk_carpet_mask.sum() > 0:
        avg_carpet = (
            dataframe[bhk_carpet_mask]
            .groupby(BHK_GROUPS)["carpet_sqft"]
            .mean()
            .round(2)
            .unstack()
            .add_suffix("_avg_carpet_area_in_sqft")
            .reset_index()
        )
        location_wise_summary = location_wise_summary.merge(
            avg_carpet, on=group_cols, how="left",
        )

    # ========================================================
    # PRICE RANGES
    # ========================================================

    price_mask = m["base"] & m["valid_price"]

    if price_mask.sum() > 0:
        location_wise_summary = location_wise_summary.merge(
            process_price_ranges(
                dataframe[price_mask],
                PT_GROUPS,
                lambda g, seg: calculate_property_type_price_range(
                    g, seg,
                    bound_strategy="fixed",
                    min_val=price_min,
                    max_val=price_max,
                    step=price_step,
                ),
                group_cols,
            ),
            on=group_cols, how="left",
        )

    if (bhk_mask & m["valid_price"]).sum() > 0:
        location_wise_summary = location_wise_summary.merge(
            process_price_ranges(
                dataframe[bhk_mask & m["valid_price"]],
                BHK_GROUPS,
                lambda g, seg: calculate_bhk_price_range(
                    g, seg,
                    bound_strategy="fixed",
                    min_val=price_min,
                    max_val=price_max,
                    step=price_step,
                ),
                group_cols,
            ),
            on=group_cols, how="left",
        )

    # ========================================================
    # RATE RANGES — PROPERTY TYPE (sold / total price / carpet)
    # ========================================================

    if rate_mask.sum() > 0:
        location_wise_summary = location_wise_summary.merge(
            process_rate_ranges(
                dataframe[rate_mask],
                PT_GROUPS,
                lambda g, seg: create_rate_range_stats_by_property_type(
                    g, seg,
                    bin_strategy="fixed",
                    interval=1000,
                    min_val=rate_min,
                    max_val=rate_max,
                ),
                group_cols,
            ),
            on=group_cols, how="left",
        )

    # ========================================================
    # RATE RANGES — BHK (sold / total price / carpet)
    # ========================================================

    if bhk_rate_mask.sum() > 0:
        location_wise_summary = location_wise_summary.merge(
            process_rate_ranges(
                dataframe[bhk_rate_mask],
                BHK_GROUPS,
                lambda g, seg: create_rate_range_stats_by_bhk(
                    g, seg,
                    bin_strategy="fixed",
                    interval=1000,
                    min_val=rate_min,
                    max_val=rate_max,
                ),
                group_cols,
            ),
            on=group_cols, how="left",
        )

    # ========================================================
    # PINCODE STATS
    # ========================================================

    if price_mask.sum() > 0:
        location_wise_summary = location_wise_summary.merge(
            get_project_pincode_stats(
                dataframe[price_mask],
                group_cols=group_cols,
            ),
            on=group_cols, how="left",
        )

    # ========================================================
    # AGE RANGES — PROPERTY TYPE
    # ========================================================

    age_mask = m["base"]

    if age_mask.sum() > 0:
        location_wise_summary = location_wise_summary.merge(
            process_age_ranges(
                dataframe[age_mask],
                PT_GROUPS,
                lambda g, seg: create_age_range_stats_by_property_type(
                    g, seg,
                    min_age=25,
                    max_age=55,
                    interval=5,
                ),
                group_cols,
            ),
            on=group_cols, how="left",
        )

    # ========================================================
    # AGE RANGES — BHK
    # ========================================================

    if bhk_mask.sum() > 0:
        location_wise_summary = location_wise_summary.merge(
            process_age_ranges(
                dataframe[bhk_mask],
                BHK_GROUPS,
                lambda g, seg: create_age_range_stats_by_bhk(
                    g, seg,
                    min_age=25,
                    max_age=55,
                    interval=5,
                ),
                group_cols,
            ),
            on=group_cols, how="left",
        )

    # ---- Final cleanup ----
    location_wise_summary = location_wise_summary.loc[
        :, ~location_wise_summary.columns.duplicated()
    ]
    location_wise_summary.columns = location_wise_summary.columns.str.lower()

    print(f"\n=== Final Output ===")
    print(f"Shape: {location_wise_summary.shape}")

    return location_wise_summary


# ============================================================
# WRAPPERS
# ============================================================

def build_location_wise(df: pd.DataFrame) -> pd.DataFrame:
    return build_location_aggregation(
        df,
        ["igr_rera_village_mapped"],
        "igr_rera_village_mapped",
    )


def build_yoy_location_wise(df: pd.DataFrame) -> pd.DataFrame:
    base = build_location_aggregation(
        df,
        ["igr_rera_village_mapped", "year"],
        "igr_rera_village_mapped",
    )
    return base.sort_values(["igr_rera_village_mapped", "year"])


def build_qoq_location_wise(df: pd.DataFrame) -> pd.DataFrame:
    base = build_location_aggregation(
        df,
        ["igr_rera_village_mapped", "quarter"],
        "igr_rera_village_mapped",
    )
    return base.sort_values(["igr_rera_village_mapped", "quarter"])