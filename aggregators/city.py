"""
aggregators/city.py
=======================
city-wise aggregation pipeline.
Flexible for:
- city
- city × Year (YoY)
- city × Year × Quarter (QoQ)
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


from config import PRICE_STEP, AREA_STEP, RATE_STEP

from config import MIN_RATE, MAX_RATE, MIN_AREA, MAX_AREA, MIN_PRICE, MAX_PRICE

from preprocessing import round_dict_floats
# ============================================================
# FLEXIBLE AGGREGATION ENGINE
# ============================================================

def build_city_aggregation(
    dataframe: pd.DataFrame,
    group_cols: list,
    base_col: str,
    prevailing_percentile: int = 80,
    prevailing_band: float = 0.10,
    rate_min: float = MIN_RATE,
    rate_max: float = MAX_RATE,
    price_min: float = MIN_PRICE,
    price_max: float = MAX_PRICE,
    price_step: int = PRICE_STEP,
    area_min: float = MIN_AREA,
    area_max: float = MAX_AREA,
    area_interval: int = AREA_STEP,
) -> pd.DataFrame:
    """
    city-wise aggregation engine.
    All range-based stats use fixed bin strategy with user-supplied bounds.
    """

    PT_GROUPS  = group_cols + ["property_type"]
    BHK_GROUPS = group_cols + ["bhk_br"]


    type_summary = [
        (
            dataframe[(dataframe['property_category'] == 'Sale')],
            group_cols+["property_type_raw"],
            "document_no",
            "count",
            "_transactions_sale"
        ),
        (
            dataframe,
            group_cols+["property_category"],
            "document_no",
            "count",
            "_transactions"
        ),
    ]

    dataframe=dataframe[dataframe['property_category']=='Sale']

    dataframe['rate_on_net_ca']=dataframe['rate_on_net_ca'].astype(float)
    dataframe['agreement_price']=dataframe['agreement_price'].astype(float)

    m = build_masks(dataframe, base_col="city")

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

    city_wise_summary = (
        base_df
        .groupby(group_cols)
        .agg(
            total_sales                     =("agreement_price",    "sum"),
            total_ca_consumed_sqft_igr      =("carpet_sqft",         "sum"),
            total_transactions              =("document_no",        "count"),
            max_floor                       =("floor_no",           "max"),
            recent_transaction_date         =("transaction_date",   "max"),
            project_type                    =("project_type",       get_project_type),
        )
        .reset_index()
    )

    print(f"\nAggregated rows: {len(city_wise_summary)}")

    # ========================================================
    # PROPERTY TYPE & BHK PIVOTS
    # ========================================================

    pivots = [
        # Property-type
        (dataframe[m["base"]],                    PT_GROUPS,  "document_no",          "count", "_sold_igr"),
        (dataframe[m["base"]],                    PT_GROUPS,  "agreement_price",      "sum",   "_total_agreement_price"),
        (dataframe[m["base"] & m["valid_price"]], PT_GROUPS,  "agreement_price",      "mean",  "_avg_agreement_price"),
        (dataframe[m["base"]],                    PT_GROUPS,  "carpet_sqft",           "sum",   "_ca_consumed_sqft_igr"),
        # BHK
        (dataframe[bhk_mask],                     BHK_GROUPS, "document_no",          "count", "_sold_igr"),
        (dataframe[bhk_mask],                     BHK_GROUPS, "agreement_price",      "sum",   "_total_agreement_price"),
        (dataframe[bhk_mask & m["valid_price"]],  BHK_GROUPS, "agreement_price",      "mean",  "_avg_agreement_price"),
        (dataframe[bhk_mask],                     BHK_GROUPS, "carpet_sqft",           "sum",   "_ca_consumed_sqft_igr"),
    ]

    for src, gcols, vcol, agg, sfx in pivots:
        city_wise_summary = city_wise_summary.merge(
            create_pivot(src, gcols, vcol, agg, sfx),
            on=group_cols,
            how="left",
        )


    # sale, lease and other
    for src, gcols, vcol, agg, sfx in type_summary:
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
            proj_rate_sa.reset_index(), on=group_cols, how="left",
        )

        # Floor-wise 90P Rate
        city_wise_summary = city_wise_summary.merge(
            apply_and_merge(rate_df, PT_GROUPS, floor_wise_wrapper, "_floor_wise_90p_rate"),
            on=group_cols, how="left",
        )

        # Prevailing rate
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
            apply_and_merge(
                rate_df,
                PT_GROUPS,
                lambda g, seg: create_rate_ranges(
                    g, seg,
                    bin_strategy="fixed",
                    interval=RATE_STEP,
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
            .pivot(index=group_cols, columns="bhk_br")
        )
        bhk_rate.columns = [f"{c[1]} - {c[0]}" for c in bhk_rate.columns]
        city_wise_summary = city_wise_summary.merge(
            bhk_rate.reset_index(), on=group_cols, how="left",
        )

    # ========================================================
    # AREA RANGES — PROPERTY TYPE
    # ========================================================

    area_mask       = m["base"] & m["valid_area"]
    area_price_mask = m["base"] & m["valid_price"] & m["valid_area"]

    if area_mask.sum() > 0:
        # Unit sold in area range
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
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
                "_total_ca_consumed_in_area_range_sqft",
            ),
            on=group_cols, how="left",
        )

    if area_price_mask.sum() > 0:
        for func, sfx in [
            (create_area_ranges_sales,     "_total_agreement_price_in_area_range"),
            (create_area_ranges_avg_sales, "_avg_agreement_price_in_area_range"),
        ]:
            city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
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
                "_total_ca_consumed_in_area_range_sqft",
            ),
            on=group_cols, how="left",
        )

        bhk_carpet_price_mask = bhk_carpet_mask & m["valid_price"]

        if bhk_carpet_price_mask.sum() > 0:
            for func, sfx in [
                (create_area_ranges_unit_sales,     "_total_agreement_price_in_area_range"),
                (create_area_ranges_unit_avg_sales, "_avg_agreement_price_in_area_range"),
            ]:
                city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
            avg_carpet, on=group_cols, how="left",
        )

    # ========================================================
    # PRICE RANGES
    # ========================================================

    price_mask = m["base"] & m["valid_price"]

    if price_mask.sum() > 0:
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
            process_rate_ranges(
                dataframe[rate_mask],
                PT_GROUPS,
                lambda g, seg: create_rate_range_stats_by_property_type(
                    g, seg,
                    bin_strategy="fixed",
                    interval=RATE_STEP,
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
        city_wise_summary = city_wise_summary.merge(
            process_rate_ranges(
                dataframe[bhk_rate_mask],
                BHK_GROUPS,
                lambda g, seg: create_rate_range_stats_by_bhk(
                    g, seg,
                    bin_strategy="fixed",
                    interval=RATE_STEP,
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
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
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
        city_wise_summary = city_wise_summary.merge(
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
    city_wise_summary = city_wise_summary.loc[:, ~city_wise_summary.columns.duplicated()]
    city_wise_summary.columns = city_wise_summary.columns.str.lower()

    # Round plain float columns
    float_cols = city_wise_summary.select_dtypes(include='float').columns
    city_wise_summary[float_cols] = city_wise_summary[float_cols].round(2)

    # Round floats inside dict columns
    dict_cols = [
        col for col in city_wise_summary.columns
        if city_wise_summary[col].apply(lambda x: isinstance(x, dict)).any()
    ]
    for col in dict_cols:
        city_wise_summary[col] = city_wise_summary[col].apply(round_dict_floats)


    print(f"\n=== Final Output ===")
    print(f"Shape: {city_wise_summary.shape}")

    return city_wise_summary


# ============================================================
# WRAPPERS
# ============================================================

def build_city_wise(df: pd.DataFrame) -> pd.DataFrame:
    return build_city_aggregation(
        df,
        ["city"],
        "city",
    )


def build_yoy_city_wise(df: pd.DataFrame) -> pd.DataFrame:
    base = build_city_aggregation(
        df,
        ["city", "year"],
        "city",
    )
    return base.sort_values(["city", "year"])


def build_qoq_city_wise(df: pd.DataFrame) -> pd.DataFrame:
    base = build_city_aggregation(
        df,
        ["city", "quarter"],
        "city",
    )
    return base.sort_values(["city", "quarter"])