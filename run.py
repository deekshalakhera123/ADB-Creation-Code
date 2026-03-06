"""
run.py
======
Entry point. Load → preprocess → BHK map → run pipeline → export.

Folder structure expected:
    Mumbai/
        Mumbai_Bandra_igr_processed_data_db1.xlsx
        Mumbai_Andheri_igr_processed_data_db1.xlsx
        Mumbai_Dadar_igr_processed_data_db1.xlsx
    Pune/
        Pune_Akurdi_igr_processed_data_db1.xlsx
        Pune_Kothrud_igr_processed_data_db1.xlsx
    ...

Rules:
    - Each folder contains multiple Excel files
    - Every file must be prefixed with the city name  e.g. Mumbai_*.xlsx
    - Each city is processed separately through pipelines (avoids column explosion)
    - Results are concatenated as rows into one output file per pipeline
    - BR columns are reordered numerically (<1br, 1br, 1.5br ... >3br, 4br)
    - All other columns stay in original order
"""

import sys
import os
import re
import time
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from preprocessing import preprocess, load_bhk_mapping, apply_bhk_mapping, load_prop_mapping, apply_prop_mapping
from aggregators.project  import build_project_wise, build_yoy_project_wise, build_qoq_project_wise
from aggregators.location import build_location_wise, build_yoy_location_wise, build_qoq_location_wise
from aggregators.city     import build_city_wise, build_yoy_city_wise, build_qoq_city_wise
from config import get_city_ranges

# ── Folder paths — one folder per city ───────────────────────────────────────
CITY_FOLDER_PATHS = {
    "Mumbai" : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Data Excels\Mumbai",
    "Pune"   : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Data Excels\Pune",
    "Thane"  : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Data Excels\Thane",
    "Dubai"  : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Data Excels\Dubai",
}

RERA_KEYWORDS_PATH = r"E:\IGR New Approach - DB1\Required Excels\RERA_All_Keywords_BHK_Prop_Type.xlsx"
PROP_TYPE_PATH     = r"E:\IGR New Approach - DB1\Required Excels\Property_type_keywords.xlsx"
OUTPUT_DIR         = r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\ADB1 Sheets"

# Columns that must exist in every city file
EXPECTED_COLUMNS = [
    "floor_no", "purchaser_name", "net_carpet_area_sqmt",
    "agreement_price", "property_category", "property_type",
    "property_type_raw", "project_type", "buyer_pincode",
    "transaction_date", "document_no",
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def select_cities(available_cities: list) -> list:
    """Interactively ask the user which cities to process."""
    print("\n" + "="*50)
    print("  CITY SELECTION")
    print("="*50)
    print("Available cities:")
    for i, city in enumerate(available_cities, 1):
        print(f"  {i}. {city}")
    print(f"  {len(available_cities)+1}. ALL cities")
    print("="*50)
    print("Enter city numbers separated by commas (e.g. 1,3) or press Enter for ALL:")

    while True:
        raw = input("Your choice: ").strip()

        if raw == "":
            print(f"  → Loading ALL cities: {available_cities}")
            return available_cities

        try:
            choices = [int(x.strip()) for x in raw.split(",")]
        except ValueError:
            print("  ✗ Invalid input. Enter numbers only, e.g. 1,3")
            continue

        all_option = len(available_cities) + 1

        if any(c < 1 or c > all_option for c in choices):
            print(f"  ✗ Numbers must be between 1 and {all_option}")
            continue

        if all_option in choices:
            print(f"  → Loading ALL cities: {available_cities}")
            return available_cities

        selected = [available_cities[c - 1] for c in choices]
        print(f"  → Loading: {selected}")
        return selected


def load_city_files(city: str, folder: str) -> pd.DataFrame:
    """
    Load ALL Excel files inside folder whose filename starts with city name.
    Tags each row with city name and source filename.

    Example:
        Mumbai/
            Mumbai_Bandra_igr_processed_data_db1.xlsx   ← loaded
            Mumbai_Andheri_igr_processed_data_db1.xlsx  ← loaded
            notes.xlsx                                   ← skipped (no city prefix)
    """
    pattern = os.path.join(folder, f"{city}*.xlsx")
    files   = glob.glob(pattern)

    if not files:
        pattern = os.path.join(folder, f"{city.lower()}*.xlsx")
        files   = glob.glob(pattern)

    if not files:
        print(f"  ✗ No files found for {city} in: {folder}")
        return pd.DataFrame()

    print(f"  Found {len(files)} file(s) for {city}:")

    frames = []
    for filepath in sorted(files)[:1]:          # ← loads ALL files (removed [:2] limit)
        filename = os.path.basename(filepath)
        try:
            df                = pd.read_excel(filepath)
            df["city"]        = city
            df["source_file"] = filename
            frames.append(df)
            print(f"    ✓ {filename}  ({len(df):,} rows)")
        except Exception as e:
            print(f"    ✗ Failed to read {filename}: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    missing_cols = [
        c for c in EXPECTED_COLUMNS
        if c not in combined.columns.str.lower().tolist()
    ]
    if missing_cols:
        print(f"    ⚠ Missing columns in {city}: {missing_cols}")

    print(f"    → {city} total: {len(combined):,} rows from {len(frames)} file(s)")
    return combined


def save_result(df: pd.DataFrame, out_path: str):
    """Save to Excel, fall back to CSV if column count exceeds Excel limit."""
    if df.shape[1] > 16384:
        print(f"  ⚠ Too many columns ({df.shape[1]}) — saving as CSV instead")
        out_path = out_path.replace(".xlsx", ".csv")
        df.to_csv(out_path, index=False)
    else:
        df.to_excel(out_path, index=False)
    print(f"  Saved: {os.path.basename(out_path)}  ({len(df):,} rows × {df.shape[1]} cols)")


# ─────────────────────────────────────────────────────────────────────────────
# BR COLUMN REORDER
# ─────────────────────────────────────────────────────────────────────────────
# Keeps all non-br columns in their original order.
# Collects all br columns, groups by prefix sorted numerically,
# metrics in consistent order within each group, appended at the end.
#
# Before: ..., 2br_sold, 1br_sold, 3br_sold, 2br_avg_price, 1br_avg_price ...
# After:  ...(non-br original order)...,
#         <1br_sold, <1br_avg_price, ...,
#          1br_sold,  1br_avg_price, ...,
#         1.5br_sold, ...,
#          2br_sold,  2br_avg_price, ...,
#         >3br_sold, ...,
#          4br_sold,  ...

_BR_METRIC_ORDER = [
    "_sold_igr",
    "_total_agreement_price",
    "_avg_agreement_price",
    "_ca_consumed_sqft_igr",
    "_wt_avg_rate_nca",
    "_p50_rate_nca",
    "_p75_rate_nca",
    "_p90_rate_nca",
    "_wt_avg_rate_sa",
    "_p50_rate_sa",
    "_p75_rate_sa",
    "_p90_rate_sa",
    "_floor_wise_90p_rate",
    "_most_prevailing_rate_range",
    "_total_unit_sold_in_rate_range",
    "_total_unit_sold_in_area_range",
    "_total_agreement_price_in_area_range",
    "_avg_agreement_price_in_area_range",
    "_total_ca_consumed_in_area_range_sqft",
    "_avg_carpet_area_in_sqft",
    "_agreement_price_range_unit_sold",
    "_agreement_price_range_total_sales",
    "_agreement_price_range_ca_consumed_sqft",
    "_rate_range_unit_sold",
    "_rate_range_total_sales",
    "_rate_range_ca_consumed_sqft",
    "_age_range_unit_sold",
    "_age_range_total_agreement_price",
    "_age_range_ca_consumed_sqft",
]

# Sorted longest-first so most specific suffix is matched first
_BR_SUFFIXES_SORTED = sorted(_BR_METRIC_ORDER, key=len, reverse=True)


def _is_br_col(col: str) -> bool:
    """True if column belongs to a br prefix e.g. 1br_*, 2.5br_*, <1br_*, >3br_*"""
    return bool(re.match(r'^[<>]?\d+(\.\d+)?br_', col))


def _get_br_prefix(col: str):
    """'2br_sold_igr' → '2br',  '<1br_avg_agreement_price' → '<1br'"""
    m = re.match(r'^([<>]?\d+(?:\.\d+)?br)_', col)
    return m.group(1) if m else None


def _br_prefix_num(prefix: str) -> float:
    """Numeric sort key: <1br=0.5, 1br=1.0, 1.5br=1.5, >3br=3.5, 4br=4.0"""
    digits = re.sub(r"[^0-9.]", "", prefix) or "0"
    n = float(digits)
    if prefix.startswith("<"):
        return n - 0.5
    if prefix.startswith(">"):
        return n + 0.5
    return n


def _br_metric_key(col: str) -> int:
    """Sort key for metric within a br prefix."""
    for sfx in _BR_SUFFIXES_SORTED:
        if col.endswith(sfx):
            try:
                return _BR_METRIC_ORDER.index(sfx)
            except ValueError:
                return 999
    return 999


def reorder_br_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep non-br columns in original order.
    Sort br columns numerically by prefix, metrics in consistent order within each group.
    """
    all_cols    = df.columns.tolist()
    non_br_cols = [c for c in all_cols if not _is_br_col(c)]
    br_cols     = [c for c in all_cols if _is_br_col(c)]

    if not br_cols:
        return df   # nothing to reorder

    # Collect unique br prefixes then sort numerically
    seen, br_prefixes = set(), []
    for c in br_cols:
        p = _get_br_prefix(c)
        if p and p not in seen:
            br_prefixes.append(p)
            seen.add(p)

    sorted_prefixes = sorted(br_prefixes, key=_br_prefix_num)

    # Build ordered br section: each prefix → metrics in consistent order
    ordered_br = []
    for prefix in sorted_prefixes:
        prefix_cols = sorted(
            [c for c in br_cols if _get_br_prefix(c) == prefix],
            key=_br_metric_key,
        )
        ordered_br.extend(prefix_cols)

    final = non_br_cols + ordered_br

    # Safety — no column lost or duplicated
    if set(final) != set(all_cols):
        lost = set(all_cols) - set(final)
        print(f"  ⚠ reorder_br_columns: {len(lost)} unmatched cols appended at end")
        final += list(lost)

    return df[final]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total_start = time.time()

    # ── 1. Ask which cities to process ───────────────────────────────────────
    available_cities = list(CITY_FOLDER_PATHS.keys())
    selected_cities  = select_cities(available_cities)

    # ── 2. Load all files for each selected city ──────────────────────────────
    print()
    city_dataframes = {}

    for city in selected_cities:
        folder = CITY_FOLDER_PATHS[city]
        print(f"\n  Loading {city} from: {folder}")
        df_city = load_city_files(city, folder)

        if df_city.empty:
            print(f"  ⚠ Skipping {city} — no data loaded")
        else:
            city_dataframes[city] = df_city

    if not city_dataframes:
        print("\nERROR: No data loaded at all. Check your folder paths.")
        sys.exit(1)

    # ── 3. Combine all cities for preprocessing + mapping ────────────────────
    # Preprocess and map once on the full dataset (efficient),
    # then split back by city for pipeline runs.

    df_raw = pd.concat(city_dataframes.values(), ignore_index=True)
    print(f"\n  Total rows loaded: {len(df_raw):,}")

    print("\nPreprocessing...")
    dataframe = preprocess(df_raw)

    print("\nApplying mappings...")
    try:
        bhk_mapping = load_bhk_mapping(RERA_KEYWORDS_PATH)
    except FileNotFoundError:
        print(f"ERROR: RERA keywords file not found -> {RERA_KEYWORDS_PATH}")
        sys.exit(1)

    try:
        prop_type_mapping = load_prop_mapping(PROP_TYPE_PATH)
    except FileNotFoundError:
        print(f"ERROR: Property type file not found -> {PROP_TYPE_PATH}")
        sys.exit(1)

    dataframe = apply_bhk_mapping(dataframe, bhk_mapping)
    dataframe = apply_prop_mapping(dataframe, prop_type_mapping)

    # ── 4. Define pipelines ───────────────────────────────────────────────────
    # Each entry: (label, build_fn, category, period_type, period_value)
    #   category    : "project" | "location" | "city"
    #   period_type : "Overall" | "YoY" | "QoQ"
    #   period_value: human-readable period label written into the Period column
    pipeline_defs = [
        ("Project",      build_project_wise,      "project",  "Overall", "Overall"),
        ("Project YoY",  build_yoy_project_wise,  "project",  "YoY",     "YoY"),
        ("Project QoQ",  build_qoq_project_wise,  "project",  "QoQ",     "QoQ"),
        ("Location",     build_location_wise,     "location", "Overall", "Overall"),
        ("Location YoY", build_yoy_location_wise, "location", "YoY",     "YoY"),
        ("Location QoQ", build_qoq_location_wise, "location", "QoQ",     "QoQ"),
        ("City",         build_city_wise,         "city",     "Overall", "Overall"),
        ("City YoY",     build_yoy_city_wise,     "city",     "YoY",     "YoY"),
        ("City QoQ",     build_qoq_city_wise,     "city",     "QoQ",     "QoQ"),
    ]

    # Accumulate in-memory: {category: [tagged DataFrames]}
    pipeline_results = {"project": [], "location": [], "city": []}

    # ── 5. Run each city separately through all pipelines ────────────────────
    #
    # WHY SEPARATELY:
    #   Running all cities in one build_fn() call causes pivot explosion.
    #   Each unique property_type and bhk_br value becomes a column.
    #   4 cities × 10 property types × 8 range buckets × 5 metrics = 30,000+ cols.
    #   Per-city: each city produces ~200 cols, then rows are stacked via concat.
    #   pd.concat aligns columns by name — missing ones filled with NaN (blank).
    #
    for city in selected_cities:

        if city not in city_dataframes:
            continue

        print(f"\n{'='*50}")
        print(f"  Processing: {city}")
        print(f"{'='*50}")

        city_df     = dataframe[dataframe["city"] == city].copy()
        city_ranges = get_city_ranges(city)

        print(f"  Rows: {len(city_df):,}")
        print(
            f"  Ranges → "
            f"Rate: {city_ranges['MIN_RATE']}-{city_ranges['MAX_RATE']} | "
            f"Area: {city_ranges['MIN_AREA']}-{city_ranges['MAX_AREA']} | "
            f"Price: {city_ranges['MIN_PRICE']}-{city_ranges['MAX_PRICE']}"
        )

        if city_df.empty:
            print(f"  ⚠ No rows after preprocessing — skipping {city}")
            continue

        for label, build_fn, category, period_type, period_value in pipeline_defs:
            print(f"\n  Running {label}...")
            t = time.time()
            try:
                result = build_fn(city_df)

                if result is None or result.empty:
                    print(f"  ⚠ Empty result — skipped")
                    continue

                # Tag with Type + Period before accumulating
                result.insert(0, "Period", period_value)
                result.insert(0, "Type",   period_type)

                pipeline_results[category].append(result)
                print(f"  → {len(result):,} rows × {result.shape[1]} cols ({time.time()-t:.1f}s)")

            except Exception as e:
                print(f"  ✗ FAILED [{label}]: {e}")

    # ── 6. Concat → reorder BR cols → save one merged file per category ───────
    print(f"\n{'='*50}")
    print("  Saving merged output files...")
    print(f"{'='*50}\n")

    output_filenames = {
        "project":  "project_merged.xlsx",
        "location": "location_merged.xlsx",
        "city":     "city_merged.xlsx",
    }

    for category, frames in pipeline_results.items():
        if not frames:
            print(f"  ✗ No data for {category} — skipped")
            continue

        # Stack all city × period results — gaps filled with NaN automatically
        final = pd.concat(frames, ignore_index=True)

        # Reorder only BR columns numerically — all other cols stay as-is
        final = reorder_br_columns(final)

        out_path = os.path.join(OUTPUT_DIR, output_filenames[category])
        save_result(final, out_path)

    print(f"\nAll done in {time.time()-total_start:.1f}s")


if __name__ == "__main__":
    main()