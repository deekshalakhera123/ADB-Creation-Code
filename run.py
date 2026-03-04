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
"""

import sys
import os
import time
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from preprocessing import preprocess, load_bhk_mapping, apply_bhk_mapping, load_prop_mapping, apply_prop_mapping
from aggregators.project  import build_project_wise, build_yoy_project_wise, build_qoq_project_wise
from aggregators.location import build_location_wise, build_yoy_location_wise, build_qoq_location_wise
from aggregators.city     import build_city_wise, build_yoy_city_wise, build_qoq_city_wise


# ── Folder paths — one folder per city ───────────────────────────────────────
# All Excel files inside each folder whose name starts with the city name
# will be loaded automatically.

CITY_FOLDER_PATHS = {
    "Mumbai" : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Mumbai",
    "Pune"   : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Pune",
    "Thane"  : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Thane",
    "Dubai"  : r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Dubai",
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
    # Match files starting with city name (case-sensitive first, then fallback)
    pattern      = os.path.join(folder, f"{city}*.xlsx")
    files        = glob.glob(pattern)

    if not files:
        pattern  = os.path.join(folder, f"{city.lower()}*.xlsx")
        files    = glob.glob(pattern)

    if not files:
        print(f"  ✗ No files found for {city} in: {folder}")
        return pd.DataFrame()

    print(f"  Found {len(files)} file(s) for {city}:")

    frames = []
    for filepath in sorted(files):
        filename = os.path.basename(filepath)
        try:
            df              = pd.read_excel(filepath)
            df["city"]        = city      # ensure city column exists
            df["source_file"] = filename  # track which file each row came from
            frames.append(df)
            print(f"    ✓ {filename}  ({len(df):,} rows)")
        except Exception as e:
            print(f"    ✗ Failed to read {filename}: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Warn about any missing expected columns
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
    city_dataframes = {}   # { city_name: dataframe }

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

    df_raw    = pd.concat(city_dataframes.values(), ignore_index=True)
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
    pipeline_defs = [
        ("Project",      build_project_wise,      "output_project_wise.xlsx"),
        ("Project YoY",  build_yoy_project_wise,  "output_yoy_project_wise.xlsx"),
        ("Project QoQ",  build_qoq_project_wise,  "output_qoq_project_wise.xlsx"),
        ("Location",     build_location_wise,     "output_location_wise.xlsx"),
        ("Location YoY", build_yoy_location_wise, "output_yoy_location_wise.xlsx"),
        ("Location QoQ", build_qoq_location_wise, "output_qoq_location_wise.xlsx"),
        ("City",         build_city_wise,         "output_city_wise.xlsx"),
        ("City YoY",     build_yoy_city_wise,     "output_yoy_city_wise.xlsx"),
        ("City QoQ",     build_qoq_city_wise,     "output_qoq_city_wise.xlsx"),
    ]

    # Collect results per pipeline across all cities
    pipeline_results = {filename: [] for _, _, filename in pipeline_defs}

    # ── 5. Run each city separately through all pipelines ────────────────────
    #
    # WHY SEPARATELY and not all together:
    #   Running all cities in one build_fn() call causes pivot explosion.
    #   Each unique property_type and bhk_br value becomes a column.
    #   4 cities × 10 property types × 8 range buckets × 5 metrics = 30,000+ cols.
    #   Per-city: each city produces ~200 cols, then rows are stacked via concat.
    #   pd.concat aligns columns by name — missing ones filled with NaN (blank).
    #
    for city in selected_cities:

        if city not in city_dataframes:
            continue   # was skipped during load

        print(f"\n{'='*50}")
        print(f"  Processing: {city}")
        print(f"{'='*50}")

        # Slice only this city's rows from the preprocessed + mapped dataframe
        city_df = dataframe[dataframe["city"] == city].copy()
        print(f"  Rows: {len(city_df):,}")

        if city_df.empty:
            print(f"  ⚠ No rows after preprocessing — skipping {city}")
            continue

        for label, build_fn, filename in pipeline_defs:
            print(f"\n  Running {label}...")
            t = time.time()
            try:
                result = build_fn(city_df)

                if result is None or result.empty:
                    print(f"  ⚠ Empty result — skipped")
                    continue

                pipeline_results[filename].append(result)
                print(f"  → {len(result):,} rows × {result.shape[1]} cols ({time.time()-t:.1f}s)")

            except Exception as e:
                print(f"  ✗ FAILED [{label}]: {e}")

    # ── 6. Concat all cities per pipeline and save one file each ─────────────
    print(f"\n{'='*50}")
    print("  Saving output files...")
    print(f"{'='*50}\n")

    for _, _, filename in pipeline_defs:
        frames = pipeline_results[filename]

        if not frames:
            print(f"  ✗ No data for {filename} — skipped")
            continue

        # Stack city results as rows — columns align by name, gaps filled with NaN
        final    = pd.concat(frames, ignore_index=True)
        out_path = os.path.join(OUTPUT_DIR, filename)
        save_result(final, out_path)

    print(f"\nAll done in {time.time()-total_start:.1f}s")


if __name__ == "__main__":
    main()