"""
run.py
======
Entry point. Load → preprocess → BHK map → run pipeline → export.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from preprocessing import preprocess, load_bhk_mapping, apply_bhk_mapping
from aggregators.project  import build_project_wise, build_yoy_project_wise, build_qoq_project_wise
from aggregators.location import build_location_wise, build_yoy_location_wise, build_qoq_location_wise
from aggregators.city     import build_city_wise, build_yoy_city_wise, build_qoq_city_wise

# ── Paths — update these ──────────────────────────────────────────────────────
DATA_PATH         = r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\Sample for ADB1 Code - Pune, Thane, Dubai.xlsx"
RERA_KEYWORDS_PATH= r"E:\IGR New Approach - DB1\Required Excels\RERA_All_Keywords_BHK_Prop_Type.xlsx"
OUTPUT_DIR         = r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes\ADB1 Sheets"


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total_start = time.time()

    # 1. Load
    print("Loading data...")
    try:
        df_raw = pd.read_excel(DATA_PATH)
    except FileNotFoundError:
        print(f"ERROR: Data file not found -> {DATA_PATH}")
        sys.exit(1)
    print(f"  Loaded {len(df_raw):,} rows")

    # 2. Preprocess
    print("Preprocessing...")
    dataframe = preprocess(df_raw)

    # 3. BHK mapping
    print("Applying BHK mapping...")
    try:
        bhk_mapping = load_bhk_mapping(RERA_KEYWORDS_PATH)
    except FileNotFoundError:
        print(f"ERROR: RERA keywords file not found -> {RERA_KEYWORDS_PATH}")
        sys.exit(1)
    dataframe = apply_bhk_mapping(dataframe, bhk_mapping)

    # 4. Run pipelines
    pipelines = [
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

    # 5. Run + Export
    for label, build_fn, filename in pipelines:
        print(f"\nRunning {label}...")
        t = time.time()
        result = build_fn(dataframe)
        out_path = os.path.join(OUTPUT_DIR, filename)
        result.to_excel(out_path, index=False)
        print(f"  Saved: {filename} ({len(result):,} rows, {time.time()-t:.1f}s)")

    print(f"\nAll done in {time.time()-total_start:.1f}s")


if __name__ == "__main__":
    main()