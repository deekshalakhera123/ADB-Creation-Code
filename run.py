"""
run.py
======
Entry point. Load → preprocess → BHK map → run pipeline → export.
"""

import sys
import os

# Ensure the project root is always on the path so that
# stats/, aggregators/ and preprocessing.py can all find config.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from preprocessing import preprocess, load_bhk_mapping, apply_bhk_mapping
from aggregators.project import (
    build_project_wise,
    build_yoy_project_wise,
    build_qoq_project_wise,
)
from aggregators.location import (
    build_location_wise,
    build_yoy_location_wise,
    build_qoq_location_wise,
)
from aggregators.city import (
    build_city_wise,
    build_yoy_city_wise,
    build_qoq_city_wise,
)

# ── Paths — update these ──────────────────────────────────────────────────────
DATA_PATH         = r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\Final DB1 with required columns\Village wise Final database db1\pune_Akurdi_igr_processed_data_db1.xlsx"
RERA_KEYWORDS_PATH= r"E:\IGR New Approach - DB1\Pune IGR excel Data 2026\Required Excels\RERA_All_Keywords_BHK_Prop_Type.xlsx"


def main():
    # 1. Load
    print("Loading data...")
    df_raw = pd.read_excel(DATA_PATH)

    # 2. Preprocess
    print("Preprocessing...")
    dataframe = preprocess(df_raw)

    # 3. BHK mapping
    print("Applying BHK mapping...")
    bhk_mapping = load_bhk_mapping(RERA_KEYWORDS_PATH)
    dataframe   = apply_bhk_mapping(dataframe, bhk_mapping)

    # 4. Run project-wise pipeline
    project = build_project_wise(dataframe)
    project_yoy     = build_yoy_project_wise(dataframe)
    project_qoq     = build_qoq_project_wise(dataframe)
    location = build_location_wise(dataframe)
    location_yoy = build_yoy_location_wise(dataframe)
    location_qoq = build_qoq_location_wise(dataframe)
    city = build_city_wise(dataframe)
    city_yoy = build_yoy_city_wise(dataframe)
    city_qoq = build_qoq_city_wise(dataframe)


    # 5. Export
    print(f"Saving to files..")
    project.to_excel("output_project_wise.xlsx", index=False)
    project_yoy.to_excel("output_yoy_project_wise.xlsx", index=False)
    project_qoq.to_excel("output_qoq_project_wise.xlsx", index=False)
    location.to_excel("output_location_wise.xlsx",index=False)
    location_yoy.to_excel("output_yoy_location_wise.xlsx",index=False)
    location_qoq.to_excel("output_qoq_location_wise.xlsx",index=False)
    city.to_excel("output_city_wise.xlsx",index=False)
    city_yoy.to_excel("output_yoy_city_wise.xlsx",index=False)
    city_qoq.to_excel("output_qoq_city_wise.xlsx",index=False)
    print("Done.")


if __name__ == "__main__":
    main()