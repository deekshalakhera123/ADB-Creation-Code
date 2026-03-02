"""
preprocessing.py
================
Raw DataFrame -> clean, analysis-ready DataFrame.
"""

import re

import numpy as np
import pandas as pd

from config import (
    FLOOR_MAP,
    RESIDENTIAL_LOADING,
    COMMERCIAL_LOADING,
    RESIDENTIAL_TYPES,
    COMMERCIAL_TYPES,
)


def classify_project_type(property_type_raw: str) -> str:
    if property_type_raw in RESIDENTIAL_TYPES:
        return "Residential"
    if property_type_raw in COMMERCIAL_TYPES:
        return "Commercial"
    return "Other"


def extract_age(text: str) -> list:
    """Extract buyer ages from Marathi-format purchaser_name strings."""
    try:
        entries = re.split(r"\s*\d+\)\s*", text)[1:]
        numbers = []
        for entry in entries:
            match = re.search(
                r"(?:वय[:-]?\s*(\d{2})|(\d{2}))\s*(?:;|\s)*पत्ता|(\d{2})\s+प्लॉट|(\d{2})\s*-,",
                entry,
            )
            if match:
                num = next((int(n) for n in match.groups() if n), None)
                if num:
                    numbers.append(num)
        return numbers
    except Exception:
        return []


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter, normalise and derive all required columns.
    Returns a clean copy ready for analysis.
    """
    
    df.columns = df.columns.str.lower()

    # Floor number normalisation
    df["floor_no"] = df["floor_no"].replace(FLOOR_MAP).astype(float)

    # Age extraction
    df["age"] = df["purchaser_name"].apply(extract_age)

    # Derived area & rate columns
    df["carpet_sqft"]    = df["net_carpet_area_sqmt"] * 10.764
    # --------TAG
    mask = df['property_category'] == 'Sale'
    df.loc[mask, "agreement_price"] = pd.to_numeric(df.loc[mask, "agreement_price"], errors='coerce')
    df.loc[mask, "carpet_sqft"] = pd.to_numeric(df.loc[mask, "carpet_sqft"], errors='coerce')

    # Replace 0 with NaN to avoid division by zero
    df["carpet_sqft"] = df["carpet_sqft"].replace(0, float('nan'))

    df.loc[mask, "rate_on_net_ca"] = (
        df.loc[mask, "agreement_price"] / df.loc[mask, "carpet_sqft"]
    )

    # Saleable area + rate
    df["saleable_sqft"] = np.where(
        df["property_type"].isin(["Flat", "Others"]),
        df["net_carpet_area_sqmt"] * RESIDENTIAL_LOADING,
        df["net_carpet_area_sqmt"] * COMMERCIAL_LOADING,
    )

    # Replace 0 with NaN to avoid division by zero
    df["saleable_sqft"] = df["saleable_sqft"].replace(0, float('nan'))

    # ✅ Use mask here too — agreement_price has strings in non-Sale rows
    df.loc[mask, "rate_on_sa"] = (
        df.loc[mask, "agreement_price"] / df.loc[mask, "saleable_sqft"]
    )

    # Project-type classification
    df["project_type"] = df["property_type_raw"].apply(classify_project_type)

    # Buyer pincode as numeric
    df["buyer_pincode"] = pd.to_numeric(df["buyer_pincode"], errors="coerce")

    return df


def load_bhk_mapping(rera_keywords_path: str) -> dict:
    
    """
    Load BHK keyword mapping from RERA excel.
    Returns {raw_bhk: final_bhk} dict.
    """
    rera = pd.read_excel(rera_keywords_path)
    rera["BHK"]       = rera["BHK"].str.strip().str.title()
    rera["standard_label"] = rera["standard_label"].str.strip().str.title()
    return (
        rera[["BHK", "standard_label"]]
        .drop_duplicates()
        .set_index("BHK")["standard_label"]
        .to_dict()
    )


def apply_bhk_mapping(df: pd.DataFrame, bhk_mapping: dict) -> pd.DataFrame:
    """Map raw BHK values to standardised standard_label values."""
    df = df.copy()
    df["bhk_br"] = df["bhk_br"].str.strip().str.title().map(bhk_mapping)
    return df

def round_dict_floats(val, decimals=2):
    """Recursively round all float values inside a dict."""
    if isinstance(val, dict):
        return {k: round_dict_floats(v, decimals) for k, v in val.items()}
    if isinstance(val, float):
        return round(val, decimals)
    return val