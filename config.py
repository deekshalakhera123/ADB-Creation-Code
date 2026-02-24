"""
config.py
=========
All project-wide constants. Import from here — never hardcode elsewhere.
"""

import numpy as np
import pandas as pd

# ── Floor mapping ─────────────────────────────────────────────────────────────
FLOOR_MAP = {
    "Stilt": 0, "Ground": 0, "Habitable": 0, "Upper Ground": 1,
    "Lower Ground": 0, "Upper Floor": 1, "Ground & 1": 0, "Top Floor": 1,
    "Mezzanine Floor": 0, "Basement": 0, "Podium": 0, "Stilt & 3": 3,
    "Stilt & 2": 2, "Ground & 2": 2, "Ground & 5": 0, "Lower Floor": 0,
    "Parking": 0, "Ground & Upper Ground": 0, "Ground & Mezzanine": 0,
    "Terrace": np.nan, "Ground & 2 Upper Floors": 0, "Stilt Ground": 0,
    "Upper Level": 0, "Lower Level": 0, "Ground Floor": 0, "Ground & Upper": 0,
    "Ground & 3": 0, "Ground & 1 & 2": 0, "Higher Ground": 0,
    "Ground & Stilt & 1": 0, "Upper Stilt": 0, "Ground & Stilt": 0,
    "Upper Parking Floor": 0, "Basement & Parking & Upper Floor": 0,
    "Uppper Ground": 1, "Ground & Upper Floor": 0, "Podium Ground": 0,
    "Plaza Floor": 0, "Ground & Upper 3 Floor": 0, "Stilt & Basement": 0,
    "Stilt & Ground": 0, "Ground & One Upper": 0, "Semi Upper": 0,
    "Ground To 2": 0, "4 & 5": 4, "1 & 2": 1, "Gorund": 0,
    "Podium Ground Floor": 0, "G & 3": 0, "5 & 6": 5, "2 & 3": 2,
    "6 & 7": 6, "Stilt & 1": 0, "Ground & Upper Gound": 0,
    "Parking Floor": 0, "Stilt Floor": 0, "Stilt & 9": 9,
    "2,3 & Terrace": 2, "35 Lower & 36 Upper": 35, "P & 1": 0,
    "Groun & 1": 0, "Plasma Floor": 0, "P1": 0,
}

# ── Loading factors ───────────────────────────────────────────────────────────
RESIDENTIAL_LOADING = 1.35
COMMERCIAL_LOADING  = 1.40

# ── Property type classification ──────────────────────────────────────────────
RESIDENTIAL_TYPES = [
    "Flat", "Apartment", "Room", "Bunglow", "Studio", "Hall",
    "Duplex", "Flat/Shop", "Row_House", "Penthouse", "Triplex", "Triplex Apartment",
]
COMMERCIAL_TYPES = [
    "Shop", "Office", "Commercial Unit", "Commercial", "Commerical", "Showroom",
]

# ── Price range step ──────────────────────────────────────────────────────────
PRICE_STEP = 2_000_000  # 20 Lakhs
AREA_STEP = 200
RATE_STEP = 1000

# ── BHK exclusions ────────────────────────────────────────────────────────────
NON_BHK_VALUES = ["Shop", "Office", "Others"]