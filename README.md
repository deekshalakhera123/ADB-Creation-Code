# ADB1 — IGR Real Estate Data Analysis

Automated pipeline to process raw IGR (Inspector General of Registration) transaction data and produce a wide **project-wise summary** with property-type and BHK-level breakdowns.

---

## Folder Structure

```
ADB1 Codes/
│
├── run.py                        ← Entry point — run this file
├── config.py                     ← All constants (floor map, loading factors, etc.)
├── preprocessing.py              ← Raw data cleaning and column derivation
│
├── stats/
│   ├── rate.py                   ← Rate calculations (percentile, rate ranges, floor-wise)
│   ├── area.py                   ← Area range calculations (property-type & BHK wise)
│   ├── price.py                  ← Price range calculations + Cr/L/K formatter
│   └── buyer.py                  ← Pincode / buyer statistics
│
└── aggregators/
    ├── base.py                   ← Shared helpers (masks, pivot, apply_and_merge)
    └── project.py                ← Full project-wise pipeline (build_project_wise)
```

---

## Setup

### Requirements

```
pandas
numpy
openpyxl
```

Install with:

```bash
pip install pandas numpy openpyxl
```

### Input Files

Two Excel files are required. Update the paths in `run.py` before running:

| Variable | Description |
|---|---|
| `DATA_PATH` | Village-wise IGR processed data (DB1 format) |
| `RERA_KEYWORDS_PATH` | RERA keywords file with BHK and property type mappings |
| `OUTPUT_PATH` | Where to save the output Excel file |

---

## How to Run

```bash
cd "E:\IGR New Approach - DB1\Pune IGR excel Data 2026\ADB1 Codes"
python run.py
```

Console output:

```
Loading data...
Preprocessing...
Applying BHK mapping...
=== Analysis Masks Summary ===
Base (all transactions)  : XXXX
Valid price              : XXXX
Valid area               : XXXX
Valid rate               : XXXX

Aggregated projects: XXX

=== Final Output ===
Shape: (XXX, XXX)
Saving to output_project_wise.xlsx...
Done.
```

---

## Pipeline Steps

### 1. Preprocessing (`preprocessing.py`)

Raw data is filtered and cleaned:

- Only rows where `manual_processed == 'Yes'` are kept
- Floor numbers are normalised using `FLOOR_MAP` (e.g. "Ground" → 0, "Stilt" → 0)
- Derived columns added:

| Column | Formula |
|---|---|
| `carpet_sqft` | `net_carpet_area_sqmt × 10.764` |
| `rate_on_net_ca` | `agreement_price / carpet_sqft` |
| `saleable_sqft` | `net_carpet_area_sqmt × 1.35` (residential) or `× 1.40` (commercial) |
| `rate_on_sa` | `agreement_price / saleable_sqft` |
| `project_type` | Classified as Residential / Commercial / Other |
| `age` | Extracted from Marathi-format `purchaser_name` |
| `buyer_pincode` | Coerced to numeric |

- BHK values are standardised using the RERA keywords mapping file

---

### 2. Analysis Masks (`aggregators/base.py`)

Boolean masks define which rows are eligible for each type of calculation:

| Mask | Purpose |
|---|---|
| `base` | Rows with a valid `project_name` — all transactions |
| `valid_price` | `agreement_price > 0` — for averages and price ranges |
| `valid_area` | `net_carpet_area_sqmt > 0` — for area ranges |
| `valid_rate` | `valid_price AND valid_area` — for rate calculations |
| `valid_carpet` | `carpet_sqft > 0` — for BHK area ranges |
| `bhk_base` | `base AND NOT in [Shop, Office, Others]` — for BHK analysis |

---

### 3. Output Columns

#### Base project metrics

- `igr_village`, `city`, `project_type`
- `total_sales`, `total_carpet_area`, `total_transactions`
- `max_floor`, `recent_transaction_date`

#### Property-type wise (Flat / Shop / Office / etc.)

- Units sold, total agreement price, average agreement price
- Carpet area consumed
- Weighted avg rate, P50 / P75 / P90 rate on net carpet area
- Weighted avg rate, P50 / P75 / P90 rate on saleable area
- Most prevailing rate range (±5% band around 90th percentile)
- Rate range bucket counts (±3 × ₹1000 intervals around mean)
- Area range bucket counts / total sales / avg sales (±2 × 200 sqft intervals around mean)
- Floor-wise 90th percentile rate (5-floor buckets: 0-5, 5-10, ...)
- Agreement price range — unit sold / total sales / carpet area consumed (20L buckets)

#### BHK wise (1BHK / 2BHK / 3BHK / etc.)

- Units sold, total agreement price, average agreement price
- Carpet area consumed, average carpet area
- P50 / P75 / P90 rate on net carpet area
- Area range bucket counts / total sales / avg sales
- Agreement price range — unit sold / total sales / carpet area consumed

#### Buyer statistics (project level)

- `pincode_stats` — dict of `{pincode: [count, pct, total_price, avg_price]}`

---

## Key Design Decisions

**Empty buckets are dropped** — any range bucket (area, price, rate) with no data is excluded from the output dict. You will never see `NaN` keys in the range columns.

**Pincode stays as int** — `buyer_pincode` is cast to `int` in the output so keys appear as `411027` not `np.float64(411027.0)`.

**Rate percentiles use MMA chart methodology** — linear interpolation between the two values bracketing the 90th percentile index, not standard `numpy.percentile`.

**Bounds computed on full group** — for price ranges, the bucket boundaries (min/max) are derived from the full dataset before filtering to a segment, so all property types / BHKs share consistent bucket labels within a project.

---

## Adding Location-wise or City-wise Pipelines

The `stats/` modules are completely reusable. To add a new aggregation level:

1. Create `aggregators/location.py` (or `city.py`)
2. Change the group key constants:
```python
# project-wise
PROJ_COLS = ["index", "project_name"]

# location-wise
PROJ_COLS = ["city", "igr_rera_village_mapped"]

# city-wise
PROJ_COLS = ["city"]
```
3. Call `build_masks(dataframe, base_col="city")` with the appropriate base column
4. Add the new pipeline to `run.py`

No changes needed in `stats/` — all stat functions take a plain DataFrame and return a dict regardless of aggregation level.

---

## Config Reference (`config.py`)

| Constant | Value | Description |
|---|---|---|
| `FLOOR_MAP` | dict | Maps raw floor strings to integers |
| `RESIDENTIAL_LOADING` | 1.35 | Loading factor for Flat / Others |
| `COMMERCIAL_LOADING` | 1.40 | Loading factor for Shop / Office |
| `PRICE_STEP` | 2,000,000 | 20 Lakh step for price range buckets |
| `NON_BHK_VALUES` | `[Shop, Office, Others]` | Excluded from BHK analysis |
| `RESIDENTIAL_TYPES` | list | Property types classified as Residential |
| `COMMERCIAL_TYPES` | list | Property types classified as Commercial |
