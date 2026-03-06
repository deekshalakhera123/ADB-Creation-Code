# ADB1 — Real Estate Analytics Pipeline

A data pipeline that pulls IGR (Inspector General of Registration) transaction data from PostgreSQL, preprocesses it, runs statistical aggregations across project / location / city dimensions, and exports three merged Excel reports.

---

## Project Structure

```
├── run.py                  # Entry point — orchestrates the full pipeline
├── config.py               # All constants, ranges, loading factors, DB config
├── preprocessing.py        # Raw DataFrame → clean, analysis-ready DataFrame
│
├── aggregators/
│   ├── base.py             # Shared pipeline mechanics (masks, pivots, merges)
│   ├── project.py          # Project-wise aggregation (Overall / YoY / QoQ)
│   ├── location.py         # Location-wise aggregation (Overall / YoY / QoQ)
│   └── city.py             # City-wise aggregation (Overall / YoY / QoQ)
│
├── stats/
│   ├── age.py              # Buyer age-range statistics
│   ├── area.py             # Carpet area-range statistics
│   ├── buyer.py            # Pincode / buyer origin statistics
│   ├── price.py            # Agreement price-range statistics
│   └── rate.py             # Rate-per-sqft statistics (percentiles, floor-wise)
│
├── .env                    # ← Local only, never push (DB credentials)
├── .env.example            # ← Safe to push (dummy credentials template)
├── .gitignore
└── README.md
```

---

## What It Does

1. **Connects to PostgreSQL** and fetches available cities from the `cities` table
2. **Prompts** you to select one or more cities interactively
3. **Loads** transaction data from `transaction_db1` filtered by `city_id`
4. **Preprocesses** the data — floor normalisation, area/rate calculations, age extraction, property type classification
5. **Applies mappings** — BHK standardisation and property type refinement from reference Excel files
6. **Runs 9 pipelines** per city (Project / Location / City × Overall / YoY / QoQ)
7. **Merges** results in memory and saves **3 final Excel files**:
   - `project_merged.xlsx`
   - `location_merged.xlsx`
   - `city_merged.xlsx`

Each output has a `Type` column (Overall / YoY / QoQ) and a `Period` column for the time dimension.

---

## Database Schema

Two tables are required:

```sql
-- City lookup
cities (
    city_id   INTEGER PRIMARY KEY,
    city_name TEXT
)

-- All transactions
transaction_db1 (
    city_id            INTEGER,   -- FK to cities
    project_name       TEXT,
    location           TEXT,
    floor_no           TEXT,
    net_carpet_area_sqmt FLOAT,
    agreement_price    FLOAT,
    property_category  TEXT,
    property_type      TEXT,
    property_type_raw  TEXT,
    purchaser_name     TEXT,
    buyer_pincode      INTEGER,
    transaction_date   DATE,
    document_no        TEXT,
    manual_processed   TEXT,
    -- ... other columns
)
```

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

### 2. Install dependencies

```bash
pip install pandas sqlalchemy psycopg2-binary openpyxl rapidfuzz python-dotenv
```

### 3. Configure environment

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=newdatadb
DB_USER=postgres
DB_PASSWORD=your_password
```

### 4. Update file paths in `run.py`

```python
RERA_KEYWORDS_PATH = r"path\to\RERA_All_Keywords_BHK_Prop_Type.xlsx"
PROP_TYPE_PATH     = r"path\to\Property_type_keywords.xlsx"
OUTPUT_DIR         = r"path\to\output\folder"
```

---

## Running

```bash
python run.py
```

You will be prompted to select cities:

```
==================================================
  CITY SELECTION
==================================================
Available cities:
  1. Mumbai
  2. Pune
  3. Thane
  4. Dubai
  5. ALL cities
==================================================
Enter city numbers separated by commas (e.g. 1,3) or press Enter for ALL:
```

---

## Output

Three Excel files are saved to `OUTPUT_DIR`:

| File | Contents |
|------|----------|
| `project_merged.xlsx` | Project-wise stats — Overall + YoY + QoQ stacked |
| `location_merged.xlsx` | Location-wise stats — Overall + YoY + QoQ stacked |
| `city_merged.xlsx` | City-wise stats — Overall + YoY + QoQ stacked |

Each file contains:

- `Type` — Overall / YoY / QoQ
- `Period` — e.g. Overall, 2023, Q1-2024
- Units sold, total sales, avg price per property type and BHK
- Rate percentiles (P50 / P75 / P90) on carpet area and saleable area
- Floor-wise 90th percentile rate
- Most prevailing rate range
- Price range distribution (unit sold / total sales / carpet area)
- Rate range distribution
- Area range distribution
- Age range distribution (buyer age bands)
- Top buyer pincodes

---

## Configuration (`config.py`)

| Constant | Description |
|----------|-------------|
| `FLOOR_MAP` | Maps raw floor strings to numeric floor numbers |
| `RESIDENTIAL_LOADING` / `COMMERCIAL_LOADING` | Global saleable area loading factors |
| `CITY_LOADING` | Per-city loading factor overrides |
| `CITY_RANGES` | Per-city min/max overrides for rate, area, price |
| `PRICE_STEP` | Price bucket width (default ₹20L) |
| `AREA_STEP` | Area bucket width (default 200 sqft) |
| `RATE_STEP` | Rate bucket width (default ₹1,000/sqft) |
| `AGE_INTERVAL` | Age bucket width (default 5 years) |
| `DB_CONFIG` | PostgreSQL connection (loaded from `.env`) |

---

## Cities Supported

| City | Rate Range | Area Range | Price Range |
|------|-----------|------------|-------------|
| Mumbai | ₹2K–40K | 200–5000 sqft | ₹5L–5Cr |
| Pune | ₹2K–40K | 200–4000 sqft | ₹5L–1.5Cr |
| Thane | ₹2K–40K | 200–4500 sqft | ₹5L–2Cr |
| Dubai | AED 1K–4K | 300–1000 sqft | 2L–1Cr |

Adding a new city requires only adding it to the `cities` table in PostgreSQL and optionally adding overrides in `CITY_RANGES` and `CITY_LOADING` in `config.py`.

---

## Notes

- Each city is processed separately through pipelines to avoid column explosion from pivots
- BR columns in output are reordered numerically: `<1br → 1br → 1.5br → 2br → >3br → 4br`
- If a pipeline result exceeds Excel's 16,384 column limit it is automatically saved as CSV
- Rows with `manual_processed != 'Yes'` or `property_category != 'Sale'` are excluded from aggregation
