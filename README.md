# 🚗 Parivahan ETL Pipeline & Market Analysis Dashboard

A full end-to-end data engineering project that **extracts**, **transforms**, and **loads** Maharashtra vehicle registration data from the Government of India's [Vahan Parivahan portal](https://vahan.parivahan.gov.in/vahan4dashboard/), then visualises it through an interactive **Streamlit dashboard** for Tata Motors market analysis.

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Data Sources](#-data-sources)
- [ETL Pipeline](#-etl-pipeline)
  - [Extract](#1-extract--extract_mh_4wpy)
  - [Transform](#2-transform--transformpy)
  - [Load](#3-load--loadpy)
  - [Orchestrator](#4-orchestrator--etl_pipelinepy)
- [Dashboard](#-dashboard)
  - [API Client](#api-client--api_clientpy)
  - [Visualisations](#visualisations)
- [Data Model](#-data-model)
- [Setup & Installation](#-setup--installation)
- [Running the Project](#-running-the-project)
- [Environment Variables](#-environment-variables)
- [Dependencies](#-dependencies)

---

## 🔍 Project Overview

This project tracks **4-Wheeler (4W) vehicle registrations** across all **60 RTOs (Regional Transport Offices)** in Maharashtra for the period **January 2025 – March 2026**, with a focus on comparing **Tata Motors** against competitors like Maruti Suzuki, Hyundai, Mahindra, and Toyota.

**Key capabilities:**

- 🕷️ **Web Scraping** — Automates data download from the Vahan government portal using Selenium + Playwright
- 🔄 **ETL Pipeline** — Extracts, cleans, and reshapes Excel reports into structured tabular data
- ☁️ **Cloud Storage** — Loads processed data into a **Supabase (PostgreSQL)** database
- 📊 **Interactive Dashboard** — Streamlit app with DuckDB queries and Plotly charts
- 🔁 **Retry Logic** — Robust extraction with 3-attempt retry on failure

---

## 🏗️ Architecture

```
Vahan Parivahan Portal
        │
        │  (Playwright detects dynamic element IDs)
        │  (Selenium downloads Excel files per RTO per month)
        ▼
┌─────────────────────┐
│   Data/             │   Raw .xlsx files (one per RTO per month)
│   ├── Data_25/      │   FY 2025 data  (732 files)
│   └── Data_26/      │   FY 2026 data  (183 files)
└─────────────────────┘
        │
        │  (Transform.py: wide → long, extract metadata)
        ▼
┌─────────────────────┐
│  consolidated_      │   Clean CSV with columns:
│  fuel_data.csv      │   title, rto, brand, fuel, count, month, year
└─────────────────────┘
        │
        │  (load.py: batch insert in groups of 100)
        ▼
┌─────────────────────┐
│  Supabase           │   PostgreSQL table: RTA_Maharashtra
│  (PostgreSQL)       │
└─────────────────────┘
        │
        │  (api_client.py: fetch + cache via st.cache_data)
        ▼
┌─────────────────────┐
│  Streamlit          │   Interactive Dashboard
│  Dashboard          │   DuckDB in-memory queries + Plotly charts
└─────────────────────┘
```

---

## 📁 Project Structure

```
Project on System/
│
├── ETL/
│   ├── ETL_Pipeline.py       # Orchestrator: runs Extract → Transform → Load
│   ├── Extract_MH_4W.py      # Web scraper (Playwright + Selenium)
│   ├── Transform.py          # Excel → CSV transformation logic
│   └── load.py               # CSV → Supabase loader
│
├── Dashboard/
│   ├── app.py                # Streamlit dashboard app
│   └── api_client.py         # Supabase data fetcher with caching
│
├── Data/
│   ├── Data_25/              # Raw Excel files for 2025 (732 files)
│   └── Data_26/              # Raw Excel files for 2026 (183 files)
│
├── .env                      # Environment variables (not committed)
├── .env.example              # Template for environment variables
├── .gitignore
├── .python-version           # Python version pin
├── pyproject.toml            # Project metadata & dependencies
├── requirement.txt           # Pip-compatible requirements
└── README.md
```

---

## 📦 Data Sources

Data is sourced from the **Vahan Parivahan Dashboard** — the official Government of India vehicle registration database.

- **URL:** `https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml`
- **Report Type:** Maker Wise Fuel Data (4W categories)
- **Scope:** All 60 RTOs in Maharashtra
- **Period:** January 2025 to March 2026
- **Vehicle Categories Extracted:**
  - MOTOR CAR
  - MAXI CAB
  - LUXURY CAB
  - MOTOR CAB
  - VINTAGE MOTOR VEHICLE
  - LIGHT MOTOR VEHICLE

**RTO Coverage (61 offices):**

| RTO Code | Office Name |
|----------|-------------|
| MH1  | MUMBAI (CENTRAL) |
| MH2  | MUMBAI (WEST) |
| MH3  | MUMBAI (EAST) |
| MH4  | THANE |
| MH5  | KALYAN |
| MH6  | PEN (RAIGAD) |
| MH7  | SINDHUDURG (KUDAL) |
| MH8  | DY RTO RATNAGIRI |
| MH9  | KOLHAPUR |
| MH10 | SANGLI |
| MH11 | RTO SATARA |
| MH12 | PUNE |
| MH13 | RTO SOLAPUR |
| MH14 | RTO PIMPRI CHINCHWAD |
| MH15 | NASHIK |
| MH16 | RTO AHILYANAGAR |
| MH17 | SRIRAMPUR |
| MH18 | DHULE |
| MH19 | RTO JALGAON |
| MH20 | CHHATRAPATI SAMBHAJINAGAR |
| MH21 | JALANA |
| MH22 | PARBHANI |
| MH23 | BEED |
| MH24 | RTO LATUR |
| MH25 | DHARASHIV |
| MH26 | NANDED |
| MH27 | AMRAWATI |
| MH28 | BULDHANA |
| MH29 | YAWATMAL |
| MH30 | RTO AKOLA |
| MH31 | NAGPUR (U) |
| MH32 | WARDHA |
| MH33 | GADCHIROLI |
| MH34 | RTO CHANDRAPUR |
| MH35 | GONDHIA |
| MH36 | BHANDARA |
| MH37 | WASHIM |
| MH38 | DY REGIONAL TRANSPORT OFFICE, HINGOLI |
| MH39 | NANDURBAR |
| MH40 | NAGPUR (RURAL) |
| MH41 | MALEGAON |
| MH42 | BARAMATI |
| MH43 | VASHI (NEW MUMBAI) |
| MH44 | AMBEJOGAI |
| MH45 | AKLUJ |
| MH46 | PANVEL |
| MH47 | R.T.O.BORIVALI |
| MH48 | VASAI |
| MH49 | NAGPUR (EAST) |
| MH50 | KARAD |
| MH51 | ICHALKARANJI |
| MH52 | CHALISGAON |
| MH53 | PHALTAN |
| MH54 | BHADGAON |
| MH55 | UDGIR |
| MH56 | KHAMGAON |
| MH57 | VAIJAPUR |
| MH58 | MIRA BHAYANDAR |
| MH99 | TC OFFICE |
| MH202 | Chiplun Track |
| MH203 | Mira Bhayander FitnessTrack |

---

## 🔄 ETL Pipeline

### 1. Extract — `Extract_MH_4W.py`

The extraction script automates the entire data download process using a **two-phase approach**:

#### Phase 1: Dynamic ID Detection (Playwright)
The Vahan portal renders dynamic element IDs (different on each load). Playwright launches a headless Chromium browser, reads the page DOM, and captures the actual runtime IDs for:
- State dropdown
- Main refresh button
- Vehicle category refresh button

#### Phase 2: Automated Download (Selenium)
Selenium opens Chrome and performs the full interaction workflow:

1. Selects **Maharashtra (59)** from the state dropdown
2. Opens the vehicle filter panel
3. Sets the **Y-Axis = Maker** (brand names)
4. Sets the **X-Axis = Fuel** (fuel type breakdown)
5. For each RTO:
   - Selects the RTO from the dropdown
   - Checks 6 vehicle category checkboxes
   - For each month (JAN, FEB, MAR):
     - Selects the month
     - Clicks Refresh
     - Downloads the Excel file (`reportTable.xlsx`)
     - Reads the report title from row 1
     - Renames and moves the file using the title as the filename
6. Closes the browser

```
Output filename format:
Maker Wise Fuel Data  of <RTO_NAME> - <MH_CODE> , Maharashtra (<MONTH>,<YEAR>).xlsx
```

**Default parameters:**
| Parameter | Value |
|-----------|-------|
| `rto_list` | All 61 RTOs listed above |
| `months` | `["JAN", "FEB", "MAR"]` |
| `download_path` | `C:\Users\sujal\Downloads\reportTable.xlsx` |
| `output_folder` | `Data/` inside the project |

---

### 2. Transform — `Transform.py`

The transform module has two functions:

#### `convert_fuel_report(input_file, output_file)`

Converts a **single** raw Excel report from a wide pivot-table layout into a clean long-format CSV.

**Processing Steps:**

| Step | Action |
|------|--------|
| 1 | Read title from row 0 |
| 2 | Extract RTO name with regex: `of <RTO> , Maharashtra` |
| 3 | Read data starting at row 3 (skip header rows) |
| 4 | Strip invisible `\xa0` characters from column names |
| 5 | Rename `Unnamed: 1` → `brand`; drop all other `Unnamed` columns |
| 6 | Remove blank rows (no brand name) |
| 7 | **Melt** wide table → long format (`brand`, `fuel`, `count`) |
| 8 | Convert `count` to integer, filling blanks with 0 |
| 9 | Extract `month` and `year` from title using regex: `(FEB,2026)` |
| 10 | Prepend `rto`, `title` columns; append `month`, `year` columns |
| 11 | Filter rows where `count > 0` |
| 12 | Save to CSV (or skip in batch mode) |

**Output columns:**

```
title | rto | brand | fuel | count | month | year
```

#### `batch_transform(input_folder, output_file)`

Processes **all** `.xlsx` files in a folder, concatenates them, and saves a single consolidated CSV.

- Skips individual file saves (passes `output_file=False` to `convert_fuel_report`)
- Reports errors per file without stopping the entire batch
- Prints a summary: files processed, errors, total rows, output path

---

### 3. Load — `load.py`

#### `run_load(csv_path, table_name, batch_size)`

Loads the consolidated CSV into Supabase PostgreSQL in batches.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `csv_path` | required | Path to the consolidated CSV |
| `table_name` | `RTA_Maharashtra` | Target Supabase table |
| `batch_size` | `100` | Rows per insert call |

**Process:**
1. Loads `.env` for Supabase credentials
2. Reads the CSV with pandas
3. Adds a `batch_id` column (ISO timestamp of current run)
4. Converts any datetime columns to ISO strings for JSON serialisation
5. Upserts rows in batches of 100 using the Supabase Python client

---

### 4. Orchestrator — `ETL_Pipeline.py`

Coordinates the full pipeline with a single command:

```python
run_pipeline()
```

**Execution order:**

```
Extract (with up to 3 retries on failure)
    ↓
Transform (batch_transform on the Data/ folder)
    ↓
Load (run_load on consolidated_fuel_data.csv)
```

**Retry logic:** If `run_extract()` fails (any exception), it retries up to **3 times** before aborting with a raised exception.

---

## 📊 Dashboard

The Streamlit dashboard (`Dashboard/app.py`) provides an interactive analytical interface.

### API Client — `api_client.py`

`fetch_fuel_data()` — Fetches all records from the `RTA_Maharashtra` Supabase table and applies the following transformations in-memory:

| Transformation | Detail |
|----------------|--------|
| Count → int | Numeric coercion, fills blanks with 0 |
| Month parsing | Handles both numeric and string months (e.g. `"FEB"` → `2`) |
| Date column | Creates `date` using `year + month + day=1` |
| `month_year` | Formatted as `"FEB-26"` (Mon-YY) |
| Fuel categories | Groups into `"EV"` or `"ICE"` |
| Brand consolidation | Maps variant brand names to canonical names |
| Data types | `brand`, `fuel`, `rto` stored as pandas `category` |

**EV fuel types included:**
- STRONG HYBRID EV
- PURE EV
- PLUG-IN HYBRID EV
- ELECTRIC(BOV)
- PETROL(E20)/HYBRID

**Brand consolidation mapping:**

| Raw Brand | Canonical |
|-----------|-----------|
| MAHINDRA & MAHINDRA LIMITED | MAHINDRA & MAHINDRA |
| MAHINDRA ELECTRIC AUTOMOBILE LTD | MAHINDRA & MAHINDRA |
| MARUTI SUZUKI INDIA LTD | MARUTI SUZUKI |
| MARUTI UDYOG LTD | MARUTI SUZUKI |
| TATA ADVANCED SYSTEMS LTD | TATA MOTORS |
| TATA MOTORS LTD | TATA MOTORS |
| TATA MOTORS PASSENGER VEHICLES LTD | TATA MOTORS |
| TATA PASSENGER ELECTRIC MOBILITY LTD | TATA MOTORS |
| TOYOTA HIACE GL COMMUTER | TOYOTA |
| TOYOTA KIRLOSKAR MOTOR PVT LTD | TOYOTA |

> Data is cached with `@st.cache_data(ttl=300)` — refreshes every 5 minutes.

### Visualisations

The dashboard uses **DuckDB** for in-memory SQL queries on the DataFrame and **Plotly** for rendering.

#### KPI Cards (top row, 4 columns)

| Metric | Query |
|--------|-------|
| Total 4W Vehicles | `SUM(Count)` across all records |
| Total 4W EV Vehicles | `SUM(Count) WHERE fuel = 'EV'` |
| Tata Motors Vehicles | `SUM(Count) WHERE brand = 'TATA MOTORS'` |
| Tata Motors EV Vehicles | `SUM(Count) WHERE brand = 'TATA MOTORS' AND fuel = 'EV'` |

#### Charts

| # | Chart | Description |
|---|-------|-------------|
| 1 | **Fuel Type Distribution** (Pie) | Overall split of EV vs ICE registrations |
| 2 | **Brand Distribution — Top 6** (Pie) | Top 6 OEMs + combined "Others" slice |
| 3 | **4W Registrations by Month** (Line) | Monthly total registrations, one line per year, sorted Jan–Dec |
| 4 | **Fuel Type Registrations by Month** (Line) | EV vs ICE trend over time (Jan 2025–Mar 2026) |
| 5 | **Top 5 Brands Registrations by Month** (Line) | Monthly trend for the top 5 OEMs by volume |
| 6 | **Top 10 Makers** (Horizontal Bar) | Total registrations ranked, colour-scaled by volume |
| 7 | **4W Registrations by Area and Brand** (Stacked Bar) | Top 10 RTOs with breakdown: Tata, Maruti, Hyundai, Others |

#### Sidebar Filters

- **Year filter** — multi-select from available years in data
- **Month-Year filter** — multi-select filtered to selected years
- **Raw Data viewer** — expandable sidebar expander showing filtered DataFrame

---

## 🗃️ Data Model

**Supabase table: `RTA_Maharashtra`**

| Column | Type | Description |
|--------|------|-------------|
| `title` | text | Full report title from Excel header |
| `rto` | text | RTO office name (e.g. `PUNE`) |
| `brand` | text | Vehicle manufacturer / OEM |
| `fuel` | text | Fuel type (e.g. `PETROL`, `PURE EV`) |
| `count` | integer | Number of vehicles registered |
| `month` | text | Month abbreviation (e.g. `FEB`) |
| `year` | text | 4-digit year (e.g. `2026`) |
| `batch_id` | text | ISO timestamp of the ETL run that loaded this batch |

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.12+
- Google Chrome (for Selenium)
- A Supabase account and project

### 1. Clone the Repository

```bash
git clone https://github.com/sujalghanti14/Parivahan-ETL-Pipeline.git
cd "Project on System"
```

### 2. Create a Virtual Environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

### 3. Install Dependencies

Using `uv` (recommended — lock file included):

```bash
pip install uv
uv sync
```

Or using pip directly:

```bash
pip install -r requirement.txt
```

### 4. Install Playwright Browsers

```bash
playwright install chromium
```

### 5. Configure Environment Variables

Copy the example file and fill in your Supabase credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-or-service-role-key
```

---

## 🚀 Running the Project

### Run the Full ETL Pipeline

```bash
cd ETL
python ETL_Pipeline.py
```

This runs Extract → Transform → Load end-to-end, with retry logic on extraction failures.

### Run Individual ETL Steps

```bash
# Extract only
python ETL/Extract_MH_4W.py

# Transform only (processes all .xlsx in Data/)
python ETL/Transform.py

# Load only (from consolidated CSV)
python ETL/load.py
```

### Launch the Dashboard

```bash
cd Dashboard
streamlit run app.py
```

The dashboard will open at `http://localhost:8501`.

---

## 🔐 Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | ✅ | Your Supabase project URL |
| `SUPABASE_KEY` | ✅ | Your Supabase anon or service-role API key |

> ⚠️ **Never commit your `.env` file.** It is listed in `.gitignore`.

---

## 📦 Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `pandas` | ≥ 3.0.2 | Data manipulation and CSV handling |
| `selenium` | ≥ 4.43.0 | Browser automation for downloads |
| `playwright` | ≥ 1.58.0 | Dynamic ID detection from portal DOM |
| `supabase` | ≥ 2.28.3 | PostgreSQL database client |
| `streamlit` | — | Web dashboard framework |
| `plotly` | — | Interactive charts |
| `duckdb` | — | In-memory SQL analytics on DataFrames |
| `python-dotenv` | — | `.env` file loading |
| `openpyxl` | — | Reading `.xlsx` Excel files |

---

## 📌 Notes

- The extraction script downloads **one `.xlsx` file per RTO per month**. With 61 RTOs and 3 months, a full extract downloads **183 files** per year of data.
- The `Data_25/` folder contains **732 Excel files** (FY 2025 — approximately 12 months × 61 RTOs).
- The `Data_26/` folder contains **183 Excel files** (Jan–Mar 2026).
- The Vahan portal uses dynamic PrimeFaces component IDs — the Playwright phase re-detects these IDs every run to ensure the scraper doesn't break when the site re-deploys.
- All data is scoped to **4-Wheeler (4W) passenger vehicle registrations** in **Maharashtra** only.
