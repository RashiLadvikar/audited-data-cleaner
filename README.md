**Live Demo → [Click to open app](https://audited-data-cleaner.streamlit.app/)**

# Audited Data Cleaning Pipeline

A production-style Python data cleaning pipeline with a full audit trail — built on the NYC 311 Service Requests dataset (50,000 rows, 44 columns). Every cleaning decision is logged with a timestamp, operation type, column name, and rows affected, producing a reproducible JSON audit report alongside the cleaned data.

---

## Why This Project Exists

Most data cleaning scripts answer *"what did you do?"* — this pipeline answers *"what did you do, to which column, when, and how many rows were affected?"*

In real engineering teams, data pipelines break silently. A column gets corrupted, nulls sneak in, a format changes upstream. This project is built around the idea that **the audit log is the deliverable** — not just the clean CSV.

---

## Demo

```bash
python run_pipeline.py
```

```
Loading raw data...
Raw data shape: (50000, 44)
Standardizing column names...
Removing duplicates...
Converting date columns...
Trimming text columns...
Standardizing text case...
Dropping high-null columns...
Filling missing text fields...
Saving cleaned data...
Saving audit log...
Generating quality report...

Pipeline completed successfully!
  Cleaned data   → nyc_311_cleaned.csv
  Audit log      → audit_log.json
  Quality report → data_quality_report.csv
```

---

## Project Structure

```
audited-data-cleaner/
│
├── src/
│   └── cleaner.py            # AuditedDataCleaner class (core logic)
│
├── download_data.py          # Pulls 50k rows from NYC Open Data API
├── explore.py                # EDA script — null report, dtypes, duplicates
├── run_pipeline.py           # Executes the full cleaning pipeline
├── app.py                    # Streamlit UI for interactive use
│
├── nyc_311_raw.csv           # Raw downloaded data (gitignored)
├── nyc_311_cleaned.csv       # Output: cleaned data
├── audit_log.json            # Output: full audit trail
└── data_quality_report.csv   # Output: per-column quality summary
```

---

## How It Works

The core class `AuditedDataCleaner` wraps a pandas DataFrame and exposes chainable cleaning methods. Every method logs its own action before returning.

```python
from src.cleaner import AuditedDataCleaner
import pandas as pd

df = pd.read_csv("nyc_311_raw.csv", low_memory=False)
cleaner = AuditedDataCleaner(df, source_name="nyc_311_service_requests")

cleaner.standardize_column_names()
cleaner.remove_duplicates()
cleaner.convert_date_columns(["created_date", "closed_date"])
cleaner.trim_text_columns()
cleaner.standardize_text_case(["borough", "complaint_type", "status"])
cleaner.drop_high_null_columns(threshold=0.70)
cleaner.fill_missing_text(fill_value="Unknown")

cleaner.save_cleaned_data("nyc_311_cleaned.csv")
cleaner.save_audit_log("audit_log.json")
```

---

## Cleaning Steps

| Step | Method | What It Does |
|------|--------|--------------|
| 1 | `standardize_column_names()` | Lowercases, strips spaces, replaces `/` and `-` with `_` |
| 2 | `remove_duplicates()` | Drops exact duplicate rows, logs count removed |
| 3 | `convert_date_columns()` | Parses date strings to datetime; logs invalid values coerced to NaT |
| 4 | `trim_text_columns()` | Strips leading/trailing whitespace from all string columns; preserves NaN |
| 5 | `standardize_text_case()` | Title-cases specified columns (borough, agency, etc.) |
| 6 | `drop_high_null_columns()` | Drops columns where null ratio exceeds configurable threshold (default 70%) |
| 7 | `fill_missing_text()` | Fills remaining string nulls with a configurable fill value |

---

## Audit Log Format

Every operation writes a structured entry to `audit_log.json`:

```json
{
  "source": "nyc_311_service_requests",
  "pipeline_run_at": "2026-03-25T16:10:42.103821",
  "pipeline_finished_at": "2026-03-25T16:11:03.887445",
  "total_steps": 54,
  "operations": [
    {
      "timestamp": "2026-03-25T16:10:42.201334",
      "step": "standardize_text_case",
      "column": "borough",
      "action": "converted_to_title_case",
      "details": "Standardized text to title case; NaN preserved",
      "rows_affected": 49959
    }
  ]
}
```

Key fields per entry: `timestamp`, `step`, `column`, `action`, `details`, `rows_affected`.

---

## Data Quality Report

After cleaning, a per-column quality report is generated as a CSV:

| column | dtype | missing_count | missing_pct | unique_values |
|--------|-------|--------------|-------------|---------------|
| complaint_type | object | 0 | 0.0 | 218 |
| borough | object | 0 | 0.0 | 6 |
| created_date | datetime64 | 0 | 0.0 | 49,214 |
| closed_date | datetime64 | 3,847 | 7.69 | 44,891 |

---

## Streamlit App

An interactive UI is included for non-technical users to run the pipeline on any CSV:

```bash
pip install streamlit
streamlit run app.py
```

Features: upload any CSV → run pipeline → preview before/after → download cleaned CSV and quality report.

---

## Dataset

**NYC 311 Service Requests (2020–Present)**
- Source: [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9)
- Pulled via Socrata API — no manual download required
- 50,000 most recent rows, 44 columns covering complaint type, borough, agency, dates, location

---

## Setup

```bash
git clone https://github.com/yourusername/audited-data-cleaner.git
cd audited-data-cleaner

pip install pandas streamlit

# Download the data
python download_data.py

# Run the pipeline
python run_pipeline.py
```

---

## Key Design Decisions

**Why log every column separately?** Granular logs let you trace exactly which step introduced a problem. A single "trim_text" entry covering 30 columns is useless for debugging; 30 individual entries with row counts are not.

**Why preserve NaN during trimming?** `str.strip()` is used instead of `astype(str).str.strip()` to avoid silently converting `NaN` to the string `"nan"` — a subtle corruption that would cause nulls to survive the fill step undetected.

**Why drop columns above 70% null?** Imputing a column that is 70%+ missing introduces more noise than signal. The threshold is configurable so downstream users can adjust it for their use case.

**Why include a SeasonalNaive-style baseline?** The `fill_value="Unknown"` default is intentionally conservative. Imputing with mean/mode was considered but rejected — for categorical columns like `city` or `borough`, a wrong imputation is worse than an honest `"Unknown"`.

---

## Tech Stack

- **Python 3.13**
- **pandas** — data manipulation and cleaning
- **Streamlit** — interactive UI
- **JSON** — structured audit output
- **NYC Open Data API (Socrata)** — data source

---

## Author

Built as a portfolio project demonstrating production-style Python, OOP design, and data engineering fundamentals.
