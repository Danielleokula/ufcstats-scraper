# UFCStats Scraper

## Purpose
This repository contains a standalone, reproducible pipeline for scraping publicly
available UFCStats data and producing canonical datasets for downstream use.

The sole responsibility of this project is **data extraction and preparation**.
It does **not** perform analysis, modeling, visualization, or domain interpretation.

Downstream projects (e.g., analytics, dashboards, research) are expected to
consume the published outputs of this repository.

---

## Data Source
- https://ufcstats.com  
- Publicly accessible HTML pages only

This project does not use private APIs or proprietary data.

---

## High-Level Pipeline
Each pipeline run performs the following steps:

1. **Ingest**
   - Scrape UFCStats source pages
   - Write raw CSV extracts exactly as observed

2. **Stage**
   - Enforce data types
   - Standardize formats
   - Preserve all records (no filtering or analysis)

3. **Publish**
   - Produce reusable, analysis-ready tables
   - One row per business entity
   - Stable keys and documented schemas

The pipeline is designed so that **all outputs from a single run belong together**
and should never be mixed with outputs from another run.

---

## Run-Based Output Organization
Each execution of the pipeline corresponds to a single **run date**.
All files generated during that run are grouped together.

Example structure:
data/
runs/
YYYY-MM-DD/
raw/
fighter_directory.csv
fighter_profiles.csv
event_directory.csv
event_pages.csv
stage/
stg_fighter_directory.csv
stg_fighter_profiles.csv
stg_event_directory.csv
stg_event_pages.csv
publish/
dim_fighter__ufcstats__YYYY-MM-DD.csv
dim_event__ufcstats__YYYY-MM-DD.csv
fact_bout__ufcstats__YYYY-MM-DD.csv
This structure ensures:
- No cross-run contamination
- Clear lineage from raw → stage → publish
- Easy historical comparison between runs

---

## Raw Extracts (As-Scraped)
Raw extracts are direct representations of UFCStats pages.
They preserve source strings, formatting, and missing-value tokens.

Raw extract types include:
- Fighter directory pages
- Fighter profile pages
- Event directory pages
- Event pages (fight listings)

Raw data is **never modified** after ingestion.

---

## Staged Data (Typed & Standardized)
Staged datasets:
- Enforce consistent data types (e.g., integers, dates)
- Normalize units and formats
- Add completeness flags where appropriate

Staging does **not**:
- Filter records
- Apply business logic
- Perform aggregation or analysis

---

## Published Tables (Reusable Outputs)
Published tables are the canonical outputs of this repository.

### `dim_fighter__ufcstats`
- One row per fighter
- Stable key derived from fighter profile URL

### `dim_event__ufcstats`
- One row per event
- Stable key derived from event URL

### `fact_bout__ufcstats`
- One row per bout
- Links fighters and events
- Represents bout-level outcomes as presented on UFCStats

These tables are intended for reuse by downstream projects and tools
(e.g., analytics notebooks, dashboards, databases).

---

## Data Dictionary
All columns, data types, keys, and known limitations are documented in:
