# UFCStats Scraper

## Purpose
# UFCStats Scraper

## Purpose
This repository provides a reproducible pipeline that mirrors publicly available UFCStats data into structured datasets.

The responsibility of this project is **data extraction and structural preparation only**.

It does NOT perform:
- analysis
- modeling
- prediction
- visualization
- domain interpretation

Downstream projects consume these outputs.

---

## Data Source
https://ufcstats.com  
Public HTML pages only  
No private APIs or proprietary feeds are used.

---

## Current Project State (IMPORTANT)

The repository currently implements the **Raw Layer**.

It creates a structured mirror of the UFCStats website so the entire site can be queried locally without repeated scraping.

Future layers (stage and publish) are planned but not yet implemented.

---

## Website → Dataset Mapping

Each dataset corresponds to a page type on UFCStats.

| Website Page | Raw Dataset | Grain |
|------|------|------|
| Events list | event_directory | one row per event |
| Event page | event_details | one row per fight |
| Fight page | fight_details | one row per fight statistics page |
| Fighters list | fighter_directory | one row per fighter |
| Fighter profile | fighter_details | one row per fighter snapshot |

Raw datasets preserve original HTML values exactly as observed.

---

## Pipeline Behavior

Each run performs a full scrape of the site structure:

1. Discover events
2. Discover fights on each event
3. Discover fighters
4. Scrape fight statistics pages
5. Scrape fighter statistics pages

All outputs from a run belong together and must not be mixed with another run.

---

## Run Organization

Each execution produces a snapshot:



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
data/raw/
event_directory__ufcstats__YYYY-MM-DD.csv
event_details__ufcstats__YYYY-MM-DD.csv
fight_details__ufcstats__YYYY-MM-DD.csv
fighter_directory__ufcstats__YYYY-MM-DD.csv
fighter_details__ufcstats__YYYY-MM-DD.csv

---

## Raw Data Principles

Raw data is:

- immutable
- untyped
- uncleaned
- source-faithful

Examples:

- Heights remain `"6' 1\""`
- Time remains `"3:42"`
- Percentages remain `"54%"`

No parsing occurs in the raw layer.

---

## Raw Relational Structure

```mermaid
erDiagram

    EVENT_DIRECTORY {
        string event_url PK
        string event_name
        string event_date_raw
        string event_location_raw
        date   snapshot
    }

    EVENT_DETAILS {
        string fight_url PK
        string event_url FK
        string event_name
        string fighter_1_url FK
        string fighter_2_url FK
        int    fight_order
        date   snapshot
    }

    FIGHT_DETAILS {
        string fight_url PK
        string event_url FK
        string fighter_1_url FK
        string fighter_2_url FK
        string weight_class_raw
        string method_raw
        string round_raw
        string time_raw
        date   snapshot
    }

    FIGHTER_DIRECTORY {
        string fighter_url PK
        string fighter_name
        string height_raw
        string weight_raw
        string reach_raw
        date   snapshot
    }

    FIGHTER_DETAILS {
        string fighter_url PK
        string first_name
        string last_name
        string dob_raw
        string slpm
        string str_acc
        string sapm
        string str_def
        string td_avg
        string td_acc
        string td_def
        string sub_avg
        date   snapshot
    }

    EVENT_DIRECTORY ||--o{ EVENT_DETAILS : contains
    EVENT_DETAILS ||--|| FIGHT_DETAILS : expands_to
    FIGHTER_DIRECTORY ||--|| FIGHTER_DETAILS : expands_to

    FIGHTER_DIRECTORY ||--o{ EVENT_DETAILS : fighter_1
    FIGHTER_DIRECTORY ||--o{ EVENT_DETAILS : fighter_2


