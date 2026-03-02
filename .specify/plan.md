# Implementation Plan: Convenience Store Accounts Payable Processing

**Branch**: `main` | **Date**: 2026-03-02 | **Updated**: 2026-03-03 | **Spec**: `.specify/spec.md`

## Summary

Build a demo that processes 100+ PDF invoices from convenience store distributors using Snowflake AI_EXTRACT, stores structured results in Snowflake tables, automates new file processing with Tasks/Streams, and visualizes everything in a Streamlit in Snowflake app on container runtime.

## Technical Context

**Language/Version**: Python 3.11, SQL
**Primary Dependencies**: reportlab (PDF generation), Streamlit 1.35+, Plotly
**Storage**: Snowflake tables + internal stage for PDFs
**Target Platform**: Snowflake (Streamlit container runtime + SPCS compute pool)
**Project Type**: Customer demo / proof-of-concept

## Tech Stack

### Data Generation

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| PDF creation | **reportlab** | Pure Python, no external deps, full PDF control |
| Output | 100 initial + 5 demo PDFs | Realistic invoice content |

### SQL Pipeline

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Extraction | **AI_EXTRACT** | Snowflake-native document AI, no external OCR |
| Automation | **Task + Stream** | Event-driven, zero compute when idle |
| Storage | **Snowflake tables** | Structured extraction results + AP ledger |

### Streamlit App

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Runtime | **Container runtime** | Shared server, fast load, caching support |
| Charts | **Plotly** | Interactive, good defaults, widely available |
| Tables | **st.dataframe** | Native Streamlit, sortable/filterable |
| Layout | **st.columns + st.metric** | Clean KPI cards, responsive layout |

### Infrastructure

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Compute pool | **CPU_X64_XS** | Minimal cost for Streamlit container |
| Warehouse | **X-Small** | Sufficient for extraction + queries |
| Stage | **Internal named stage** | Simple, no external storage needed |

## Project Structure

```
convenience-store-accounts-payable/
├── .specify/                      # Spec-kit artifacts (this directory)
│   ├── constitution.md
│   ├── spec.md
│   ├── plan.md                    # This file
│   └── tasks.md
├── data/
│   ├── generate_invoices.py       # Creates 100 + 5 PDFs
│   ├── invoices/                  # 100 initial PDFs (gitignored)
│   └── demo_invoices/             # 5 demo PDFs (gitignored)
├── docs/
│   └── *.png                      # App screenshots for README
├── scripts/
│   └── capture_screenshots.py     # Async Playwright screenshot automation
├── sql/
│   ├── 01_setup.sql               # DB, schema, warehouse, stage, compute pool
│   ├── 02_tables.sql              # All tables + vendor seed data
│   ├── 03_extract.sql             # Batch AI_EXTRACT
│   ├── 04_task.sql                # Stream + proc + task
│   ├── 05_views.sql               # 8 analytical views
│   ├── 06_tests.sql               # 58 SQL E2E validation tests
│   ├── 07_generate_udf.sql        # Python UDTF for in-Snowflake PDF generation
│   └── 08_grants.sql              # Re-runnable role grants
├── streamlit/
│   ├── streamlit_app.py           # Landing page (architecture, business value)
│   ├── config.py                  # Dynamic environment config (CURRENT_DATABASE/SCHEMA)
│   ├── pages/
│   │   ├── 0_Dashboard.py         # KPI dashboard
│   │   ├── 1_AP_Ledger.py         # Invoice ledger with aging + drill-down
│   │   ├── 2_Analytics.py         # Spend analytics (6 chart types)
│   │   ├── 3_Process_New.py       # Live extraction + UDTF invoice generation
│   │   └── 4_AI_Extract_Lab.py    # Interactive AI_EXTRACT prompt builder
│   ├── pyproject.toml             # Dependencies + pytest config
│   ├── environment.yml            # Conda environment for container runtime
│   └── tests/                     # 146 Playwright E2E tests
│       ├── conftest.py
│       ├── test_functional/       # 6 page-level test files
│       └── test_integration/      # 2 cross-page test files
├── deploy.sh                      # One-command deploy (env var configurable)
├── teardown.sh                    # Drop all Snowflake objects
├── README.md
├── DESIGN.md                      # Consolidated design document
├── LICENSE                        # Apache 2.0
└── .gitignore
```

## Data Flow

```
┌──────────────┐     PUT to stage      ┌─────────────────┐
│  PDF Invoices │ ───────────────────► │ @INVOICE_STAGE   │
│  (100 + 5)   │                       └────────┬────────┘
└──────────────┘                                │
                                                │ AI_EXTRACT
                                                ▼
                                   ┌────────────────────────┐
                                   │  EXTRACTED_INVOICES     │ (headers)
                                   │  EXTRACTED_LINE_ITEMS   │ (details)
                                   └────────────┬───────────┘
                                                │
                              ┌─────────────────┼──────────────────┐
                              ▼                 ▼                  ▼
                     ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                     │  AP Ledger   │  │  Analytics   │  │  Live Demo   │
                     │  (aging,     │  │  (vendor,    │  │  (stage new, │
                     │   status)    │  │   trends)    │  │   extract)   │
                     └──────────────┘  └──────────────┘  └──────────────┘
                              └─────────────────┼──────────────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │  Streamlit in         │
                                    │  Snowflake (container)│
                                    └───────────────────────┘
```

## Automation Flow

```
New PDF on stage
       │
       ▼
INSERT into RAW_INVOICES (extracted = FALSE)
       │
       ▼
Stream RAW_INVOICES_STREAM detects new rows
       │
       ▼
Task EXTRACT_NEW_INVOICES_TASK fires (every 5 min or on-demand via EXECUTE TASK)
       │
       ▼
Stored Proc SP_EXTRACT_NEW_INVOICES:
  - Reads unprocessed files from RAW_INVOICES
  - Runs AI_EXTRACT for headers → EXTRACTED_INVOICES
  - Runs AI_EXTRACT for line items → EXTRACTED_LINE_ITEMS
  - Marks RAW_INVOICES.extracted = TRUE
```
