# Implementation Plan: Convenience Store Accounts Payable Processing

**Branch**: `main` | **Date**: 2026-03-02 | **Updated**: 2026-03-11 | **Spec**: `.specify/spec.md`

## Summary

Build a demo that processes 100+ PDF invoices from convenience store distributors using Snowflake AI_EXTRACT, stores structured results in Snowflake tables, automates new file processing with Tasks/Streams, and visualizes everything in a Streamlit in Snowflake app on container runtime.

## Technical Context

**Language/Version**: Python 3.11, SQL
**Primary Dependencies**: reportlab (PDF generation), Streamlit 1.35+, Plotly, snowflake-connector-python
**Storage**: Snowflake tables + internal stages for PDFs (per document type)
**Target Platform**: Snowflake (Streamlit container runtime + SPCS compute pool)
**Testing**: pytest, Playwright (E2E), snowflake-connector-python (SQL integration)
**CI/CD**: GitHub Actions (3-cloud matrix: AWS, Azure, GCP)
**Project Type**: Customer demo / proof-of-concept (POC Kit)

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
| Tables | **st.dataframe / st.data_editor** | Native Streamlit, sortable/filterable; data_editor for inline corrections |
| Layout | **st.columns + st.metric** | Clean KPI cards, responsive layout |

### Testing

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Unit tests | **pytest** | Standard Python test framework |
| E2E tests | **Playwright** | Cross-browser, auto-wait, reliable selectors |
| SQL integration | **snowflake-connector-python** | Direct SQL execution against live Snowflake |
| CI | **GitHub Actions** | Matrix strategy for 3-cloud parallel runs |

### Infrastructure

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Compute pool | **CPU_X64_XS** | Minimal cost for Streamlit container |
| Warehouse | **X-Small** | Sufficient for extraction + queries |
| Stage | **Internal named stage** | Simple, no external storage needed |

## Project Structure

```
convenience-store-accounts-payable/
├── .specify/                          # Spec-driven development artifacts
│   ├── constitution.md                # Core principles + dev standards
│   ├── spec.md                        # User stories + requirements
│   ├── plan.md                        # This file — architecture + data flows
│   └── tasks.md                       # Phase-by-phase task tracking
├── .github/workflows/
│   └── test.yml                       # CI: unit + SQL×3 + E2E×3 + summary (8 jobs)
├── poc/                               # POC Kit (primary deliverable)
│   ├── deploy_poc.sh                  # One-command deploy (11-step, env var configurable)
│   ├── teardown_poc.sh                # Drop all POC objects
│   ├── Makefile                       # Common commands: deploy, test, server, upload-streamlit
│   ├── sql/                           # SQL scripts (run in order)
│   │   ├── 01_setup.sql               # DB, schema, WH, role, compute pool, stages
│   │   ├── 02_tables.sql              # RAW_DOCUMENTS, EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA
│   │   ├── 03_extract.sql             # Batch AI_EXTRACT (headers + line items)
│   │   ├── 04_views.sql               # V_DOCUMENT_SUMMARY, V_EXTRACTION_STATUS, analytics views
│   │   ├── 05_automation.sql          # Stream + stored proc + Task (auto-extract new files)
│   │   ├── 06_generate_udf.sql        # Python UDTF for in-Snowflake PDF generation
│   │   ├── 07_review.sql              # INVOICE_REVIEW table + V_INVOICE_SUMMARY view
│   │   ├── 08_grants.sql              # RBAC: AI_EXTRACT_APP role grants
│   │   ├── 09_document_types.sql      # DOCUMENT_TYPE_CONFIG table + seed data (4 types)
│   │   ├── 09_line_item_review.sql    # LINE_ITEM_REVIEW table + V_LINE_ITEM_DETAIL view
│   │   ├── 10_generate_all_types.sql  # Multi-type document generation (CONTRACT, RECEIPT, UTILITY_BILL)
│   │   └── 11_alerts.sql              # Extraction failure alerts
│   ├── streamlit/                     # Streamlit app (deployed to Snowflake)
│   │   ├── streamlit_app.py           # Landing page (architecture diagram, business value)
│   │   ├── config.py                  # Dynamic env config (CURRENT_DATABASE/SCHEMA)
│   │   ├── pages/
│   │   │   ├── 1_Dashboard.py         # KPI metrics + extraction status charts
│   │   │   ├── 2_Document_Viewer.py   # Single-doc viewer + line item editor
│   │   │   ├── 3_Analytics.py         # Spend analytics (vendor, category, time)
│   │   │   ├── 4_Review.py            # Inline data_editor for document-level corrections
│   │   │   └── 5_Admin.py             # Config viewer, extraction triggers, AI Extract Lab
│   │   ├── pyproject.toml             # Dependencies + pytest config
│   │   └── environment.yml            # Conda environment for container runtime
│   ├── tests/                         # Test suite (~1100 tests across 44+ files)
│   │   ├── conftest.py                # Shared fixtures (sf_cursor, Snowflake connection)
│   │   ├── e2e/                       # Playwright E2E tests (121 tests)
│   │   │   ├── conftest.py            # E2E fixtures (live_server, page, BASE_URL)
│   │   │   ├── test_dashboard.py      # Dashboard page E2E
│   │   │   ├── test_document_viewer.py # Document Viewer + line item E2E
│   │   │   ├── test_analytics.py      # Analytics page E2E
│   │   │   ├── test_review.py         # Review page E2E
│   │   │   ├── test_admin.py          # Admin page E2E
│   │   │   └── test_line_item_writeback.py # Line item save + DB round-trip E2E
│   │   ├── sql/                       # SQL integration tests (~400 tests)
│   │   │   ├── test_objects.py        # Table/view existence + schema validation
│   │   │   ├── test_data_quality.py   # Row counts, NULL checks, type accuracy
│   │   │   ├── test_rbac.py           # Role grants, privilege verification
│   │   │   ├── test_concurrency.py    # Concurrent extraction + review writes
│   │   │   └── test_deployment_readiness.py # Cross-cloud deployment checks
│   │   └── unit/                      # Pure Python unit tests (~600 tests)
│   │       ├── test_config.py         # Config module unit tests
│   │       ├── test_sql_scripts.py    # SQL file parsing + syntax validation
│   │       └── ...                    # Page-level unit tests
│   ├── data/
│   │   ├── generate_invoices.py       # Creates 100 + 5 PDFs locally
│   │   └── invoices/                  # Generated PDFs (gitignored)
│   ├── README.md                      # Comprehensive setup + usage guide (800+ lines)
│   ├── DEMO.md                        # Presenter-facing demo walkthrough
│   └── ADMIN_GUIDE.md                 # Admin operations guide
├── data/                              # Original demo app data generation
│   ├── generate_invoices.py
│   └── invoices/
├── sql/                               # Original demo app SQL (01-08)
├── streamlit/                         # Original demo app Streamlit
├── deploy.sh                          # Original demo app deploy
├── DESIGN.md                          # Consolidated design document (Phases 1-15)
├── README.md                          # Root README (demo app + POC kit overview)
├── LICENSE                            # Apache 2.0
└── .gitignore
```

## Data Flow — Extraction Pipeline

```
┌──────────────────┐    PUT to stage     ┌─────────────────────────────┐
│  PDF Documents   │ ──────────────────► │ @DOCUMENT_STAGE/<type>/     │
│  (130 across     │                     │  invoices/, contracts/,     │
│   4 doc types)   │                     │  receipts/, utility_bills/  │
└──────────────────┘                     └────────────┬────────────────┘
                                                      │
                                              INSERT into RAW_DOCUMENTS
                                                      │
                                                      ▼
                                         ┌────────────────────────┐
                                         │  RAW_DOCUMENTS          │
                                         │  (filename, doc_type,   │
                                         │   extracted=FALSE)      │
                                         └────────────┬───────────┘
                                                      │
                                              AI_EXTRACT (per type,
                                              using DOCUMENT_TYPE_CONFIG
                                              extraction prompts)
                                                      │
                                    ┌─────────────────┴──────────────────┐
                                    ▼                                    ▼
                       ┌────────────────────────┐         ┌──────────────────────────┐
                       │  EXTRACTED_FIELDS       │         │  EXTRACTED_TABLE_DATA     │
                       │  (field_1..field_10)    │         │  (col_1..col_5, row_index)│
                       │  Header-level data      │         │  Line items / table rows  │
                       └────────────────────────┘         └──────────────────────────┘
```

## Data Flow — Review Writeback

```
Document-Level Review (Review page):
┌─────────────────┐    st.data_editor    ┌──────────────────────┐
│  V_DOCUMENT_    │ ◄──── reads ──────  │  Review Page          │
│  SUMMARY        │                      │  (inline editing)     │
└────────┬────────┘                      └──────────┬───────────┘
         │                                          │ Save clicked
         │ COALESCE(correction, original)           │ INSERT (append-only)
         │ ROW_NUMBER() for latest                  ▼
         │                               ┌──────────────────────┐
         └────────── reads ─────────────│  INVOICE_REVIEW       │
                                         │  (corrected_field_*,  │
                                         │   reviewed_by,        │
                                         │   reviewed_at, notes) │
                                         └──────────────────────┘

Line-Item Review (Document Viewer page):
┌─────────────────┐    st.data_editor    ┌──────────────────────┐
│  V_LINE_ITEM_   │ ◄──── reads ──────  │  Document Viewer      │
│  DETAIL         │                      │  (line item editor)   │
└────────┬────────┘                      └──────────┬───────────┘
         │                                          │ Save clicked
         │ COALESCE(correction, original)           │ INSERT (append-only)
         │ ROW_NUMBER() for latest                  ▼
         │                               ┌──────────────────────┐
         └────────── reads ─────────────│  LINE_ITEM_REVIEW     │
                                         │  (corrected_col_*,    │
                                         │   reviewed_by,        │
                                         │   reviewed_at, notes) │
                                         └──────────────────────┘
```

## Automation Flow

```
New PDF on stage
       │
       ▼
INSERT into RAW_DOCUMENTS (extracted = FALSE)
       │
       ▼
Stream RAW_DOCUMENTS_STREAM detects new rows
       │
       ▼
Task EXTRACT_NEW_DOCUMENTS_TASK fires (every 5 min or on-demand via EXECUTE TASK)
       │
       ▼
Stored Proc SP_EXTRACT_NEW_DOCUMENTS:
  - Reads unprocessed files from RAW_DOCUMENTS
  - Looks up extraction prompt from DOCUMENT_TYPE_CONFIG
  - Runs AI_EXTRACT for headers → EXTRACTED_FIELDS
  - Runs AI_EXTRACT for line items → EXTRACTED_TABLE_DATA
  - Marks RAW_DOCUMENTS.extracted = TRUE
```

## RBAC Architecture

```
ACCOUNTADMIN (initial setup only)
       │
       ├── CREATE ROLE AI_EXTRACT_APP
       ├── GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_EXTRACT_APP
       └── GRANT ROLE AI_EXTRACT_APP TO USER <deployer>
              │
              │  AI_EXTRACT_APP role has:
              ├── USAGE on DB: AI_EXTRACT_POC
              ├── USAGE + CREATE TABLE on SCHEMA: DOCUMENTS
              ├── USAGE on WH: AI_EXTRACT_WH
              ├── SELECT on all tables + views
              ├── INSERT on RAW_DOCUMENTS, EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA
              ├── INSERT on INVOICE_REVIEW, LINE_ITEM_REVIEW
              ├── READ + WRITE on stages (@DOCUMENT_STAGE)
              └── USAGE on Task, Stream, Stored Proc, UDTF

SYSADMIN
       └── Owns all objects (schema uses MANAGED ACCESS)
```

## SQL Script Dependency Order

Scripts must run in numeric order. `deploy_poc.sh` handles this automatically.

| Script | Purpose | Dependencies |
|--------|---------|-------------|
| `01_setup.sql` | DB, schema, WH, role, compute pool, stages | None (ACCOUNTADMIN) |
| `02_tables.sql` | RAW_DOCUMENTS, EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA | 01 |
| `03_extract.sql` | Batch AI_EXTRACT for all staged documents | 01, 02 |
| `04_views.sql` | V_DOCUMENT_SUMMARY, V_EXTRACTION_STATUS, analytics views | 02, 03 |
| `05_automation.sql` | Stream + SP + Task for auto-extraction | 02 |
| `06_generate_udf.sql` | GENERATE_INVOICE_PDF Python UDTF | 01 |
| `07_review.sql` | INVOICE_REVIEW table + V_INVOICE_SUMMARY view | 02, 04 |
| `08_grants.sql` | AI_EXTRACT_APP role grants on all objects | 01-07 |
| `09_document_types.sql` | DOCUMENT_TYPE_CONFIG table + 4 type seeds | 01 |
| `09_line_item_review.sql` | LINE_ITEM_REVIEW table + V_LINE_ITEM_DETAIL view | 02 |
| `10_generate_all_types.sql` | Generate CONTRACT, RECEIPT, UTILITY_BILL PDFs | 06, 09 |
| `11_alerts.sql` | Extraction failure alert notifications | 05 |

## CI/CD — GitHub Actions

The CI workflow (`.github/workflows/test.yml`) runs 8 jobs on every push to `main`:

```
Push to main
       │
       ├── unit-tests (pytest, no Snowflake connection)
       │
       ├── sql-tests-aws    (POC_CONNECTION=ci, AWS account QIB24518)
       ├── sql-tests-azure   (POC_CONNECTION=ci, Azure account ADA11264)
       ├── sql-tests-gcp     (POC_CONNECTION=ci, GCP account)
       │
       ├── e2e-tests-aws    (Playwright, headless Chromium, AWS)
       ├── e2e-tests-azure   (Playwright, headless Chromium, Azure)
       ├── e2e-tests-gcp     (Playwright, headless Chromium, GCP)
       │
       └── summary (wait for all, report pass/fail)
```

- **Matrix strategy**: `cloud: [aws, azure, gcp]` × `test-type: [sql, e2e]` + unit + summary
- **Secrets**: Per-cloud connection configs stored as GitHub Actions secrets
- **Env var**: `POC_CONNECTION=ci` — tests use this to select the right Snowflake connection
- **Cross-region inference**: `CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'` handles model availability

## Test Architecture

| Category | Count | Location | What It Tests |
|----------|-------|----------|---------------|
| Unit (Python) | ~600 | `poc/tests/unit/` | Config, SQL parsing, page logic, helpers |
| SQL Integration | ~400 | `poc/tests/sql/` | Object existence, data quality, RBAC, concurrency, deployment readiness |
| E2E (Playwright) | ~120 | `poc/tests/e2e/` | Every Streamlit page, save workflows, DB round-trip verification |
| **Total** | **~1100** | **44+ files** | |

Key testing patterns:
- **sf_cursor fixture**: Shared Snowflake cursor across SQL tests (session-scoped)
- **DB round-trip verification**: E2E save tests INSERT, then SELECT to verify data landed
- **Test data cleanup**: E2E tests DELETE their test rows after verification
- **Viewport guards**: E2E tests set explicit viewport size to prevent flaky element visibility
- **Data guards**: E2E tests skip gracefully if required data is missing (vs. failing)
