# Changelog

All notable changes to the AI Extract POC are documented in this file.

## [Unreleased]

### Added
- Bounds validation on line item save paths (negative quantity, NUMBER(12,2) range)
- 14-day `DATA_RETENTION_TIME_IN_DAYS` on EXTRACTED_FIELDS, INVOICE_REVIEW, LINE_ITEM_REVIEW
- SECURITY.md with threat model, RBAC summary, and data handling policy
- CHANGELOG.md

### Changed
- All Streamlit pages now wrap SQL queries in try/except with `st.error()` fallback
- Architecture diagram updated with LINE_ITEM_REVIEW, DOCUMENT_TYPE_CONFIG, V_DOCUMENT_SUMMARY, V_LINE_ITEM_DETAIL
- File structure in README updated with 09_line_item_review.sql
- 10_harden.sql now includes data retention settings

## [0.9.0] — 2025-06-10

### Added
- Line item writeback with append-only audit trail (LINE_ITEM_REVIEW table, V_LINE_ITEM_DETAIL view)
- `make redeploy-streamlit` target for container runtime cache flush
- Numpy type binding fix (`.item()` conversion for Snowpark params)

## [0.8.0] — 2025-06-09

### Added
- E2E browser tests with Playwright (dashboard, analytics, document viewer, review, admin, multi-doc)
- Snowflake-branded CSS theming (sidebar branding, custom colors)
- RBAC hardening: ownership transfer to SYSADMIN, BIND SERVICE ENDPOINT revocation
- DEMO.md presenter walkthrough

## [0.7.0] — 2025-06-08

### Added
- Cross-cloud CI/CD pipeline (AWS, Azure, GCP)
- PAT-based authentication for CI
- Date validation and input validation on Streamlit forms
- 1,098 tests across unit, integration, SQL, and E2E

## [0.6.0] — 2025-06-07

### Added
- Edge-case tests: SQL injection, unicode, overflow, boundary values
- User workflow E2E: approve, correct, re-edit, audit trail, rollback
- VARIANT crash fix with TRY_TO_* functions
- COALESCE priority in views (corrections > extracted values)

## [0.5.0] — 2025-06-06

### Added
- Multi-document-type support (DOCUMENT_TYPE_CONFIG, config-driven prompts/labels)
- Dynamic field rendering from raw_extraction VARIANT
- Config-driven stored procedure (SP_EXTRACT_DOCUMENTS)
- Contract and receipt extraction templates

## [0.4.0] — 2025-06-05

### Added
- Review & Approve page with inline data_editor and append-only audit trail
- INVOICE_REVIEW table with V_DOCUMENT_SUMMARY view
- Extraction failure alert (11_alerts.sql)
- Resource monitor (100 credits/month)

## [0.3.0] — 2025-06-04

### Added
- Analytics page (vendor bar chart, monthly trend, aging distribution, top items)
- Document Viewer with PDF rendering (pypdfium2)
- Stream + Task automation (5-minute polling)

## [0.2.0] — 2025-06-03

### Added
- Batch extraction pipeline (entity + table extraction)
- Analytical views (V_DOCUMENT_LEDGER, V_EXTRACTION_STATUS, V_AGING_SUMMARY)
- Dashboard with KPI cards

## [0.1.0] — 2025-06-02

### Added
- Initial POC: database, schema, warehouse, stage, RBAC role
- RAW_DOCUMENTS, EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA tables
- Single-file test script (03_test_single_file.sql)
- Sample invoice generator (generate_sample_docs.py)
