# Convenience Store Accounts Payable Demo Constitution

## Core Principles

### I. Demo-First Design

This is a customer-facing demo, not a production application. Every design decision optimizes for:
- Visual impact in a 20-minute meeting
- Single-command deployability to any Snowflake account
- Minimal prerequisites (no external services, API keys, or complex config)
- A clear "wow moment" when AI_EXTRACT processes invoices live on stage

### II. Snowflake-Native Architecture

The demo must showcase Snowflake capabilities, not generic Python data processing:
- All data lives in Snowflake tables
- PDF extraction uses `AI_EXTRACT` — no external OCR services
- Scheduling uses Snowflake Tasks + Streams — no Airflow or cron
- Visualization runs in Streamlit in Snowflake on container runtime
- Authentication uses Snowflake-native auth (zero app-level auth code)

### III. End-to-End Pipeline Story

The demo tells a complete story from raw PDF to actionable insight:
1. **Ingest**: PDF invoices land on a Snowflake stage
2. **Extract**: AI_EXTRACT pulls structured data (headers + line items)
3. **Automate**: A Task/Stream pipeline processes new files automatically
4. **Visualize**: Streamlit app shows AP ledger, aging, and spend analytics
5. **Live Demo**: Presenter stages new PDFs and watches extraction happen in real-time

### IV. Realistic Synthetic Data

The demo data must be believable in a customer meeting:
- Real distributor names (McLane, Core-Mark, Coca-Cola, Frito-Lay)
- Real convenience store product names (not "Product A")
- Realistic invoice amounts, payment terms, and date ranges
- 100 invoices for initial load + 5 held back for live demo

### V. Streamlit Interactivity

The Streamlit app goes beyond visualization — it supports live document processing:
- Pages cover KPI Dashboard, AP Ledger, Analytics, live extraction, and an AI Extract Lab
- Charts use Plotly for interactivity
- Clean layout with KPI cards and filterable tables
- The "Process New Invoices" page generates PDFs via an in-Snowflake UDTF and runs extraction live
- The "AI Extract Lab" page supports file upload, custom prompt building, and interactive AI_EXTRACT testing

### VI. Human-in-the-Loop Review

AI extraction is not perfect. The system includes a correction workflow:
- Every correction is an INSERT into an append-only audit table — never UPDATE or DELETE
- Views use `COALESCE(correction, original)` so corrections override AI output transparently
- `ROW_NUMBER() OVER (PARTITION BY id ORDER BY reviewed_at DESC)` picks the latest correction
- Full audit trail: who changed what, when, with reviewer notes
- Pattern applies to both document-level reviews (`INVOICE_REVIEW`) and line-item reviews (`LINE_ITEM_REVIEW`)

### VII. Config-Driven Extensibility

Adding a new document type requires zero code changes:
- `DOCUMENT_TYPE_CONFIG` table stores extraction prompts, field labels, and table column labels per doc type
- Generic columns (`field_1`..`field_10`, `col_1`..`col_5`) map to any document schema
- All Streamlit pages read config at runtime and adapt UI labels dynamically
- Built-in types: INVOICE, CONTRACT, RECEIPT, UTILITY_BILL — add more via INSERT

### VIII. Multi-Cloud Parity

The same code must produce identical results on every Snowflake cloud:
- Deployed and tested on AWS (US East 1), Azure (East US 2), and GCP (US Central 1)
- CI runs the full test suite against all three clouds on every push
- Cross-region inference (`CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'`) handles region availability
- No cloud-specific code paths — `CURRENT_DATABASE()` / `CURRENT_SCHEMA()` resolve dynamically

## Development Standards

### SQL Excellence

- No `SELECT *` — explicitly name columns
- CTEs over subqueries for readability
- Comments explaining business logic
- Queries are maintainable by an SE who isn't a SQL expert

### Cost Awareness

- Default to `CPU_X64_XS` compute pool
- X-Small warehouse for queries
- Task runs only when stream has data (no wasted compute)
- Teardown script removes all objects

### Security & RBAC

- Dedicated `AI_EXTRACT_APP` role with least-privilege grants — not ACCOUNTADMIN
- `SNOWFLAKE.CORTEX_USER` database role for Cortex access
- All user-facing SQL uses `params=[]` (parameterized queries) — no f-string interpolation
- Object ownership transferred to SYSADMIN with managed access on schema
- ACCOUNTADMIN only needed for initial role creation and Cortex grants

### Test Coverage

- Target: ~1000 automated tests across unit, SQL integration, and E2E (Playwright)
- Every Streamlit page has E2E coverage
- RBAC, concurrency, data drift, extraction accuracy, and deployment readiness are tested
- CI matrix runs all tests on 3 clouds (AWS, Azure, GCP) on every push
- E2E save tests include DB round-trip verification and test data cleanup

## Governance

This constitution guides all development decisions. When in doubt:
1. Does it make the demo more impressive? Do it.
2. Does it add complexity without demo value? Skip it.
3. Would an SE be confused by the code? Simplify it.

**Version**: 1.2 | **Ratified**: 2026-03-02 | **Updated**: 2026-03-11
