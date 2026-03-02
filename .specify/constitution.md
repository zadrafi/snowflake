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

## Governance

This constitution guides all development decisions. When in doubt:
1. Does it make the demo more impressive? Do it.
2. Does it add complexity without demo value? Skip it.
3. Would an SE be confused by the code? Simplify it.

**Version**: 1.1 | **Ratified**: 2026-03-02 | **Updated**: 2026-03-03
