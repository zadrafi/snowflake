# Feature Specification: Convenience Store Accounts Payable Processing

**Feature Branch**: `001-ap-processing-mvp`
**Created**: 2026-03-02
**Status**: Implemented
**Updated**: 2026-03-11
**Input**: Customer demo — end-to-end PDF invoice processing using AI_EXTRACT, visualized in Streamlit in Snowflake, for convenience store chains

## User Scenarios & Testing *(mandatory)*

### User Story 1 - AP Ledger Overview (Priority: P1)

An SE opens the Streamlit app during a customer meeting with a convenience store chain's finance team. The landing page shows KPI cards: total payables outstanding, invoice count, overdue amount, and average days to pay. The AP Ledger page shows all extracted invoices with aging buckets and drill-down to line items.

**Why this priority**: The AP ledger is the core deliverable — it proves that AI_EXTRACT turned unstructured PDFs into a structured, queryable accounts payable system.

**Independent Test**: Open the Streamlit app. KPI cards load with data from 100 extracted invoices. AP Ledger table is filterable and shows aging buckets.

**Acceptance Scenarios**:

1. **Given** the app loads, **When** no filters are applied, **Then** KPI cards show aggregate AP metrics across all invoices
2. **Given** the AP Ledger page, **When** the user filters by vendor, **Then** the table updates to show only that vendor's invoices
3. **Given** an invoice row, **When** the user expands it, **Then** extracted line items (product, qty, price) are displayed

---

### User Story 2 - Spend Analytics (Priority: P1)

The SE navigates to the Analytics page showing spend by vendor (horizontal bar chart), monthly spend trend (line chart), category breakdown (treemap), and aging distribution (stacked bar). The customer sees how Snowflake turns raw invoices into actionable financial insights.

**Why this priority**: Analytics is the "so what" — it shows the business value of automated invoice processing beyond just data extraction.

**Independent Test**: Navigate to Analytics page. All charts render with data from the 100 extracted invoices. Filters work across all visualizations.

**Acceptance Scenarios**:

1. **Given** the Analytics page, **When** it loads, **Then** spend by vendor chart shows top vendors ranked by total spend
2. **Given** a date range filter, **When** applied, **Then** all charts update to reflect the selected period
3. **Given** the category breakdown, **When** displayed, **Then** product categories (beverages, snacks, tobacco, etc.) show relative spend

---

### User Story 3 - Live Extraction Demo (Priority: P1)

The SE demonstrates the "magic moment": from the Streamlit app, they stage 5 new PDF invoices, trigger the extraction task, and watch the results appear in real-time. The progress panel shows files being detected and extracted one by one.

**Why this priority**: This is the wow moment — live AI processing in front of the customer proves the system works end-to-end, not just on pre-loaded data.

**Independent Test**: Click "Stage Demo Invoices" and "Run Extraction" buttons. Progress panel shows 5/5 files processed. New invoices appear in the AP Ledger and Analytics pages.

**Acceptance Scenarios**:

1. **Given** the Process New page, **When** the user clicks "Stage Demo Invoices", **Then** 5 PDFs are PUT to the Snowflake stage and registered in RAW_INVOICES
2. **Given** files are staged, **When** the user clicks "Run Extraction", **Then** the extraction task executes and a progress panel shows live status
3. **Given** extraction completes, **When** the user navigates to AP Ledger, **Then** the 5 new invoices appear with their extracted data

---

### User Story 4 - Automated Pipeline (Priority: P2)

The SE explains that a Snowflake Task monitors the stage via a Stream. Any new files added are automatically extracted without manual intervention. The task runs every 5 minutes and only activates when new data exists.

**Why this priority**: Automation completes the story — it's not just a one-time extraction but an ongoing pipeline.

**Acceptance Scenarios**:

1. **Given** the task is enabled, **When** new files appear on stage, **Then** the stream detects them and the task processes them within 5 minutes
2. **Given** no new files, **When** the task schedule fires, **Then** no compute is consumed (stream-gated)

---

### Edge Cases

- What happens if AI_EXTRACT fails on a malformed PDF? Mark as `EXTRACTION_FAILED` with error message.
- What if the same invoice is staged twice? Deduplicate on filename.
- What if the extraction task is already running when triggered manually? Snowflake handles task concurrency — only one instance runs.

---

### User Story 5 - AI Extract Lab (Priority: P2)

The SE opens the AI Extract Lab page to interactively explore AI_EXTRACT capabilities. Three modes are available: Starter Template (pre-built prompts for invoice headers, line items, general Q&A), Visual Builder (point-and-click entity definition), and Raw JSON Editor. The SE can test extraction against any staged or uploaded PDF and see results immediately.

**Why this priority**: The lab lets technically curious customers experiment with AI_EXTRACT prompts beyond the canned demo, extending the conversation into their own use cases.

**Acceptance Scenarios**:

1. **Given** the AI Extract Lab page, **When** the user selects "Starter Template" mode, **Then** pre-built prompts are available for invoice headers, line items, and general Q&A
2. **Given** a staged PDF, **When** the user runs extraction with a custom prompt, **Then** results display in a structured format
3. **Given** the upload option, **When** the user uploads a PDF, **Then** it is staged and available for extraction

---

### User Story 6 - In-Snowflake Invoice Generation (Priority: P2)

The SE generates new PDF invoices directly inside Snowflake using a Python UDTF (`GENERATE_INVOICE_PDF`). This eliminates the need for local PDF generation and demonstrates Snowflake as a complete platform — even document creation runs inside the warehouse.

**Why this priority**: Generating PDFs inside Snowflake is a strong "platform completeness" talking point and makes the live demo self-contained.

**Acceptance Scenarios**:

1. **Given** the Process New page, **When** the user selects vendors and clicks generate, **Then** new PDF invoices are created via the UDTF and staged to @INVOICE_STAGE
2. **Given** generated invoices, **When** extraction runs, **Then** the AI_EXTRACT results match the generated content

---

### User Story 7 - Document Review Workflow (Priority: P1)

A reviewer opens the Review page and sees all extracted documents in an inline `st.data_editor` grid. They can edit any field (status, vendor name, amounts, dates, notes), then click Save. Each save INSERTs a new row into `INVOICE_REVIEW` — nothing is ever updated or deleted. The view `V_DOCUMENT_SUMMARY` always shows the latest correction per document via `ROW_NUMBER()` and `COALESCE`.

**Why this priority**: AI extraction accuracy is typically 90-95%. Human-in-the-loop correction is essential for production use. The append-only pattern provides full audit traceability.

**Acceptance Scenarios**:

1. **Given** the Review page, **When** a reviewer edits a field and clicks Save, **Then** a new row is INSERTed into `INVOICE_REVIEW` with the correction
2. **Given** multiple corrections for one document, **When** querying `V_DOCUMENT_SUMMARY`, **Then** only the latest correction is shown (via `ROW_NUMBER()`)
3. **Given** a correction, **When** the corrected field is NULL, **Then** `COALESCE` falls back to the original extracted value
4. **Given** any save, **When** querying the audit trail, **Then** all previous corrections are preserved with reviewer, timestamp, and notes

---

### User Story 8 - Line Item Review (Priority: P1)

From the Document Viewer page, a reviewer scrolls to the line item editor and edits individual line items (description, category, quantity, unit price, line total). Each save INSERTs into `LINE_ITEM_REVIEW`. The view `V_LINE_ITEM_DETAIL` overlays corrections on original `EXTRACTED_TABLE_DATA` using the same `ROW_NUMBER()` + `COALESCE` pattern.

**Why this priority**: Line items are the highest-value extraction target for AP processing. Per-line corrections are essential for reconciliation accuracy.

**Acceptance Scenarios**:

1. **Given** the Document Viewer line item editor, **When** a reviewer edits a Description cell and saves, **Then** a row is INSERTed into `LINE_ITEM_REVIEW` with `corrected_col_1`
2. **Given** the view `V_LINE_ITEM_DETAIL`, **When** a line item has corrections, **Then** `COALESCE(correction, original)` returns the corrected value
3. **Given** a fresh page load with no edits, **When** viewing line items, **Then** a "No pending changes" caption is shown

---

### User Story 9 - Multi-Document-Type Support (Priority: P2)

The system supports multiple document types (INVOICE, CONTRACT, RECEIPT, UTILITY_BILL) via a config-driven `DOCUMENT_TYPE_CONFIG` table. Each type defines its own extraction prompts, field labels, and table column labels. Adding a new type is an INSERT — no code changes required. All Streamlit pages filter by document type.

**Why this priority**: Demonstrates that the POC is not invoice-specific — it generalizes to any document type the customer needs.

**Acceptance Scenarios**:

1. **Given** the `DOCUMENT_TYPE_CONFIG` table, **When** a new doc type is INSERTed, **Then** the extraction pipeline and Streamlit pages pick it up automatically
2. **Given** the Admin page, **When** viewing config, **Then** all active document types are listed with their field labels and prompts
3. **Given** 4 built-in types, **When** filtering by type on any page, **Then** only documents of that type are shown

---

### User Story 10 - Cross-Cloud Deployment (Priority: P2)

The POC deploys identically to AWS, Azure, and GCP Snowflake accounts. The same code, same tests, same results. CI validates all three clouds on every push.

**Why this priority**: Customers run on different clouds. The POC must prove it works everywhere, not just on the SE's preferred cloud.

**Acceptance Scenarios**:

1. **Given** `deploy_poc.sh`, **When** run against any cloud, **Then** all objects are created and extraction succeeds
2. **Given** the CI workflow, **When** a push to `main` occurs, **Then** unit + SQL + E2E tests run on AWS, Azure, and GCP (8 jobs total)
3. **Given** 993+ non-E2E tests, **When** run on all 3 clouds, **Then** all pass with zero failures

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST extract vendor name, invoice number, date, due date, subtotal, tax, and total from PDF invoices using AI_EXTRACT
- **FR-002**: System MUST extract line items (product, quantity, unit price, line total) as structured table data
- **FR-003**: System MUST display an AP ledger with aging buckets (current, 1-30, 31-60, 61-90, 90+ days)
- **FR-004**: System MUST provide spend analytics by vendor, category, and time period
- **FR-005**: System MUST support live extraction of new invoices from within the Streamlit app
- **FR-006**: System MUST include a scheduled Task that auto-extracts newly staged PDFs via Stream
- **FR-007**: System MUST generate 100 realistic synthetic invoices + 5 demo invoices for live presentation
- **FR-008**: System MUST deploy with a single `deploy.sh` command
- **FR-009**: System MUST provide an interactive AI_EXTRACT prompt builder (AI Extract Lab) with starter templates, visual builder, and raw JSON editor
- **FR-010**: System MUST generate PDF invoices inside Snowflake via a Python UDTF (no local dependencies for live demo)
- **FR-011**: System MUST support dual-environment deployment via dynamic config (CURRENT_DATABASE/CURRENT_SCHEMA) with zero hardcoded values
- **FR-012**: System MUST include a comprehensive E2E test suite (Playwright) covering all pages
- **FR-013**: System MUST support an append-only review/correction workflow with inline `st.data_editor` and `INVOICE_REVIEW` table
- **FR-014**: System MUST use a dedicated RBAC role (`AI_EXTRACT_APP`) with least-privilege grants — not ACCOUNTADMIN
- **FR-015**: System MUST support multiple document types (invoices, contracts, receipts, utility bills) via configuration table with per-type prompts and UI labels
- **FR-016**: System MUST use parameterized SQL queries (`params=[]`) throughout the Streamlit app to prevent SQL injection
- **FR-017**: System MUST deploy and validate on all three Snowflake clouds (AWS, Azure, GCP) with identical test results

### Key Entities

- **Invoice**: Vendor, invoice number, date, due date, PO number, subtotal, tax, total, payment terms, status
- **Line Item**: Invoice reference, product name, quantity, unit price, line total, category
- **Vendor**: Name, normalized name, address, payment terms
- **Raw Invoice**: Staged filename, upload timestamp, extraction status
- **Review**: Document-level correction record with status, corrected fields, reviewer, timestamp
- **Line Item Review**: Line-level correction record with corrected columns, reviewer, timestamp
- **Document Type Config**: Per-type extraction prompts, field labels, column labels, stage subfolder

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100 PDF invoices are extracted with >90% field accuracy on header fields
- **SC-002**: AP Ledger displays all invoices with correct aging calculations
- **SC-003**: Live extraction of 5 demo invoices completes within 2 minutes and results appear in the app
- **SC-004**: An SE can deploy the full demo in under 15 minutes
- **SC-005**: The demo tells a complete story (ingest → extract → automate → visualize) in a 20-minute meeting
- **SC-006**: ~1000 automated tests pass (non-E2E + E2E) covering SQL objects, data quality, RBAC, concurrency, and every Streamlit page
- **SC-007**: Full test suite passes on all three Snowflake clouds (AWS, Azure, GCP) with zero failures
- **SC-008**: App runs with a least-privilege RBAC role (not ACCOUNTADMIN) on all three clouds
