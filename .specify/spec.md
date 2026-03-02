# Feature Specification: Convenience Store Accounts Payable Processing

**Feature Branch**: `001-ap-processing-mvp`
**Created**: 2026-03-02
**Status**: Implemented
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

### Key Entities

- **Invoice**: Vendor, invoice number, date, due date, PO number, subtotal, tax, total, payment terms, status
- **Line Item**: Invoice reference, product name, quantity, unit price, line total, category
- **Vendor**: Name, normalized name, address, payment terms
- **Raw Invoice**: Staged filename, upload timestamp, extraction status

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100 PDF invoices are extracted with >90% field accuracy on header fields
- **SC-002**: AP Ledger displays all invoices with correct aging calculations
- **SC-003**: Live extraction of 5 demo invoices completes within 2 minutes and results appear in the app
- **SC-004**: An SE can deploy the full demo in under 15 minutes
- **SC-005**: The demo tells a complete story (ingest → extract → automate → visualize) in a 20-minute meeting
