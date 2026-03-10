# AI_EXTRACT POC — Demo Walkthrough

A presenter-facing guide for walking through the AI_EXTRACT POC. Covers talking points per page, audience-specific paths, and "wow moments" to highlight.

---

## Pre-Flight Checklist

Before starting the demo, confirm:

- [ ] Snowsight is open and logged in
- [ ] Streamlit app is running — navigate to **Projects > Streamlit > AI_EXTRACT_DASHBOARD**
- [ ] You have at least one fresh (unextracted) document ready to upload for the live extraction moment
- [ ] Warehouse `AI_EXTRACT_WH` is running (it auto-resumes, but cold start adds ~10s)
- [ ] Browser zoom is at 100% (Streamlit renders best at default zoom)

**Optional prep:**
- Pre-open a sample PDF in a browser tab to show what the source document looks like
- Have Snowsight open to `EXTRACTED_FIELDS` table in a second tab for the "show me the data" moment

---

## Audience Paths

### Executive Path (~5 minutes)

Best for: CTO, VP of Finance, business stakeholders who care about outcomes, not implementation.

1. **Landing page** — 30s — "Here's what we built: AI reads your documents and extracts structured data. No external APIs, everything stays in Snowflake."
2. **Dashboard** — 60s — Walk through KPI cards (total docs, total amount, unique vendors, overdue count). Point at the recent documents table.
3. **Analytics** — 60s — Show vendor spend breakdown, monthly trend, aging distribution. "This is auto-generated from extracted data — no manual data entry."
4. **Review** — 60s — Show inline editing. "Humans stay in the loop. Every correction is audited."
5. **Wrap** — 60s — "This runs on your data, in your account, with role-based access. Ready to try with your documents?"

### Technical Path (~15 minutes)

Best for: Data engineers, architects, Snowflake admins who want to understand the how.

1. **Landing page** — 60s — Architecture overview, pipeline status counters
2. **Document Viewer** — 3min — Filter by doc type, drill into a document, show side-by-side PDF + extracted fields
3. **Dashboard + Analytics** — 2min — KPIs, vendor breakdown, monthly trend
4. **Review** — 2min — Inline editing, audit trail, COALESCE override pattern
5. **Admin** — 2min — Document type config, extraction schema definitions
6. **Live extraction** — 3min — Upload a new doc, run extraction, show it appear in the dashboard
7. **Under the hood** — 2min — Show SQL: `AI_EXTRACT()` call, `LATERAL FLATTEN`, dynamic table, stream + task automation

---

## Page-by-Page Talking Points

### Landing Page (`streamlit_app.py`)

**What it shows:** Pipeline status (total / extracted / pending / failed), architecture overview.

**Key talking points:**
- "This is a complete document processing pipeline built entirely in Snowflake"
- "No external services — AI_EXTRACT runs inside Snowflake's Cortex layer"
- "Documents are staged, extracted by AI, then available for review and analytics"
- Point at the extraction status counters — "We've processed X documents across 4 document types"

**Wow moment:** The counters update live as new documents are processed.

---

### Dashboard (`pages/0_Dashboard.py`)

**What it shows:** KPI metric cards, recent documents table.

**Key talking points:**
- "Four KPIs at a glance: total documents, total extracted value, unique senders, overdue count"
- "The recent documents table shows the latest extractions with status"
- "All of this is powered by SQL views on top of the extracted data — no ETL pipeline needed"

**Wow moment:** The overdue count and aging are computed automatically from extracted due dates.

---

### Document Viewer (`pages/1_Document_Viewer.py`)

**What it shows:** Document browser with filters, drill-down to see extracted fields alongside rendered PDF.

**Key talking points:**
- "Filter by document type, sender, or extraction status"
- "Click any document to see the source PDF side-by-side with extracted fields"
- "The AI pulled out vendor name, dates, amounts, line items — all from unstructured PDF"
- "This works with invoices, contracts, receipts, utility bills — any document type"

**Wow moment:** Side-by-side view — source PDF on the left, clean structured data on the right. The audience can visually verify the extraction accuracy.

**Demo tip:** Pick a document with clear, readable fields so the audience can follow along and confirm the extracted values match.

---

### Analytics (`pages/2_Analytics.py`)

**What it shows:** Vendor spend breakdown (bar chart), monthly trend (area chart), aging distribution, top line items.

**Key talking points:**
- "Spend by vendor — instantly see where your money goes"
- "Monthly trend — track volume and value over time"
- "Aging buckets — Current, 1-30 days, 31-60, 61-90, 90+ overdue"
- "Top line items — extracted from table data inside the documents"

**Wow moment:** The aging distribution is computed from AI-extracted due dates — no manual categorization.

---

### Review (`pages/3_Review.py`)

**What it shows:** Inline data editor for reviewing/correcting extractions, append-only audit trail.

**Key talking points:**
- "Human-in-the-loop: reviewers can approve, reject, or correct any extraction"
- "Every change is an INSERT, never an UPDATE — full audit trail"
- "Corrections override the AI output via COALESCE — the view always shows the best available value"
- "You can see who changed what, when, with reviewer notes"

**Wow moment:** Edit a value inline, save it, then show the audit trail — multiple rows for the same document, each with a timestamp and reviewer name. "This is append-only. Nothing is ever deleted."

**Demo tip:** Deliberately show a slightly wrong extraction (e.g., a total that's off by a penny), correct it inline, and show the audit trail update.

---

### Admin (`pages/4_Admin.py`)

**What it shows:** Document type configuration, extraction schema definitions.

**Key talking points:**
- "The system supports multiple document types — invoices, contracts, receipts, utility bills"
- "Each type has its own extraction schema defined in DOCUMENT_TYPE_CONFIG"
- "Adding a new document type is just an INSERT — define the fields and prompts, and the extraction pipeline picks it up"
- "Table extraction schemas are also configurable — line items, contract clauses, receipt items"

**Wow moment:** "Adding a new document type doesn't require any code changes — it's config-driven."

---

## Live Extraction (The "Magic Moment")

This is the most impactful part of the demo. Do this during the technical path.

### Steps:

1. **Upload a document** — Use Snowsight to upload a PDF to `DOCUMENT_STAGE`, or have one pre-staged but not yet extracted
2. **Register it** — Run in Snowsight:
   ```sql
   USE ROLE AI_EXTRACT_APP;
   USE DATABASE AI_EXTRACT_POC;
   USE SCHEMA DOCUMENTS;

   INSERT INTO RAW_DOCUMENTS (FILE_NAME, FILE_PATH, DOC_TYPE)
   VALUES ('new_invoice.pdf', '@DOCUMENT_STAGE/new_invoice.pdf', 'INVOICE');
   ```
3. **Extract** — Run the stored procedure:
   ```sql
   CALL SP_EXTRACT_BY_DOC_TYPE('INVOICE');
   ```
4. **Show results** — Switch back to the Streamlit app and refresh. The new document appears in the dashboard with fully extracted fields and line items.

**Narration:** "We just uploaded a brand-new document. The AI read it, extracted the header fields and line items, and it's already in our dashboard. No templates, no rules, no training data."

**Timing:** Extraction takes 5-15 seconds per document depending on page count. Narrate while it runs.

---

## Common Questions & Answers

**Q: How accurate is the extraction?**
A: For clean, well-formatted documents, accuracy is typically 95%+. That's why we have the review workflow — humans verify and correct the remaining cases.

**Q: What about handwritten documents?**
A: AI_EXTRACT works with scanned documents and images. Handwriting quality varies — print-style handwriting works better than cursive.

**Q: How does this handle different layouts?**
A: AI_EXTRACT uses a language model, not template matching. It understands the *meaning* of fields, so it works across different layouts and formats for the same document type.

**Q: What's the cost?**
A: Each page costs ~970 tokens. For 100 single-page invoices, that's about 97,000 tokens — roughly comparable to a few cents per document. The warehouse cost is minimal (X-SMALL is sufficient).

**Q: Can this run automatically?**
A: Yes — there's a stream + task that fires every 5 minutes when new documents are staged. Show `06_automate.sql` if they're interested.

**Q: What about security?**
A: Everything runs inside Snowflake. Documents never leave your account. The AI_EXTRACT_APP role has least-privilege access — no ACCOUNTADMIN. All objects are owned by SYSADMIN with managed access on the schema.

**Q: Can we use our own document types?**
A: Absolutely — that's the whole point. The extraction schema is config-driven. Define your fields and prompts in DOCUMENT_TYPE_CONFIG and the pipeline handles the rest.

---

## Troubleshooting During Demo

| Problem | Fix |
|---|---|
| Streamlit app shows "Loading..." for >30s | Compute pool may be suspended. Check: `DESCRIBE COMPUTE POOL AI_EXTRACT_POC_POOL;` — it auto-resumes but takes ~60s. |
| Extraction returns empty/null fields | Check the document is readable (not a scanned image with poor OCR). Try a different file. |
| "Warehouse is suspended" error | Run `ALTER WAREHOUSE AI_EXTRACT_WH RESUME;` — it auto-resumes but there's a brief delay. |
| Dashboard shows 0 documents | Verify `SELECT COUNT(*) FROM EXTRACTED_FIELDS;` returns data. If empty, run `CALL SP_EXTRACT_BY_DOC_TYPE('INVOICE');` |
| PDF viewer not rendering | The viewer uses `pypdfium2` which requires Container Runtime. Verify the compute pool is active. |

---

## After the Demo

- Share this repo with the audience (if appropriate)
- Offer to run the POC with their actual documents
- Point them to the README for self-service setup
- Highlight the test suite: "1,000+ automated tests validate every component"
