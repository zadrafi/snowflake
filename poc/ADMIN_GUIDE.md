# AI_EXTRACT POC — Admin Getting Started Guide

This guide is for administrators who manage the AI_EXTRACT deployment. It covers
day-to-day operations, adding new document types, monitoring, and troubleshooting.

For initial setup and deployment, see [README.md](README.md).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Environment Variables](#environment-variables)
- [Deploying to a New Account](#deploying-to-a-new-account)
- [Adding a New Document Type](#adding-a-new-document-type)
- [Managing the Extraction Task](#managing-the-extraction-task)
- [Uploading Documents](#uploading-documents)
- [Monitoring Extraction Status](#monitoring-extraction-status)
- [RBAC and Permissions](#rbac-and-permissions)
- [Updating Extraction Prompts](#updating-extraction-prompts)
- [Running Tests](#running-tests)
- [Performance Tuning](#performance-tuning)
- [Backup and Recovery](#backup-and-recovery)
- [Troubleshooting](#troubleshooting)
- [Common SQL Recipes](#common-sql-recipes)

---

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│  PDF/Image      │     │  DOCUMENT_STAGE       │     │  RAW_DOCUMENTS   │
│  (local files)  │────>│  (Snowflake stage)    │────>│  (registry)      │
└─────────────────┘ PUT └──────────────────────┘     └────────┬─────────┘
                                                              │
                         EXTRACT_NEW_DOCUMENTS_TASK           │
                         (runs every 5 min)                   │
                                                              v
                    ┌────────────────────┐    ┌───────────────────────────┐
                    │ EXTRACTED_FIELDS   │    │  EXTRACTED_TABLE_DATA     │
                    │ (entity/header)    │    │  (line items/rows)        │
                    └────────┬───────────┘    └───────────────────────────┘
                             │
                             v
                    ┌────────────────────┐    ┌───────────────────────────┐
                    │ V_INVOICE_SUMMARY  │    │  INVOICE_REVIEW           │
                    │ (view for UI)      │    │  (append-only audit)      │
                    └────────────────────┘    └───────────────────────────┘
                             │
                             v
                    ┌────────────────────┐
                    │ Streamlit App      │
                    │ (review dashboard) │
                    └────────────────────┘
```

**Key objects:**

| Object | Type | Purpose |
|--------|------|---------|
| `RAW_DOCUMENTS` | Table | File registry — one row per uploaded document |
| `EXTRACTED_FIELDS` | Table | Entity/header fields extracted from each document |
| `EXTRACTED_TABLE_DATA` | Table | Tabular/line-item data extracted from each document |
| `INVOICE_REVIEW` | Table | Append-only audit trail for human review corrections |
| `V_INVOICE_SUMMARY` | View | Joins extracted fields + latest review (for the dashboard) |
| `DOCUMENT_TYPE_CONFIG` | Table | Config-driven prompts and field mappings per doc type |
| `SP_EXTRACT_BY_DOC_TYPE` | Stored Proc | Extracts documents by type, reading config from above |
| `SP_EXTRACT_NEW_DOCUMENTS` | Stored Proc | Extracts new (unextracted) invoices |
| `EXTRACT_NEW_DOCUMENTS_TASK` | Task | Scheduled (5 min) — calls the extraction SPs |
| `DOCUMENT_STAGE` | Stage | Internal stage holding uploaded PDFs/images |

---

## Environment Variables

All scripts and the deploy tool are parameterized via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `POC_CONNECTION` | `default` | Snowflake connection name (in `~/.snowflake/connections.toml`) |
| `POC_DB` | `AI_EXTRACT_POC` | Database name |
| `POC_SCHEMA` | `DOCUMENTS` | Schema name |
| `POC_WH` | `AI_EXTRACT_WH` | Warehouse name |
| `POC_ROLE` | `AI_EXTRACT_APP` | Operational role |
| `POC_POOL` | `AI_EXTRACT_POC_POOL` | Compute pool (for SPCS) |

Example — deploy with custom names:

```bash
POC_DB=ACME_EXTRACT POC_SCHEMA=DOCS POC_WH=ACME_WH POC_ROLE=ACME_APP \
  POC_CONNECTION=my_connection ./poc/deploy_poc.sh
```

---

## Deploying to a New Account

### Prerequisites

1. A Snowflake connection configured in `~/.snowflake/connections.toml`
2. ACCOUNTADMIN access (for initial role/grant setup only)
3. Account in a [supported AI_EXTRACT region](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-extract#regional-availability), or cross-region inference enabled

### Steps

```bash
# 1. Deploy all objects
POC_CONNECTION=<your_connection> ./poc/deploy_poc.sh

# 2. Verify deployment
POC_CONNECTION=<your_connection> .venv/bin/python3 -m pytest \
  tests/test_deployment_readiness.py -v --timeout=60
```

The deploy script runs SQL files `01_setup.sql` through `11_alerts.sql` in order, uploads sample documents, and runs extraction. Total time: ~15–30 minutes depending on document count.

### What `deploy_poc.sh` Does

1. Creates database, schema, warehouse, role
2. Creates tables (`RAW_DOCUMENTS`, `EXTRACTED_FIELDS`, `EXTRACTED_TABLE_DATA`, etc.)
3. Creates the internal stage and uploads sample PDFs
4. Runs batch extraction
5. Creates views (`V_INVOICE_SUMMARY`)
6. Creates automation (stored procs + task)
7. Deploys the Streamlit app (if SPCS compute pool exists)
8. Sets up writeback tables and review views
9. Seeds document type configs (INVOICE, UTILITY_BILL, CONTRACT, RECEIPT)
10. Applies RBAC hardening
11. Creates alert monitors

---

## Adding a New Document Type

This is the most common admin task. The system is fully config-driven.

### 1. Plan Your Fields

Decide what entity fields (header data) and table columns (line items) you need.
The system supports up to 10 entity fields and 5 table columns per document type.

### 2. Insert the Configuration

```sql
USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;

INSERT INTO DOCUMENT_TYPE_CONFIG (
    doc_type, is_active, display_name,
    entity_prompt, table_prompt,
    field_1_label, field_2_label, field_3_label,
    field_4_label, field_5_label, field_6_label,
    field_7_label, field_8_label, field_9_label, field_10_label,
    col_1_label, col_2_label, col_3_label, col_4_label, col_5_label,
    stage_subfolder
) VALUES (
    'PURCHASE_ORDER',  -- doc_type (uppercase, no spaces)
    TRUE,              -- is_active
    'Purchase Order',  -- display_name (for the UI)

    -- Entity prompt: tell the AI what to extract
    'Extract these fields from the purchase order:
     - po_number: The purchase order number
     - vendor_name: Name of the vendor/supplier
     - order_date: Date the PO was created (YYYY-MM-DD)
     - delivery_date: Expected delivery date (YYYY-MM-DD)
     - ship_to_address: Delivery address
     - buyer_name: Name of the buyer/requestor
     - subtotal: Subtotal before tax
     - tax_amount: Tax amount
     - total_amount: Total amount due
     Return as JSON with these exact keys.',

    -- Table prompt: describe the line items
    'Extract ALL line items from this purchase order.
     For each item return:
     - item_number: Line item number
     - description: Item description
     - quantity: Quantity ordered
     - unit_price: Price per unit
     - line_total: Total for this line
     Return as a JSON array of objects.',

    -- Field labels (map to field_1 through field_10 in EXTRACTED_FIELDS)
    'PO Number', 'Vendor Name', 'Order Date',
    'Delivery Date', 'Ship-To Address', 'Buyer Name',
    'Subtotal', 'Tax Amount', 'Total Amount', NULL,

    -- Column labels (map to col_1 through col_5 in EXTRACTED_TABLE_DATA)
    'Item Number', 'Description', 'Quantity', 'Unit Price', 'Line Total',

    -- Stage subfolder (where PDFs go within DOCUMENT_STAGE)
    'purchase_orders'
);
```

### 3. Upload Documents

```bash
# From your local machine
snow stage copy ./my_purchase_orders/*.pdf \
  @AI_EXTRACT_POC.DOCUMENTS.DOCUMENT_STAGE/purchase_orders/ \
  --connection <your_connection> --overwrite
```

Or via SQL:

```sql
PUT 'file:///path/to/purchase_orders/*.pdf'
  @DOCUMENT_STAGE/purchase_orders/
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### 4. Register the Documents

```sql
ALTER STAGE DOCUMENT_STAGE REFRESH;

INSERT INTO RAW_DOCUMENTS (file_name, file_path, doc_type)
SELECT
    RELATIVE_PATH,
    BUILD_SCOPED_FILE_URL(@DOCUMENT_STAGE, RELATIVE_PATH),
    'PURCHASE_ORDER'
FROM DIRECTORY(@DOCUMENT_STAGE)
WHERE RELATIVE_PATH LIKE 'purchase_orders/%'
  AND RELATIVE_PATH NOT IN (SELECT file_name FROM RAW_DOCUMENTS);
```

### 5. Run Extraction

```sql
CALL SP_EXTRACT_BY_DOC_TYPE('PURCHASE_ORDER');
```

Or let the background task pick them up automatically (within 5 minutes).

### 6. Verify

```sql
-- Check extraction results
SELECT * FROM EXTRACTED_FIELDS
WHERE file_name LIKE 'purchase_orders/%'
ORDER BY extracted_at DESC;

-- Check table data
SELECT * FROM EXTRACTED_TABLE_DATA
WHERE file_name LIKE 'purchase_orders/%';
```

---

## Managing the Extraction Task

The `EXTRACT_NEW_DOCUMENTS_TASK` runs every 5 minutes and processes any
unextracted documents.

```sql
-- Check task status
SHOW TASKS LIKE 'EXTRACT_NEW%' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS;

-- Suspend (e.g., during bulk uploads to avoid duplicate processing)
ALTER TASK AI_EXTRACT_POC.DOCUMENTS.EXTRACT_NEW_DOCUMENTS_TASK SUSPEND;

-- Resume
ALTER TASK AI_EXTRACT_POC.DOCUMENTS.EXTRACT_NEW_DOCUMENTS_TASK RESUME;

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'EXTRACT_NEW_DOCUMENTS_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

**Important:** Always suspend the task before bulk uploads to prevent the
background task from processing partially-uploaded batches.

---

## Uploading Documents

### Best Practice Workflow

1. **Suspend** the extraction task
2. **Upload** files to the stage (PUT or `snow stage copy`)
3. **Refresh** the stage directory (`ALTER STAGE ... REFRESH`)
4. **Register** files in RAW_DOCUMENTS
5. **Extract** (manually via `CALL SP_EXTRACT_BY_DOC_TYPE(...)` or resume the task)
6. **Resume** the extraction task

### Via the Streamlit UI

The Admin page (if deployed) provides an upload form. Files are automatically
registered and queued for extraction.

### Via SQL

```sql
-- Step 1: Suspend task
ALTER TASK AI_EXTRACT_POC.DOCUMENTS.EXTRACT_NEW_DOCUMENTS_TASK SUSPEND;

-- Step 2: Upload (from SQL worksheet or SnowSQL)
PUT 'file:///local/path/*.pdf' @DOCUMENT_STAGE/invoices/
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Step 3: Refresh
ALTER STAGE DOCUMENT_STAGE REFRESH;

-- Step 4: Register
INSERT INTO RAW_DOCUMENTS (file_name, file_path, doc_type)
SELECT RELATIVE_PATH, BUILD_SCOPED_FILE_URL(@DOCUMENT_STAGE, RELATIVE_PATH), 'INVOICE'
FROM DIRECTORY(@DOCUMENT_STAGE)
WHERE RELATIVE_PATH LIKE 'invoices/%'
  AND RELATIVE_PATH NOT IN (SELECT file_name FROM RAW_DOCUMENTS);

-- Step 5: Extract
CALL SP_EXTRACT_BY_DOC_TYPE('INVOICE');

-- Step 6: Resume task
ALTER TASK AI_EXTRACT_POC.DOCUMENTS.EXTRACT_NEW_DOCUMENTS_TASK RESUME;
```

---

## Monitoring Extraction Status

### Quick Health Check

```sql
-- Overall counts
SELECT 'RAW_DOCUMENTS' AS tbl, COUNT(*) AS cnt FROM RAW_DOCUMENTS
UNION ALL
SELECT 'EXTRACTED_FIELDS', COUNT(*) FROM EXTRACTED_FIELDS
UNION ALL
SELECT 'EXTRACTED_TABLE_DATA', COUNT(*) FROM EXTRACTED_TABLE_DATA;

-- Unextracted documents (should be 0 if task is running)
SELECT COUNT(*) AS unextracted
FROM RAW_DOCUMENTS
WHERE EXTRACTED = FALSE OR EXTRACTED IS NULL;

-- Extraction errors
SELECT file_name, extraction_error, staged_at
FROM RAW_DOCUMENTS
WHERE EXTRACTION_ERROR IS NOT NULL
ORDER BY staged_at DESC;

-- Breakdown by doc type
SELECT doc_type, COUNT(*) AS doc_count,
       SUM(CASE WHEN EXTRACTED THEN 1 ELSE 0 END) AS extracted_count
FROM RAW_DOCUMENTS
GROUP BY doc_type;
```

### Extraction Timing

```sql
-- Average extraction time per doc type
SELECT r.doc_type,
       COUNT(*) AS cnt,
       AVG(DATEDIFF('second', r.staged_at, e.extracted_at)) AS avg_seconds
FROM RAW_DOCUMENTS r
JOIN EXTRACTED_FIELDS e ON r.file_name = e.file_name
GROUP BY r.doc_type;
```

---

## RBAC and Permissions

The POC uses a dedicated `AI_EXTRACT_APP` role for all operations. ACCOUNTADMIN
is only needed for initial setup (role creation, Cortex grants).

### Role Hierarchy

```
ACCOUNTADMIN
  └── SYSADMIN (owns all objects)
        └── AI_EXTRACT_APP (operational role — SELECT, INSERT, CALL)
```

### Granting Access to Additional Users

```sql
-- Grant the operational role to a user
USE ROLE ACCOUNTADMIN;
GRANT ROLE AI_EXTRACT_APP TO USER <username>;

-- For read-only access, create a viewer role
CREATE ROLE IF NOT EXISTS AI_EXTRACT_VIEWER;
GRANT USAGE ON DATABASE AI_EXTRACT_POC TO ROLE AI_EXTRACT_VIEWER;
GRANT USAGE ON SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS
  TO ROLE AI_EXTRACT_VIEWER;
GRANT SELECT ON ALL VIEWS IN SCHEMA AI_EXTRACT_POC.DOCUMENTS
  TO ROLE AI_EXTRACT_VIEWER;
GRANT USAGE ON WAREHOUSE AI_EXTRACT_WH TO ROLE AI_EXTRACT_VIEWER;
```

---

## Updating Extraction Prompts

To improve extraction quality, update the prompts in `DOCUMENT_TYPE_CONFIG`:

```sql
UPDATE DOCUMENT_TYPE_CONFIG
SET entity_prompt = 'Your improved prompt here...'
WHERE doc_type = 'INVOICE';
```

**Tips for good prompts:**
- Be explicit about field names and expected formats (e.g., "date in YYYY-MM-DD format")
- Include per-field descriptions (e.g., "vendor_name: the company that issued the invoice")
- For numeric fields, specify "as a number without currency symbols"
- For table data, describe each column clearly
- Test with 3–5 documents before running a full batch

After updating a prompt, re-extract affected documents:

```sql
-- Mark documents as unextracted
UPDATE RAW_DOCUMENTS SET EXTRACTED = FALSE
WHERE doc_type = 'INVOICE';

-- Re-run extraction
CALL SP_EXTRACT_BY_DOC_TYPE('INVOICE');
```

---

## Running Tests

### Full Test Suite (requires Snowflake connection)

```bash
cd poc
POC_CONNECTION=<your_connection> POC_DB=AI_EXTRACT_POC POC_SCHEMA=DOCUMENTS \
  POC_WH=AI_EXTRACT_WH POC_ROLE=AI_EXTRACT_APP \
  .venv/bin/python3 -m pytest tests/ --ignore=tests/test_e2e --timeout=60 -q
```

### Unit Tests Only (no Snowflake needed)

```bash
cd poc
.venv/bin/python3 -m pytest \
  tests/test_normalize_unit.py \
  tests/test_review_helpers.py \
  tests/test_config_helpers.py \
  tests/test_admin_builder.py \
  -v
```

### Specific Test Categories

```bash
# Deployment readiness
pytest tests/test_deployment_readiness.py -v --timeout=60

# RBAC permissions
pytest tests/test_rbac_permissions.py -v --timeout=60

# Document type extraction quality
pytest tests/test_contract_extraction.py -v --timeout=60
pytest tests/test_receipt_extraction.py -v --timeout=60
pytest tests/test_utility_bill_extraction.py -v --timeout=60
```

---

## Performance Tuning

### Warehouse Sizing

| Document Count | Recommended WH Size | Expected Extraction Time |
|----------------|---------------------|--------------------------|
| 1–50 | X-SMALL | ~1–5 min |
| 50–200 | SMALL | ~5–15 min |
| 200–1000 | MEDIUM | ~15–60 min |
| 1000+ | LARGE | Varies |

Extraction is the bottleneck — it calls `AI_EXTRACT()` per document. Read queries
(views, dashboards) are fast even on X-SMALL.

### Running the Benchmark

```bash
cd poc

# Baseline (current data)
POC_CONNECTION=<conn> .venv/bin/python3 benchmark_extraction.py

# Generate 370 more invoices, benchmark, then clean up
POC_CONNECTION=<conn> .venv/bin/python3 benchmark_extraction.py --generate 370 --cleanup
```

### Auto-Suspend

The warehouse is configured with `AUTO_SUSPEND = 60` (1 minute). This is
appropriate for interactive use. For batch-heavy workloads, consider increasing
to 300 seconds to avoid cold-start overhead.

---

## Backup and Recovery

### Extraction Data

All extraction results are stored in standard Snowflake tables with Time Travel
enabled (default 1 day on Standard, up to 90 days on Enterprise).

```sql
-- Recover accidentally deleted rows (within retention period)
INSERT INTO EXTRACTED_FIELDS
SELECT * FROM EXTRACTED_FIELDS AT(OFFSET => -3600)
WHERE record_id NOT IN (SELECT record_id FROM EXTRACTED_FIELDS);
```

### Review Audit Trail

`INVOICE_REVIEW` is append-only — corrections are never overwritten. The view
`V_INVOICE_SUMMARY` always shows the latest review per record via
`ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY reviewed_at DESC)`.

---

## Troubleshooting

### Documents Not Getting Extracted

1. **Is the task running?**
   ```sql
   SHOW TASKS LIKE 'EXTRACT_NEW%' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS;
   -- Check STATE column — should be 'started'
   ```

2. **Are documents registered?**
   ```sql
   SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE EXTRACTED = FALSE;
   ```

3. **Check for extraction errors:**
   ```sql
   SELECT file_name, extraction_error FROM RAW_DOCUMENTS
   WHERE EXTRACTION_ERROR IS NOT NULL;
   ```

4. **Is the warehouse active?**
   ```sql
   ALTER WAREHOUSE AI_EXTRACT_WH RESUME IF SUSPENDED;
   ```

### AI_EXTRACT Returns Poor Results

- Check the prompt in `DOCUMENT_TYPE_CONFIG` — be more specific about field names and formats
- Verify the PDF is readable (not a scanned image without OCR)
- Try extracting a single document manually to debug:
  ```sql
  SELECT AI_EXTRACT(
      BUILD_SCOPED_FILE_URL(@DOCUMENT_STAGE, 'invoices/test.pdf'),
      'Extract the invoice number, vendor name, and total amount. Return as JSON.'
  );
  ```

### Duplicate Rows in EXTRACTED_FIELDS

This usually happens when the background task runs during a manual upload/extract cycle.

**Fix:**
```sql
-- Find duplicates
SELECT file_name, COUNT(*) FROM EXTRACTED_FIELDS
GROUP BY file_name HAVING COUNT(*) > 1;

-- Remove duplicates (keep the latest)
DELETE FROM EXTRACTED_FIELDS
WHERE record_id NOT IN (
    SELECT MAX(record_id) FROM EXTRACTED_FIELDS GROUP BY file_name
);
```

**Prevention:** Always suspend the task before manual operations.

### Permission Errors

```sql
-- Check current role
SELECT CURRENT_ROLE();

-- Switch to operational role
USE ROLE AI_EXTRACT_APP;

-- Verify grants
SHOW GRANTS TO ROLE AI_EXTRACT_APP;
```

---

## Common SQL Recipes

### Export Extraction Results to CSV

```sql
COPY INTO @DOCUMENT_STAGE/exports/extracted_fields.csv
FROM (SELECT * FROM EXTRACTED_FIELDS)
FILE_FORMAT = (TYPE = CSV HEADER = TRUE)
OVERWRITE = TRUE
SINGLE = TRUE;
```

### Re-Extract a Single Document

```sql
UPDATE RAW_DOCUMENTS SET EXTRACTED = FALSE
WHERE file_name = 'invoices/invoice_042.pdf';

CALL SP_EXTRACT_BY_DOC_TYPE('INVOICE');
```

### Count Line Items Per Document

```sql
SELECT file_name, COUNT(*) AS line_items
FROM EXTRACTED_TABLE_DATA
GROUP BY file_name
ORDER BY line_items DESC;
```

### View All Active Document Type Configs

```sql
SELECT doc_type, display_name, is_active,
       field_1_label, field_2_label, field_3_label,
       col_1_label, col_2_label, col_3_label
FROM DOCUMENT_TYPE_CONFIG
WHERE is_active = TRUE;
```

### Teardown (Complete Removal)

```sql
-- WARNING: This drops everything
USE ROLE SYSADMIN;
DROP DATABASE IF EXISTS AI_EXTRACT_POC;
DROP WAREHOUSE IF EXISTS AI_EXTRACT_WH;
USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS AI_EXTRACT_APP;
```
