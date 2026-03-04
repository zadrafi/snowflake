-- =============================================================================
-- 02_tables.sql — Document Tracking and Extraction Results Tables
--
-- These tables store:
--   1. File metadata (what's been staged and processed)
--   2. Entity extraction results (header-level fields from each document)
--   3. Table extraction results (line items / tabular data from each document)
--
-- CUSTOMIZE: Rename columns in EXTRACTED_FIELDS and EXTRACTED_TABLE_DATA
-- to match YOUR document type. See comments for examples.
-- =============================================================================

USE DATABASE AI_EXTRACT_POC;      -- <-- match your 01_setup.sql values
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- RAW_DOCUMENTS: Tracks every file staged for processing
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_DOCUMENTS (
    file_name         VARCHAR NOT NULL,
    file_path         VARCHAR NOT NULL,
    staged_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    extracted         BOOLEAN DEFAULT FALSE,
    extracted_at      TIMESTAMP_NTZ,
    extraction_error  VARCHAR,
    CONSTRAINT pk_raw_documents PRIMARY KEY (file_name)
);

-- ---------------------------------------------------------------------------
-- EXTRACTED_FIELDS: Entity-level data pulled from each document
-- ---------------------------------------------------------------------------
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  CUSTOMIZE THESE COLUMNS for your document type.                       │
-- │                                                                        │
-- │  Invoice example (default):                                            │
-- │    vendor_name, invoice_number, invoice_date, due_date, total_amount   │
-- │                                                                        │
-- │  Contract example:                                                     │
-- │    party_a, party_b, effective_date, expiration_date, contract_value   │
-- │                                                                        │
-- │  Receipt example:                                                      │
-- │    store_name, receipt_number, transaction_date, total, payment_method │
-- │                                                                        │
-- │  Medical claim example:                                                │
-- │    patient_name, provider, service_date, diagnosis_code, billed_amount│
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS EXTRACTED_FIELDS (
    record_id         NUMBER AUTOINCREMENT PRIMARY KEY,
    file_name         VARCHAR NOT NULL,

    -- Document header fields — RENAME THESE to match your document type
    field_1           VARCHAR,       -- e.g., vendor_name / party_a / store_name
    field_2           VARCHAR,       -- e.g., invoice_number / contract_id / receipt_number
    field_3           VARCHAR,       -- e.g., po_number / reference_number
    field_4           DATE,          -- e.g., document_date / invoice_date / effective_date
    field_5           DATE,          -- e.g., due_date / expiration_date
    field_6           VARCHAR,       -- e.g., payment_terms / contract_type
    field_7           VARCHAR,       -- e.g., bill_to / ship_to / recipient
    field_8           NUMBER(12,2),  -- e.g., subtotal / base_amount
    field_9           NUMBER(12,2),  -- e.g., tax_amount / discount
    field_10          NUMBER(12,2),  -- e.g., total_amount / contract_value

    -- Metadata
    status            VARCHAR DEFAULT 'EXTRACTED',
    extracted_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT fk_raw FOREIGN KEY (file_name) REFERENCES RAW_DOCUMENTS(file_name)
);

-- ---------------------------------------------------------------------------
-- EXTRACTED_TABLE_DATA: Line items / tabular data from each document
-- ---------------------------------------------------------------------------
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  CUSTOMIZE THESE COLUMNS for the tables inside your documents.         │
-- │                                                                        │
-- │  Invoice line items (default):                                         │
-- │    product_name, category, quantity, unit_price, line_total            │
-- │                                                                        │
-- │  Contract schedule of payments:                                        │
-- │    milestone, due_date, amount, status                                 │
-- │                                                                        │
-- │  Medical claim line items:                                             │
-- │    procedure_code, description, quantity, charge, allowed_amount       │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS EXTRACTED_TABLE_DATA (
    line_id           NUMBER AUTOINCREMENT PRIMARY KEY,
    file_name         VARCHAR NOT NULL,
    record_id         VARCHAR,       -- Links to parent document (e.g., invoice_number)
    line_number       NUMBER,

    -- Table columns — RENAME THESE to match your document's tabular data
    col_1             VARCHAR,       -- e.g., product_name / procedure_code / milestone
    col_2             VARCHAR,       -- e.g., category / description
    col_3             NUMBER(10,2),  -- e.g., quantity
    col_4             NUMBER(10,2),  -- e.g., unit_price / charge
    col_5             NUMBER(12,2),  -- e.g., line_total / amount

    CONSTRAINT fk_table_raw FOREIGN KEY (file_name) REFERENCES RAW_DOCUMENTS(file_name)
);

-- ---------------------------------------------------------------------------
-- Register all staged files into RAW_DOCUMENTS
-- ---------------------------------------------------------------------------
-- Run this AFTER uploading your documents to the stage.
-- Safe to re-run — skips files already registered.

INSERT INTO RAW_DOCUMENTS (file_name, file_path, staged_at, extracted)
SELECT
    RELATIVE_PATH                              AS file_name,
    '@DOCUMENT_STAGE/' || RELATIVE_PATH        AS file_path,
    CURRENT_TIMESTAMP()                        AS staged_at,
    FALSE                                      AS extracted
FROM DIRECTORY(@DOCUMENT_STAGE)
WHERE RELATIVE_PATH LIKE '%.pdf'         -- <-- adjust file extension filter as needed
  AND RELATIVE_PATH NOT IN (SELECT file_name FROM RAW_DOCUMENTS);

-- Verify registered files
SELECT * FROM RAW_DOCUMENTS ORDER BY staged_at DESC;
