-- =============================================================================
-- 06_automate.sql — Stream + Task for Automated Extraction (OPTIONAL)
--
-- Set this up AFTER you've validated batch extraction works in 04.
--
-- This creates:
--   1. A Stream on RAW_DOCUMENTS to detect new files
--   2. A Stored Procedure that extracts unprocessed files
--   3. A Task that runs every 5 minutes (only when new data exists)
--
-- The result: drop a new file on the stage, and it's automatically
-- extracted within 5 minutes — no manual intervention.
-- =============================================================================

USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- Stream: Detect new rows inserted into RAW_DOCUMENTS
-- ---------------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS RAW_DOCUMENTS_STREAM
    ON TABLE RAW_DOCUMENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Detects newly staged documents for automated extraction';

-- ---------------------------------------------------------------------------
-- Stored Procedure: Extract data from unprocessed documents
-- ---------------------------------------------------------------------------
-- This wraps the same logic from 04_batch_extract.sql into a callable proc.
-- CUSTOMIZE: Copy your validated prompts from 04_batch_extract.sql here.

CREATE OR REPLACE PROCEDURE SP_EXTRACT_NEW_DOCUMENTS()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    LET files_processed INT := 0;

    -- Entity extraction for unprocessed files
    INSERT INTO EXTRACTED_FIELDS (
        file_name, field_1, field_2, field_3, field_4, field_5,
        field_6, field_7, field_8, field_9, field_10
    )
    SELECT
        r.file_name,
        ext.extraction:response:vendor_name::VARCHAR,
        ext.extraction:response:document_number::VARCHAR,
        ext.extraction:response:reference::VARCHAR,
        TRY_TO_DATE(ext.extraction:response:document_date::VARCHAR),
        TRY_TO_DATE(ext.extraction:response:due_date::VARCHAR),
        ext.extraction:response:terms::VARCHAR,
        ext.extraction:response:recipient::VARCHAR,
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:subtotal::VARCHAR, '[^0-9.]', ''), 12, 2),
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:tax::VARCHAR, '[^0-9.]', ''), 12, 2),
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:total::VARCHAR, '[^0-9.]', ''), 12, 2)
    FROM RAW_DOCUMENTS r,
        LATERAL (
            SELECT
                AI_EXTRACT(
                    TO_FILE('@DOCUMENT_STAGE', r.file_name),
                    {
                        'vendor_name':    'What is the vendor or company name on this document?',
                        'document_number':'What is the invoice number or document ID?',
                        'reference':      'What is the PO number, reference number, or order number?',
                        'document_date':  'What is the document date or invoice date? Return in YYYY-MM-DD format.',
                        'due_date':       'What is the due date or expiration date? Return in YYYY-MM-DD format.',
                        'terms':          'What are the payment terms or contract terms (e.g., Net 30)?',
                        'recipient':      'Who is this document addressed to? Return name and address.',
                        'subtotal':       'What is the subtotal amount before tax? Return as a number only.',
                        'tax':            'What is the tax amount? Return as a number only.',
                        'total':          'What is the total amount? Return as a number only.'
                    }
                ) AS extraction
        ) AS ext
    WHERE r.extracted = FALSE
      AND r.file_name NOT IN (SELECT file_name FROM EXTRACTED_FIELDS);

    -- Table extraction for unprocessed files
    INSERT INTO EXTRACTED_TABLE_DATA (
        file_name, record_id, line_number, col_1, col_2, col_3, col_4, col_5
    )
    WITH extracted AS (
        SELECT
            r.file_name,
            ef.field_2 AS record_id,
            AI_EXTRACT(
                file => TO_FILE('@DOCUMENT_STAGE', r.file_name),
                responseFormat => {
                    'schema': {
                        'type': 'object',
                        'properties': {
                            'line_items': {
                                'description': 'The table of line items on the document',
                                'type': 'object',
                                'column_ordering': ['Line', 'Description', 'Category', 'Qty', 'Unit Price', 'Total'],
                                'properties': {
                                    'Line':       { 'description': 'Line item number',          'type': 'array' },
                                    'Description':{ 'description': 'Product or service name',   'type': 'array' },
                                    'Category':   { 'description': 'Product category or type',  'type': 'array' },
                                    'Qty':        { 'description': 'Quantity',                  'type': 'array' },
                                    'Unit Price': { 'description': 'Price per unit in dollars', 'type': 'array' },
                                    'Total':      { 'description': 'Line total in dollars',     'type': 'array' }
                                }
                            }
                        }
                    }
                }
            ) AS extraction
        FROM RAW_DOCUMENTS r
            JOIN EXTRACTED_FIELDS ef ON r.file_name = ef.file_name
        WHERE r.extracted = FALSE
          AND r.file_name NOT IN (SELECT DISTINCT file_name FROM EXTRACTED_TABLE_DATA)
    )
    SELECT
        e.file_name,
        e.record_id,
        TRY_TO_NUMBER(ln.value::VARCHAR)                                            AS line_number,
        pr.value::VARCHAR                                                           AS col_1,
        ca.value::VARCHAR                                                           AS col_2,
        TRY_TO_NUMBER(REGEXP_REPLACE(qt.value::VARCHAR, '[^0-9.]', ''), 10, 2)     AS col_3,
        TRY_TO_NUMBER(REGEXP_REPLACE(up.value::VARCHAR, '[^0-9.]', ''), 10, 2)     AS col_4,
        TRY_TO_NUMBER(REGEXP_REPLACE(tl.value::VARCHAR, '[^0-9.]', ''), 12, 2)     AS col_5
    FROM extracted e,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Line)             ln,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Description)      pr,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Category)         ca,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Qty)              qt,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:"Unit Price")     up,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Total)            tl
    WHERE ln.index = pr.index
      AND ln.index = ca.index
      AND ln.index = qt.index
      AND ln.index = up.index
      AND ln.index = tl.index;

    -- Mark files as extracted
    SELECT COUNT(*) INTO :files_processed
    FROM RAW_DOCUMENTS WHERE extracted = FALSE
      AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS);

    UPDATE RAW_DOCUMENTS
    SET extracted = TRUE,
        extracted_at = CURRENT_TIMESTAMP()
    WHERE extracted = FALSE
      AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS);

    RETURN 'Processed ' || :files_processed || ' new document(s)';
END;
$$;

-- ---------------------------------------------------------------------------
-- Task: Run extraction every 5 minutes when new documents exist
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK EXTRACT_NEW_DOCUMENTS_TASK
    WAREHOUSE = AI_EXTRACT_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Auto-extract newly staged documents using AI_EXTRACT'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_DOCUMENTS_STREAM')
AS
    CALL SP_EXTRACT_NEW_DOCUMENTS();

-- Enable the task (tasks are created in SUSPENDED state by default)
ALTER TASK EXTRACT_NEW_DOCUMENTS_TASK RESUME;

-- ---------------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------------
SHOW TASKS LIKE 'EXTRACT_NEW_DOCUMENTS_TASK';

-- To test the task manually (without waiting 5 minutes):
-- EXECUTE TASK EXTRACT_NEW_DOCUMENTS_TASK;

-- To pause the task:
-- ALTER TASK EXTRACT_NEW_DOCUMENTS_TASK SUSPEND;
