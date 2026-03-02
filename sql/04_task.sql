-- =============================================================================
-- 04_task.sql — Stream + Stored Procedure + Scheduled Task for new files
--
-- This creates an automated pipeline:
--   1. Stream on RAW_INVOICES detects newly inserted rows
--   2. Stored procedure extracts data from unprocessed PDFs
--   3. Task runs every 5 minutes (only when stream has data)
--   4. Task can also be triggered on-demand via EXECUTE TASK
-- =============================================================================

USE DATABASE AP_DEMO_DB;
USE SCHEMA AP;
USE WAREHOUSE AP_DEMO_WH;

-- ---------------------------------------------------------------------------
-- Stream: Detects new inserts into RAW_INVOICES
-- ---------------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS RAW_INVOICES_STREAM
    ON TABLE RAW_INVOICES
    APPEND_ONLY = TRUE
    COMMENT = 'Detects newly staged invoice files for automated extraction';

-- ---------------------------------------------------------------------------
-- Stored Procedure: Extract data from unprocessed invoices
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_EXTRACT_NEW_INVOICES()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    LET files_processed INT := 0;

    -- Extract header fields from unprocessed invoices
    INSERT INTO EXTRACTED_INVOICES (
        file_name, vendor_name, invoice_number, po_number,
        invoice_date, due_date, payment_terms, bill_to,
        subtotal, tax_amount, total_amount, status
    )
    SELECT
        r.file_name,
        ext.extraction:response:vendor_name::VARCHAR,
        ext.extraction:response:invoice_number::VARCHAR,
        ext.extraction:response:po_number::VARCHAR,
        TRY_TO_DATE(ext.extraction:response:invoice_date::VARCHAR),
        TRY_TO_DATE(ext.extraction:response:due_date::VARCHAR),
        ext.extraction:response:payment_terms::VARCHAR,
        ext.extraction:response:bill_to::VARCHAR,
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:subtotal::VARCHAR, '[^0-9.]', ''), 12, 2),
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:tax_amount::VARCHAR, '[^0-9.]', ''), 12, 2),
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:total_amount::VARCHAR, '[^0-9.]', ''), 12, 2),
        'PENDING'
    FROM RAW_INVOICES r,
        LATERAL (
            SELECT
                AI_EXTRACT(
                    TO_FILE('@AP_DEMO_DB.AP.INVOICE_STAGE', r.file_name),
                    {
                        'vendor_name':    'What is the vendor or company name on this invoice?',
                        'invoice_number': 'What is the invoice number?',
                        'po_number':      'What is the PO number or purchase order number?',
                        'invoice_date':   'What is the invoice date? Return in YYYY-MM-DD format.',
                        'due_date':       'What is the due date or payment due date? Return in YYYY-MM-DD format.',
                        'payment_terms':  'What are the payment terms (e.g., Net 15, Net 30)?',
                        'bill_to':        'Who is the invoice billed to? Return the store name and address.',
                        'subtotal':       'What is the subtotal amount before tax? Return as a number only.',
                        'tax_amount':     'What is the tax amount? Return as a number only.',
                        'total_amount':   'What is the total amount due? Return as a number only.'
                    }
                ) AS extraction
        ) AS ext
    WHERE r.extracted = FALSE
      AND r.file_name NOT IN (SELECT file_name FROM EXTRACTED_INVOICES);

    -- Extract line items from unprocessed invoices (JSON schema table format)
    INSERT INTO EXTRACTED_LINE_ITEMS (
        file_name, invoice_number, line_number,
        product_name, category, quantity, unit_price, line_total
    )
    WITH extracted AS (
        SELECT
            r.file_name,
            ei.invoice_number,
            AI_EXTRACT(
                file => TO_FILE('@AP_DEMO_DB.AP.INVOICE_STAGE', r.file_name),
                responseFormat => {
                    'schema': {
                        'type': 'object',
                        'properties': {
                            'line_items': {
                                'description': 'The table of line items on the invoice',
                                'type': 'object',
                                'column_ordering': ['Line', 'Product', 'Category', 'Qty', 'Unit Price', 'Total'],
                                'properties': {
                                    'Line': { 'description': 'Line item number', 'type': 'array' },
                                    'Product': { 'description': 'Product name or description', 'type': 'array' },
                                    'Category': { 'description': 'Product category', 'type': 'array' },
                                    'Qty': { 'description': 'Quantity ordered', 'type': 'array' },
                                    'Unit Price': { 'description': 'Price per unit in dollars', 'type': 'array' },
                                    'Total': { 'description': 'Line total in dollars', 'type': 'array' }
                                }
                            }
                        }
                    }
                }
            ) AS extraction
        FROM RAW_INVOICES r
            JOIN EXTRACTED_INVOICES ei ON r.file_name = ei.file_name
        WHERE r.extracted = FALSE
          AND r.file_name NOT IN (SELECT DISTINCT file_name FROM EXTRACTED_LINE_ITEMS)
    )
    SELECT
        e.file_name,
        e.invoice_number,
        TRY_TO_NUMBER(ln.value::VARCHAR)          AS line_number,
        pr.value::VARCHAR                          AS product_name,
        ca.value::VARCHAR                          AS category,
        TRY_TO_NUMBER(REGEXP_REPLACE(qt.value::VARCHAR, '[^0-9.]', ''), 10, 2)   AS quantity,
        TRY_TO_NUMBER(REGEXP_REPLACE(up.value::VARCHAR, '[^0-9.]', ''), 10, 2)   AS unit_price,
        TRY_TO_NUMBER(REGEXP_REPLACE(tl.value::VARCHAR, '[^0-9.]', ''), 12, 2)   AS line_total
    FROM extracted e,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Line) ln,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Product) pr,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Category) ca,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Qty) qt,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:"Unit Price") up,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Total) tl
    WHERE ln.index = pr.index
      AND ln.index = ca.index
      AND ln.index = qt.index
      AND ln.index = up.index
      AND ln.index = tl.index;

    -- Mark files as extracted and count
    SELECT COUNT(*) INTO :files_processed
    FROM RAW_INVOICES WHERE extracted = FALSE
      AND file_name IN (SELECT file_name FROM EXTRACTED_INVOICES);

    UPDATE RAW_INVOICES
    SET extracted = TRUE,
        extracted_at = CURRENT_TIMESTAMP()
    WHERE extracted = FALSE
      AND file_name IN (SELECT file_name FROM EXTRACTED_INVOICES);

    RETURN 'Processed ' || :files_processed || ' new invoice(s)';
END;
$$;

-- ---------------------------------------------------------------------------
-- Task: Runs every 5 minutes, only when stream has data
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK EXTRACT_NEW_INVOICES_TASK
    WAREHOUSE = AP_DEMO_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Auto-extract newly staged PDF invoices using AI_EXTRACT'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_INVOICES_STREAM')
AS
    CALL SP_EXTRACT_NEW_INVOICES();

-- Enable the task
ALTER TASK EXTRACT_NEW_INVOICES_TASK RESUME;
