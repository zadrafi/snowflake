-- =============================================================================
-- 09_line_item_review.sql — Writeback Table + View for Line Item Corrections
--
-- Creates:
--   1. LINE_ITEM_REVIEW     — Append-only audit table for line item edits
--   2. V_LINE_ITEM_DETAIL   — View overlaying corrections on original data
--
-- Pattern mirrors INVOICE_REVIEW / V_DOCUMENT_SUMMARY exactly:
--   - Never UPDATE/MERGE — every edit INSERTs a new row (full traceability)
--   - View picks latest correction per line_id via ROW_NUMBER()
--   - COALESCE(correction, original) for each column
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- LINE_ITEM_REVIEW: Append-only audit trail for line item corrections
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS LINE_ITEM_REVIEW (
    review_id         NUMBER AUTOINCREMENT PRIMARY KEY,
    line_id           NUMBER NOT NULL,
    file_name         VARCHAR NOT NULL,
    record_id         VARCHAR,
    corrected_col_1   VARCHAR,
    corrected_col_2   VARCHAR,
    corrected_col_3   NUMBER(10,2),
    corrected_col_4   NUMBER(10,2),
    corrected_col_5   NUMBER(12,2),
    corrections       VARIANT,
    reviewer_notes    VARCHAR,
    reviewed_by       VARCHAR DEFAULT CURRENT_USER(),
    reviewed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- V_LINE_ITEM_DETAIL: View — corrected line items (instant, no lag)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW V_LINE_ITEM_DETAIL AS
SELECT
    td.line_id,
    td.file_name,
    td.record_id,
    td.line_number,

    COALESCE(rv.corrections:col_1::VARCHAR,                                    rv.corrected_col_1, td.col_1) AS description,
    COALESCE(rv.corrections:col_2::VARCHAR,                                    rv.corrected_col_2, td.col_2) AS category,
    COALESCE(TRY_TO_NUMBER(rv.corrections:col_3::VARCHAR, 10, 2),              rv.corrected_col_3, td.col_3) AS quantity,
    COALESCE(TRY_TO_NUMBER(rv.corrections:col_4::VARCHAR, 10, 2),              rv.corrected_col_4, td.col_4) AS unit_price,
    COALESCE(TRY_TO_NUMBER(rv.corrections:col_5::VARCHAR, 12, 2),              rv.corrected_col_5, td.col_5) AS line_total,

    td.raw_line_data,

    rv.review_id      AS last_review_id,
    rv.reviewer_notes,
    rv.reviewed_by,
    rv.reviewed_at,
    rv.corrections

FROM EXTRACTED_TABLE_DATA td

LEFT JOIN (
    SELECT *
    FROM LINE_ITEM_REVIEW
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY line_id ORDER BY reviewed_at DESC
    ) = 1
) rv
    ON td.line_id = rv.line_id;

-- Verify
SELECT 'V_LINE_ITEM_DETAIL' AS object_name, COUNT(*) AS row_count
FROM V_LINE_ITEM_DETAIL;
