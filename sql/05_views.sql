-- =============================================================================
-- 05_views.sql — Analytical views for the Streamlit app
-- =============================================================================

USE DATABASE AP_DEMO_DB;
USE SCHEMA AP;

-- ---------------------------------------------------------------------------
-- V_AP_LEDGER: Enriched invoice view with aging buckets
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AP_LEDGER AS
SELECT
    ei.invoice_id,
    ei.file_name,
    ei.vendor_name,
    ei.invoice_number,
    ei.po_number,
    ei.invoice_date,
    ei.due_date,
    ei.payment_terms,
    ei.bill_to,
    ei.subtotal,
    ei.tax_amount,
    ei.total_amount,
    ei.status,
    ei.payment_date,
    ei.extracted_at,
    -- Aging calculation (days past due)
    CASE
        WHEN ei.status = 'PAID' THEN 0
        ELSE GREATEST(DATEDIFF(day, ei.due_date, CURRENT_DATE()), 0)
    END AS days_past_due,
    -- Aging bucket
    CASE
        WHEN ei.status = 'PAID' THEN 'Paid'
        WHEN CURRENT_DATE() <= ei.due_date THEN 'Current'
        WHEN DATEDIFF(day, ei.due_date, CURRENT_DATE()) BETWEEN 1 AND 30 THEN '1-30 Days'
        WHEN DATEDIFF(day, ei.due_date, CURRENT_DATE()) BETWEEN 31 AND 60 THEN '31-60 Days'
        WHEN DATEDIFF(day, ei.due_date, CURRENT_DATE()) BETWEEN 61 AND 90 THEN '61-90 Days'
        ELSE '90+ Days'
    END AS aging_bucket,
    -- Outstanding amount (unpaid only)
    CASE
        WHEN ei.status = 'PAID' THEN 0
        ELSE ei.total_amount
    END AS outstanding_amount
FROM EXTRACTED_INVOICES ei;

-- ---------------------------------------------------------------------------
-- V_AGING_SUMMARY: Aggregate aging by bucket
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AGING_SUMMARY AS
SELECT
    aging_bucket,
    COUNT(*) AS invoice_count,
    SUM(outstanding_amount) AS total_outstanding,
    -- Sort order for display
    CASE aging_bucket
        WHEN 'Current'    THEN 1
        WHEN '1-30 Days'  THEN 2
        WHEN '31-60 Days' THEN 3
        WHEN '61-90 Days' THEN 4
        WHEN '90+ Days'   THEN 5
        WHEN 'Paid'       THEN 6
    END AS sort_order
FROM V_AP_LEDGER
GROUP BY aging_bucket
ORDER BY sort_order;

-- ---------------------------------------------------------------------------
-- V_SPEND_BY_VENDOR: Total spend by vendor
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SPEND_BY_VENDOR AS
SELECT
    vendor_name,
    COUNT(*) AS invoice_count,
    SUM(total_amount) AS total_spend,
    AVG(total_amount) AS avg_invoice_amount,
    MIN(invoice_date) AS first_invoice,
    MAX(invoice_date) AS last_invoice
FROM EXTRACTED_INVOICES
GROUP BY vendor_name
ORDER BY total_spend DESC;

-- ---------------------------------------------------------------------------
-- V_SPEND_BY_CATEGORY: Spend breakdown by product category
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SPEND_BY_CATEGORY AS
SELECT
    li.category,
    COUNT(DISTINCT li.file_name) AS invoice_count,
    SUM(li.line_total) AS total_spend,
    SUM(li.quantity) AS total_units,
    AVG(li.unit_price) AS avg_unit_price
FROM EXTRACTED_LINE_ITEMS li
WHERE li.category IS NOT NULL
GROUP BY li.category
ORDER BY total_spend DESC;

-- ---------------------------------------------------------------------------
-- V_MONTHLY_TREND: Monthly spend trend
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_MONTHLY_TREND AS
SELECT
    DATE_TRUNC('month', invoice_date) AS month,
    COUNT(*) AS invoice_count,
    SUM(total_amount) AS total_spend,
    SUM(subtotal) AS subtotal,
    SUM(tax_amount) AS total_tax,
    AVG(total_amount) AS avg_invoice_amount
FROM EXTRACTED_INVOICES
WHERE invoice_date IS NOT NULL
GROUP BY DATE_TRUNC('month', invoice_date)
ORDER BY month;

-- ---------------------------------------------------------------------------
-- V_TOP_LINE_ITEMS: Top products by total spend
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_TOP_LINE_ITEMS AS
SELECT
    product_name,
    category,
    COUNT(*) AS appearance_count,
    SUM(quantity) AS total_quantity,
    AVG(unit_price) AS avg_unit_price,
    SUM(line_total) AS total_spend
FROM EXTRACTED_LINE_ITEMS
WHERE product_name IS NOT NULL
GROUP BY product_name, category
ORDER BY total_spend DESC;

-- ---------------------------------------------------------------------------
-- V_VENDOR_PAYMENT_TERMS: Payment terms summary by vendor
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_VENDOR_PAYMENT_TERMS AS
SELECT
    vendor_name,
    payment_terms,
    COUNT(*) AS invoice_count,
    SUM(total_amount) AS total_spend,
    SUM(CASE WHEN status = 'PAID' THEN total_amount ELSE 0 END) AS paid_amount,
    SUM(CASE WHEN status != 'PAID' THEN total_amount ELSE 0 END) AS outstanding_amount
FROM EXTRACTED_INVOICES
GROUP BY vendor_name, payment_terms
ORDER BY total_spend DESC;

-- ---------------------------------------------------------------------------
-- V_EXTRACTION_STATUS: Pipeline monitoring view
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_EXTRACTION_STATUS AS
SELECT
    COUNT(*) AS total_files,
    SUM(CASE WHEN extracted = TRUE THEN 1 ELSE 0 END) AS extracted_files,
    SUM(CASE WHEN extracted = FALSE THEN 1 ELSE 0 END) AS pending_files,
    SUM(CASE WHEN extraction_error IS NOT NULL THEN 1 ELSE 0 END) AS failed_files,
    MAX(extracted_at) AS last_extraction
FROM RAW_INVOICES;

-- Grant select on new views
GRANT SELECT ON ALL VIEWS IN SCHEMA AP_DEMO_DB.AP TO ROLE PUBLIC;
