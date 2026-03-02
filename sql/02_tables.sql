-- =============================================================================
-- 02_tables.sql — Tables for raw invoices, extraction results, and AP ledger
-- =============================================================================

USE DATABASE AP_DEMO_DB;
USE SCHEMA AP;

-- ---------------------------------------------------------------------------
-- RAW_INVOICES: Tracks every PDF file staged for processing
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_INVOICES (
    file_name       VARCHAR NOT NULL,
    file_path       VARCHAR NOT NULL,          -- full stage path: @INVOICE_STAGE/invoice_001.pdf
    staged_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    extracted       BOOLEAN DEFAULT FALSE,
    extracted_at    TIMESTAMP_NTZ,
    extraction_error VARCHAR,
    CONSTRAINT pk_raw_invoices PRIMARY KEY (file_name)
);

-- ---------------------------------------------------------------------------
-- EXTRACTED_INVOICES: Header-level data extracted by AI_EXTRACT
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EXTRACTED_INVOICES (
    invoice_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    file_name           VARCHAR NOT NULL,
    vendor_name         VARCHAR,
    invoice_number      VARCHAR,
    po_number           VARCHAR,
    invoice_date        DATE,
    due_date            DATE,
    payment_terms       VARCHAR,
    bill_to             VARCHAR,
    subtotal            NUMBER(12,2),
    tax_amount          NUMBER(12,2),
    total_amount        NUMBER(12,2),
    status              VARCHAR DEFAULT 'PENDING',   -- PENDING, APPROVED, PAID
    payment_date        DATE,
    extracted_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT fk_raw FOREIGN KEY (file_name) REFERENCES RAW_INVOICES(file_name)
);

-- ---------------------------------------------------------------------------
-- EXTRACTED_LINE_ITEMS: Line-level data extracted by AI_EXTRACT (table mode)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EXTRACTED_LINE_ITEMS (
    line_item_id    NUMBER AUTOINCREMENT PRIMARY KEY,
    file_name       VARCHAR NOT NULL,
    invoice_number  VARCHAR,
    line_number     NUMBER,
    product_name    VARCHAR,
    category        VARCHAR,
    quantity        NUMBER(10,2),
    unit_price      NUMBER(10,2),
    line_total      NUMBER(12,2),
    CONSTRAINT fk_line_raw FOREIGN KEY (file_name) REFERENCES RAW_INVOICES(file_name)
);

-- ---------------------------------------------------------------------------
-- VENDORS: Reference table for vendor normalization
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS VENDORS (
    vendor_id       NUMBER AUTOINCREMENT PRIMARY KEY,
    vendor_name     VARCHAR NOT NULL,
    address         VARCHAR,
    default_terms   VARCHAR,
    CONSTRAINT uq_vendor_name UNIQUE (vendor_name)
);

-- Seed vendor reference data
INSERT INTO VENDORS (vendor_name, address, default_terms)
SELECT column1, column2, column3
FROM VALUES
    ('McLane Company, Inc.',       '4747 McLane Parkway, Temple, TX 76504',                   'Net 15'),
    ('Core-Mark International',     '395 Oyster Point Blvd, South San Francisco, CA 94080',   'Net 30'),
    ('S&P Company',                 '1600 Distribution Dr, Duluth, GA 30097',                 'Net 15'),
    ('Coca-Cola Bottling Co.',      'One Coca-Cola Plaza, Atlanta, GA 30313',                 'Net 30'),
    ('PepsiCo / Frito-Lay',        '7701 Legacy Drive, Plano, TX 75024',                     'Net 30'),
    ('Anheuser-Busch InBev',       'One Busch Place, St. Louis, MO 63118',                   'Net 30'),
    ('Altria Group / Philip Morris','6601 W Broad Street, Richmond, VA 23230',                'Net 15'),
    ('Keurig Dr Pepper',           '53 South Avenue, Burlington, MA 01803',                   'Net 30'),
    ('Mars Wrigley Confectionery',  '800 High Street, Hackettstown, NJ 07840',               'Net 30'),
    ('Mondelez International',      '905 W Fulton Market, Chicago, IL 60607',                'Net 60'),
    ('Red Bull Distribution',       '1740 Stewart Street, Santa Monica, CA 90404',           'Net 15'),
    ('Monster Beverage Corp.',      '1 Monster Way, Corona, CA 92879',                       'Net 30')
WHERE NOT EXISTS (SELECT 1 FROM VENDORS);
