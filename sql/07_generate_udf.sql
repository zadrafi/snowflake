-- =============================================================================
-- 07_generate_udf.sql — Python UDTF to generate invoice PDFs + wrapper proc
-- =============================================================================
USE DATABASE AP_DEMO_DB;
USE SCHEMA AP;
USE WAREHOUSE AP_DEMO_WH;

-- ---------------------------------------------------------------------------
-- UDTF: Generate a single invoice PDF and return it as a file
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION GENERATE_INVOICE_PDF(
    vendor_name STRING,
    vendor_address STRING,
    vendor_terms STRING,
    num_items INT,
    categories STRING,
    approx_total FLOAT,
    invoice_seed INT
)
RETURNS TABLE (file STRING)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'fpdf')
HANDLER = 'InvoiceGenerator'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from fpdf import FPDF
import random
import math
from datetime import datetime, timedelta

PRODUCTS = {
    "Beverages": [
        ("Coca-Cola Classic 20oz", 1.05, 1.35),
        ("Pepsi 20oz", 1.00, 1.30),
        ("Red Bull 8.4oz", 2.10, 2.60),
        ("Monster Energy 16oz", 1.80, 2.20),
        ("Gatorade Fruit Punch 28oz", 1.25, 1.55),
        ("Dasani Water 20oz", 0.60, 0.85),
        ("Dr Pepper 20oz", 1.05, 1.35),
        ("Mountain Dew 20oz", 1.00, 1.30),
        ("Celsius Energy 12oz", 1.65, 2.00),
        ("Body Armor 28oz", 1.40, 1.75),
        ("AriZona Iced Tea 23oz", 0.75, 1.00),
        ("Tropicana OJ 15.2oz", 1.20, 1.50),
    ],
    "Snacks": [
        ("Doritos Nacho Cheese 2.75oz", 1.10, 1.45),
        ("Lay's Classic 2.625oz", 1.10, 1.40),
        ("Cheetos Crunchy 3.25oz", 1.15, 1.45),
        ("Pringles Original 2.5oz", 1.20, 1.50),
        ("Takis Fuego 4oz", 1.30, 1.60),
        ("Ruffles Cheddar 2.5oz", 1.10, 1.40),
        ("SunChips Harvest Cheddar 2.75oz", 1.15, 1.45),
        ("Fritos Original 3.5oz", 1.05, 1.35),
    ],
    "Candy & Gum": [
        ("Snickers Bar 1.86oz", 0.95, 1.20),
        ("M&M's Peanut 1.74oz", 0.95, 1.20),
        ("Reese's PB Cups 1.5oz", 0.95, 1.20),
        ("Skittles Original 2.17oz", 0.90, 1.15),
        ("Twix Bar 1.79oz", 0.95, 1.20),
        ("Sour Patch Kids 2oz", 0.85, 1.10),
        ("Trident Spearmint 14ct", 1.10, 1.40),
        ("Extra Polar Ice 15ct", 1.10, 1.40),
    ],
    "Tobacco": [
        ("Marlboro Red Kings Pack", 5.50, 6.80),
        ("Camel Blue Kings Pack", 5.30, 6.50),
        ("Newport Menthol Kings Pack", 5.60, 6.90),
        ("Grizzly Wintergreen Pouch", 3.80, 4.50),
        ("ZYN Cool Mint 6mg 15ct", 3.20, 3.80),
    ],
    "Dairy & Refrigerated": [
        ("Fairlife Whole Milk 14oz", 1.80, 2.20),
        ("Red Diamond Sweet Tea 1gal", 2.50, 3.10),
        ("Chobani Vanilla Greek Yogurt", 1.20, 1.50),
        ("Lunchables Turkey & Cheddar", 1.60, 2.00),
        ("Oscar Mayer P3 Protein Pack", 1.50, 1.85),
    ],
    "Frozen": [
        ("Hot Pockets Pepperoni Pizza", 1.40, 1.75),
        ("Tornados Ranchero Beef 3oz", 0.85, 1.10),
        ("Ben & Jerry's Half Baked Pint", 3.20, 3.90),
        ("Klondike Bar 6-pack", 3.50, 4.20),
    ],
    "General Merchandise": [
        ("Energizer AA 4-pack", 3.50, 4.20),
        ("BIC Classic Lighter 2-pack", 2.00, 2.50),
        ("5-Hour Energy Berry 2oz", 2.20, 2.70),
        ("Advil Ibuprofen 2ct", 0.90, 1.20),
        ("Chapstick Original", 1.50, 1.90),
    ],
}

CITIES = [
    ("Springfield", "IL"), ("Riverside", "CA"), ("Fairview", "OH"),
    ("Madison", "WI"), ("Georgetown", "TX"), ("Franklin", "TN"),
    ("Clinton", "NC"), ("Arlington", "VA"), ("Burlington", "CO"),
    ("Lakewood", "PA"),
]
STREETS = ["Main", "Highway", "Commerce", "Oak", "Elm", "Pine", "Maple", "Cedar", "Park", "Lake"]
SUFFIXES = ["St", "Rd", "Blvd", "Ave", "Dr"]

class InvoiceGenerator:
    def __init__(self):
        self.pdf = None

    def process(self, vendor_name, vendor_address, vendor_terms, num_items,
                categories, approx_total, invoice_seed):
        rng = random.Random(invoice_seed)

        # Parse categories
        cat_list = [c.strip() for c in categories.split(",") if c.strip() in PRODUCTS]
        if not cat_list:
            cat_list = list(PRODUCTS.keys())

        # Collect products from selected categories
        pool = []
        for cat in cat_list:
            for p in PRODUCTS[cat]:
                pool.append((cat, p[0], p[1], p[2]))

        # Pick line items
        n = min(num_items, len(pool))
        selected = rng.sample(pool, n)

        # Generate line items, scale quantities to hit approx_total
        line_items = []
        avg_price = sum((lo + hi) / 2 for _, _, lo, hi in selected) / len(selected)
        target_per_item = approx_total / (n * 1.08)  # rough pre-tax per item
        base_qty = max(6, int(target_per_item / avg_price))

        for cat, name, lo, hi in selected:
            price = round(rng.uniform(lo, hi), 2)
            qty = rng.choice([max(6, base_qty - 12), base_qty, base_qty + 12,
                              base_qty + 24])
            qty = max(6, qty)
            total = round(price * qty, 2)
            line_items.append((cat, name, qty, price, total))

        subtotal = round(sum(t[4] for t in line_items), 2)
        tax_rate = round(rng.uniform(0.06, 0.09), 4)
        tax = round(subtotal * tax_rate, 2)
        grand_total = round(subtotal + tax, 2)

        # Invoice metadata
        inv_num = f"INV-{invoice_seed:05d}"
        po_num = f"PO-{rng.randint(100000, 999999)}"
        inv_date = datetime.now() - timedelta(days=rng.randint(0, 7))
        terms_days = int(vendor_terms.replace("Net ", "").strip()) if "Net" in vendor_terms else 30
        due_date = inv_date + timedelta(days=terms_days)

        city, state = rng.choice(CITIES)
        store_num = rng.randint(100, 999)
        street_num = rng.randint(100, 9999)
        street = rng.choice(STREETS)
        suffix = rng.choice(SUFFIXES)
        zipcode = rng.randint(30000, 89999)

        # --- Build PDF ---
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)

        # Vendor header
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(26, 35, 126)
        pdf.cell(0, 10, vendor_name, ln=True)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(0, 0, 0)
        for line in vendor_address.split("\\n"):
            pdf.cell(0, 5, line.strip(), ln=True)

        pdf.ln(5)

        # INVOICE title
        pdf.set_font("Helvetica", "B", 22)
        pdf.set_text_color(26, 35, 126)
        pdf.cell(0, 12, "INVOICE", ln=True)
        pdf.set_text_color(0, 0, 0)
        pdf.ln(3)

        # Invoice details
        pdf.set_font("Helvetica", "", 10)
        details = [
            ("Invoice Number:", inv_num),
            ("PO Number:", po_num),
            ("Invoice Date:", inv_date.strftime("%B %d, %Y")),
            ("Due Date:", due_date.strftime("%B %d, %Y")),
            ("Payment Terms:", vendor_terms),
        ]
        for label, val in details:
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(45, 6, label)
            pdf.set_font("Helvetica", "", 10)
            pdf.cell(0, 6, val, ln=True)

        pdf.ln(4)

        # Bill To
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 6, "Bill To:", ln=True)
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 5, f"QuickStop Convenience Store #{store_num}", ln=True)
        pdf.cell(0, 5, f"{street_num} {street} {suffix}", ln=True)
        pdf.cell(0, 5, f"{city}, {state} {zipcode}", ln=True)

        pdf.ln(6)

        # Line items table header
        col_w = [10, 70, 35, 15, 25, 25]
        headers = ["#", "Product", "Category", "Qty", "Unit Price", "Total"]
        pdf.set_fill_color(26, 35, 126)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 8)
        for i, h in enumerate(headers):
            pdf.cell(col_w[i], 7, h, border=1, fill=True, align="C")
        pdf.ln()

        # Line items rows
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 8)
        for idx, (cat, name, qty, price, total) in enumerate(line_items, 1):
            if idx % 2 == 0:
                pdf.set_fill_color(245, 245, 245)
                fill = True
            else:
                fill = False
            pdf.cell(col_w[0], 6, str(idx), border=1, align="C", fill=fill)
            pdf.cell(col_w[1], 6, name[:35], border=1, fill=fill)
            pdf.cell(col_w[2], 6, cat[:18], border=1, fill=fill)
            pdf.cell(col_w[3], 6, str(qty), border=1, align="C", fill=fill)
            pdf.cell(col_w[4], 6, f"${price:.2f}", border=1, align="R", fill=fill)
            pdf.cell(col_w[5], 6, f"${total:.2f}", border=1, align="R", fill=fill)
            pdf.ln()

        pdf.ln(4)

        # Totals
        x_start = pdf.w - 70
        pdf.set_x(x_start)
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(35, 7, "Subtotal:", align="R")
        pdf.cell(30, 7, f"${subtotal:,.2f}", align="R", ln=True)

        pdf.set_x(x_start)
        pdf.cell(35, 7, f"Tax ({tax_rate*100:.1f}%):", align="R")
        pdf.cell(30, 7, f"${tax:,.2f}", align="R", ln=True)

        pdf.set_x(x_start)
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(35, 8, "TOTAL DUE:", align="R")
        pdf.cell(30, 8, f"${grand_total:,.2f}", align="R", ln=True)

        pdf.ln(8)

        # Footer
        pdf.set_font("Helvetica", "I", 9)
        pdf.cell(0, 6,
                 f"Payment Terms: {vendor_terms} - Please remit by {due_date.strftime('%B %d, %Y')}",
                 align="C", ln=True)

        # Write to Snowflake file
        f = SnowflakeFile.open_new_result("wb")
        f.write(pdf.output(dest="S").encode("latin-1"))
        yield f,

    def end_partition(self):
        pass
$$;


-- ---------------------------------------------------------------------------
-- Stored Procedure: Generate N demo invoices, persist to stage, register
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_GENERATE_DEMO_INVOICES(
    P_VENDOR_NAME STRING,
    P_NUM_ITEMS INT,
    P_CATEGORIES STRING,
    P_APPROX_TOTAL FLOAT,
    P_NUM_INVOICES INT
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_seed INT;
    v_vendor_address VARCHAR;
    v_vendor_terms VARCHAR;
    v_filename VARCHAR;
    v_count INT DEFAULT 0;
    i INT DEFAULT 0;
BEGIN
    -- Look up vendor details
    SELECT address, default_terms
    INTO :v_vendor_address, :v_vendor_terms
    FROM AP_DEMO_DB.AP.VENDORS
    WHERE vendor_name = :P_VENDOR_NAME
    LIMIT 1;

    IF (v_vendor_address IS NULL) THEN
        RETURN 'Error: Vendor not found - ' || P_VENDOR_NAME;
    END IF;

    -- Base seed from epoch seconds to ensure uniqueness
    v_seed := EXTRACT(EPOCH_SECOND FROM CURRENT_TIMESTAMP())::INT;

    -- Generate invoices one at a time
    FOR i IN 1 TO P_NUM_INVOICES DO
        LET current_seed INT := v_seed + i;
        LET current_filename VARCHAR := 'gen_invoice_' || current_seed::VARCHAR || '.pdf';

        -- Generate PDF and copy to stage
        COPY FILES INTO @AP_DEMO_DB.AP.INVOICE_STAGE FROM (
            SELECT
                file,
                :current_filename AS filename
            FROM TABLE(AP_DEMO_DB.AP.GENERATE_INVOICE_PDF(
                :P_VENDOR_NAME,
                :v_vendor_address,
                :v_vendor_terms,
                :P_NUM_ITEMS,
                :P_CATEGORIES,
                :P_APPROX_TOTAL,
                :current_seed
            ))
        );

        v_count := v_count + 1;
    END FOR;

    -- Refresh stage directory
    ALTER STAGE AP_DEMO_DB.AP.INVOICE_STAGE REFRESH;

    -- Register new files in RAW_INVOICES
    INSERT INTO AP_DEMO_DB.AP.RAW_INVOICES (file_name, file_path, staged_at, extracted)
    SELECT
        RELATIVE_PATH AS file_name,
        '@INVOICE_STAGE/' || RELATIVE_PATH AS file_path,
        CURRENT_TIMESTAMP() AS staged_at,
        FALSE AS extracted
    FROM DIRECTORY(@AP_DEMO_DB.AP.INVOICE_STAGE)
    WHERE RELATIVE_PATH LIKE 'gen_%'
      AND RELATIVE_PATH NOT IN (
          SELECT file_name FROM AP_DEMO_DB.AP.RAW_INVOICES
      );

    RETURN 'Generated ' || v_count::VARCHAR || ' invoice(s) and registered in RAW_INVOICES';
END;
$$;
