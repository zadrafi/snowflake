#!/usr/bin/env python3
"""
generate_invoices.py — Generate 100 realistic convenience store distributor invoices + 5 demo invoices.

Usage:
    pip install reportlab
    python generate_invoices.py

Outputs:
    data/invoices/         — 100 PDFs for initial batch load
    data/demo_invoices/    — 5 PDFs for live demo
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER

# ---------------------------------------------------------------------------
# Reference data — real c-store distributors and products
# ---------------------------------------------------------------------------

VENDORS = [
    {
        "name": "McLane Company, Inc.",
        "address": "4747 McLane Parkway\nTemple, TX 76504",
        "terms": "Net 15",
    },
    {
        "name": "Core-Mark International",
        "address": "395 Oyster Point Blvd, Suite 415\nSouth San Francisco, CA 94080",
        "terms": "Net 30",
    },
    {
        "name": "S&P Company",
        "address": "1600 Distribution Dr\nDuluth, GA 30097",
        "terms": "Net 15",
    },
    {
        "name": "Coca-Cola Bottling Co.",
        "address": "One Coca-Cola Plaza\nAtlanta, GA 30313",
        "terms": "Net 30",
    },
    {
        "name": "PepsiCo / Frito-Lay",
        "address": "7701 Legacy Drive\nPlano, TX 75024",
        "terms": "Net 30",
    },
    {
        "name": "Anheuser-Busch InBev",
        "address": "One Busch Place\nSt. Louis, MO 63118",
        "terms": "Net 30",
    },
    {
        "name": "Altria Group / Philip Morris",
        "address": "6601 W Broad Street\nRichmond, VA 23230",
        "terms": "Net 15",
    },
    {
        "name": "Keurig Dr Pepper",
        "address": "53 South Avenue\nBurlington, MA 01803",
        "terms": "Net 30",
    },
    {
        "name": "Mars Wrigley Confectionery",
        "address": "800 High Street\nHackettstown, NJ 07840",
        "terms": "Net 30",
    },
    {
        "name": "Mondelez International",
        "address": "905 W Fulton Market, Suite 200\nChicago, IL 60607",
        "terms": "Net 60",
    },
    {
        "name": "Red Bull Distribution",
        "address": "1740 Stewart Street\nSanta Monica, CA 90404",
        "terms": "Net 15",
    },
    {
        "name": "Monster Beverage Corp.",
        "address": "1 Monster Way\nCorona, CA 92879",
        "terms": "Net 30",
    },
]

# Products by category with realistic price ranges (unit cost to the store)
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


def _random_date(start: datetime, end: datetime) -> datetime:
    """Return a random date between start and end."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def _generate_invoice_data(invoice_num: int, date_start: datetime, date_end: datetime) -> dict:
    """Generate a single invoice's data."""
    vendor = random.choice(VENDORS)
    invoice_date = _random_date(date_start, date_end)

    # Payment terms -> due date
    terms_days = int(vendor["terms"].split()[-1])
    due_date = invoice_date + timedelta(days=terms_days)

    # Pick 3-15 line items from categories the vendor would supply
    num_items = random.randint(3, 15)
    all_products = []
    for cat, products in PRODUCTS.items():
        for p in products:
            all_products.append((cat, p[0], p[1], p[2]))

    selected = random.sample(all_products, min(num_items, len(all_products)))

    line_items = []
    for cat, name, price_low, price_high in selected:
        unit_price = round(random.uniform(price_low, price_high), 2)
        qty = random.choice([6, 12, 24, 36, 48, 60, 72, 96, 120])
        line_total = round(unit_price * qty, 2)
        line_items.append(
            {
                "category": cat,
                "product": name,
                "quantity": qty,
                "unit_price": unit_price,
                "line_total": line_total,
            }
        )

    subtotal = round(sum(li["line_total"] for li in line_items), 2)
    tax_rate = round(random.uniform(0.06, 0.09), 4)
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)

    po_number = f"PO-{random.randint(100000, 999999)}"
    invoice_number = f"INV-{invoice_num:05d}"

    return {
        "vendor": vendor,
        "invoice_number": invoice_number,
        "po_number": po_number,
        "invoice_date": invoice_date,
        "due_date": due_date,
        "line_items": line_items,
        "subtotal": subtotal,
        "tax_rate": tax_rate,
        "tax": tax,
        "total": total,
    }


def _build_pdf(invoice_data: dict, output_path: str):
    """Render a single invoice as a PDF."""
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )

    styles = getSampleStyleSheet()
    style_right = ParagraphStyle("Right", parent=styles["Normal"], alignment=TA_RIGHT)
    style_center = ParagraphStyle("Center", parent=styles["Normal"], alignment=TA_CENTER)
    style_title = ParagraphStyle(
        "InvoiceTitle",
        parent=styles["Heading1"],
        fontSize=20,
        textColor=colors.HexColor("#1a237e"),
    )
    style_vendor = ParagraphStyle(
        "VendorName",
        parent=styles["Heading2"],
        fontSize=14,
        textColor=colors.HexColor("#1a237e"),
    )

    elements = []
    v = invoice_data["vendor"]

    # --- Header ---
    elements.append(Paragraph(v["name"], style_vendor))
    elements.append(Paragraph(v["address"].replace("\n", "<br/>"), styles["Normal"]))
    elements.append(Spacer(1, 0.3 * inch))
    elements.append(Paragraph("INVOICE", style_title))
    elements.append(Spacer(1, 0.15 * inch))

    # --- Invoice details table ---
    details_data = [
        ["Invoice Number:", invoice_data["invoice_number"]],
        ["PO Number:", invoice_data["po_number"]],
        ["Invoice Date:", invoice_data["invoice_date"].strftime("%B %d, %Y")],
        ["Due Date:", invoice_data["due_date"].strftime("%B %d, %Y")],
        ["Payment Terms:", v["terms"]],
    ]
    details_table = Table(details_data, colWidths=[1.8 * inch, 3 * inch])
    details_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    elements.append(details_table)
    elements.append(Spacer(1, 0.3 * inch))

    # --- Bill To (fictional store) ---
    store_num = random.randint(100, 999)
    elements.append(Paragraph("<b>Bill To:</b>", styles["Normal"]))
    elements.append(
        Paragraph(
            f"QuickStop Convenience Store #{store_num}<br/>"
            f"{random.randint(100, 9999)} {random.choice(['Main', 'Highway', 'Commerce', 'Oak', 'Elm', 'Pine', 'Maple', 'Cedar', 'Park', 'Lake'])} "
            f"{random.choice(['St', 'Rd', 'Blvd', 'Ave', 'Dr'])}<br/>"
            f"{random.choice(['Springfield', 'Riverside', 'Fairview', 'Madison', 'Georgetown', 'Franklin', 'Clinton', 'Arlington', 'Burlington', 'Lakewood'])}, "
            f"{random.choice(['GA', 'FL', 'TX', 'TN', 'NC', 'OH', 'IL', 'PA', 'VA', 'CO'])} {random.randint(30000, 89999)}",
            styles["Normal"],
        )
    )
    elements.append(Spacer(1, 0.3 * inch))

    # --- Line items table ---
    header_row = ["#", "Product", "Category", "Qty", "Unit Price", "Total"]
    table_data = [header_row]
    for i, li in enumerate(invoice_data["line_items"], 1):
        table_data.append(
            [
                str(i),
                li["product"],
                li["category"],
                str(li["quantity"]),
                f"${li['unit_price']:.2f}",
                f"${li['line_total']:.2f}",
            ]
        )

    col_widths = [0.4 * inch, 2.5 * inch, 1.3 * inch, 0.6 * inch, 0.9 * inch, 0.9 * inch]
    line_table = Table(table_data, colWidths=col_widths)
    line_table.setStyle(
        TableStyle(
            [
                # Header
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a237e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                # Body
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("ALIGN", (0, 1), (0, -1), "CENTER"),
                ("ALIGN", (3, 1), (3, -1), "CENTER"),
                ("ALIGN", (4, 1), (5, -1), "RIGHT"),
                # Grid
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    elements.append(line_table)
    elements.append(Spacer(1, 0.2 * inch))

    # --- Totals ---
    totals_data = [
        ["Subtotal:", f"${invoice_data['subtotal']:,.2f}"],
        [f"Tax ({invoice_data['tax_rate']*100:.1f}%):", f"${invoice_data['tax']:,.2f}"],
        ["TOTAL DUE:", f"${invoice_data['total']:,.2f}"],
    ]
    totals_table = Table(totals_data, colWidths=[1.5 * inch, 1.2 * inch])
    totals_table.setStyle(
        TableStyle(
            [
                ("ALIGN", (0, 0), (0, -1), "RIGHT"),
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, -1), (-1, -1), 12),
                ("LINEABOVE", (0, -1), (-1, -1), 1, colors.black),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    # Right-align the totals table
    totals_wrapper = Table([[None, totals_table]], colWidths=[4.0 * inch, 2.7 * inch])
    elements.append(totals_wrapper)

    elements.append(Spacer(1, 0.4 * inch))
    elements.append(
        Paragraph(
            f"<i>Payment Terms: {v['terms']} — Please remit by {invoice_data['due_date'].strftime('%B %d, %Y')}</i>",
            style_center,
        )
    )

    doc.build(elements)


def main():
    script_dir = Path(__file__).parent
    invoices_dir = script_dir / "invoices"
    demo_dir = script_dir / "demo_invoices"
    invoices_dir.mkdir(exist_ok=True)
    demo_dir.mkdir(exist_ok=True)

    # Date range: ~6 months back from today
    date_end = datetime.now()
    date_start = date_end - timedelta(days=180)

    random.seed(42)  # Reproducible output

    # --- Generate 100 initial invoices ---
    print("Generating 100 initial invoices...")
    for i in range(1, 101):
        data = _generate_invoice_data(i, date_start, date_end)
        path = invoices_dir / f"invoice_{i:03d}.pdf"
        _build_pdf(data, str(path))
        if i % 25 == 0:
            print(f"  {i}/100 done")

    # --- Generate 5 demo invoices (recent dates, distinct vendors) ---
    print("Generating 5 demo invoices...")
    demo_date_start = date_end - timedelta(days=7)  # Last week
    demo_vendors = random.sample(VENDORS, 5)
    for i, vendor in enumerate(demo_vendors, 101):
        data = _generate_invoice_data(i, demo_date_start, date_end)
        # Override vendor to ensure distinct vendors for demo
        data["vendor"] = vendor
        terms_days = int(vendor["terms"].split()[-1])
        data["due_date"] = data["invoice_date"] + timedelta(days=terms_days)
        path = demo_dir / f"demo_invoice_{i - 100:02d}.pdf"
        _build_pdf(data, str(path))

    print(f"\nDone! Created:")
    print(f"  {len(list(invoices_dir.glob('*.pdf')))} invoices in {invoices_dir}")
    print(f"  {len(list(demo_dir.glob('*.pdf')))} demo invoices in {demo_dir}")


if __name__ == "__main__":
    main()
