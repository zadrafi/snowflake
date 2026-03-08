"""
generate_receipts.py — Generate 10 sample convenience store receipts for
the AI_EXTRACT POC.

Creates realistic receipt-style PDFs with line items (grocery/snack purchases).
Each receipt's ground truth is saved as JSON for validation after extraction.

Usage:
    python generate_receipts.py

Output:
    poc/sample_documents/receipt_01.pdf ... receipt_10.pdf
    poc/sample_documents/receipt_ground_truth.json
"""

import json
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
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
    HRFlowable,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

MERCHANTS = [
    {
        "name": "QuickStop Convenience",
        "address": "1423 Broadway\nNew York, NY 10036",
        "phone": "(212) 555-0142",
        "brand_color": "#D32F2F",
    },
    {
        "name": "Garden State Mart",
        "address": "87 Main Street\nHoboken, NJ 07030",
        "phone": "(201) 555-0387",
        "brand_color": "#1B5E20",
    },
    {
        "name": "7-Eleven Store #3847",
        "address": "250 West 42nd Street\nNew York, NY 10036",
        "phone": "(212) 555-0711",
        "brand_color": "#FF6F00",
    },
    {
        "name": "Wawa Store #892",
        "address": "1560 Route 22 West\nWatchung, NJ 07069",
        "phone": "(908) 555-0892",
        "brand_color": "#C62828",
    },
    {
        "name": "Hudson News & Snacks",
        "address": "Penn Station, Lower Level\nNew York, NY 10001",
        "phone": "(212) 555-0499",
        "brand_color": "#0D47A1",
    },
]

ITEMS_POOL = [
    ("Coffee (Large)", 2.99),
    ("Bagel w/ Cream Cheese", 3.49),
    ("Banana", 0.79),
    ("Bottled Water (500ml)", 1.99),
    ("Red Bull (12oz)", 3.99),
    ("Doritos Cool Ranch", 4.29),
    ("Kind Bar — Dark Chocolate", 2.49),
    ("Gatorade (20oz)", 2.79),
    ("NY Post", 1.50),
    ("Lotto Ticket — Mega Millions", 2.00),
    ("Cigarettes — Marlboro", 14.99),
    ("Milk (1 gal, 2%)", 5.49),
    ("Bread — White", 3.99),
    ("Eggs (dozen)", 4.99),
    ("AA Batteries (4pk)", 7.99),
    ("Advil (24ct)", 9.99),
    ("Ice Cream Sandwich", 2.99),
    ("Sour Patch Kids", 2.29),
    ("Diet Coke (20oz)", 2.49),
    ("Hot Dog", 2.99),
    ("Beef Jerky (3oz)", 6.99),
    ("Orange Juice (12oz)", 3.49),
    ("Chapstick", 3.29),
    ("Phone Charger Cable", 12.99),
    ("Scratch-Off Ticket", 5.00),
]

PAYMENT_METHODS = ["VISA ****1234", "MASTERCARD ****5678", "AMEX ****9012",
                   "CASH", "DEBIT ****3456", "APPLE PAY", "CASH"]

TAX_RATES = {"NY": 0.08875, "NJ": 0.06625}


def _d(val):
    """Round to 2 decimal places."""
    return float(Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _generate_receipt_data(receipt_num: int) -> dict:
    """Generate one receipt's data with ground truth."""
    random.seed(2026_04 + receipt_num)

    merchant = random.choice(MERCHANTS)
    is_nj = "NJ" in merchant["address"]
    tax_rate = TAX_RATES["NJ"] if is_nj else TAX_RATES["NY"]

    purchase_date = datetime(2026, 2, 1) + timedelta(
        days=random.randint(0, 30),
        hours=random.randint(6, 22),
        minutes=random.randint(0, 59),
    )
    return_by_date = purchase_date + timedelta(days=30)

    receipt_number = f"R{purchase_date.strftime('%m%d')}-{random.randint(1000, 9999)}"
    transaction_id = f"TXN{random.randint(100000, 999999)}"

    # Pick 2-8 items with quantities
    num_items = random.randint(2, 8)
    chosen_items = random.sample(ITEMS_POOL, min(num_items, len(ITEMS_POOL)))
    line_items = []
    for item_name, base_price in chosen_items:
        qty = random.choice([1, 1, 1, 2, 2, 3])
        total = _d(base_price * qty)
        line_items.append({
            "item": item_name,
            "qty": qty,
            "price": base_price,
            "total": total,
        })

    subtotal = _d(sum(li["total"] for li in line_items))

    # In NJ, most grocery items are tax-exempt; in NY, prepared food is taxed
    # Simplify: tax about 60-80% of items
    taxable_fraction = random.uniform(0.6, 0.8)
    tax_amount = _d(subtotal * taxable_fraction * tax_rate)
    total_paid = _d(subtotal + tax_amount)

    payment_method = random.choice(PAYMENT_METHODS)

    return {
        "receipt_num": receipt_num,
        "merchant": merchant,
        "receipt_number": receipt_number,
        "transaction_id": transaction_id,
        "purchase_date": purchase_date,
        "return_by_date": return_by_date,
        "payment_method": payment_method,
        "line_items": line_items,
        "subtotal": subtotal,
        "tax_amount": tax_amount,
        "total_paid": total_paid,
    }


def _build_receipt_pdf(data: dict, output_path: str):
    """Build a receipt-style PDF."""
    doc = SimpleDocTemplate(
        output_path, pagesize=letter,
        leftMargin=0.8 * inch, rightMargin=0.8 * inch,
        topMargin=0.5 * inch, bottomMargin=0.5 * inch,
    )
    styles = getSampleStyleSheet()
    bc = data["merchant"]["brand_color"]

    s_title = ParagraphStyle("Title", parent=styles["Heading1"], fontSize=16,
                             textColor=colors.HexColor(bc), alignment=TA_CENTER)
    s_center = ParagraphStyle("Center", parent=styles["Normal"], alignment=TA_CENTER,
                              fontSize=9)
    s_right = ParagraphStyle("Right", parent=styles["Normal"], alignment=TA_RIGHT)
    s_bold_right = ParagraphStyle("BoldRight", parent=styles["Normal"],
                                  alignment=TA_RIGHT, fontName="Helvetica-Bold",
                                  fontSize=12)

    elements = []

    # Store header
    elements.append(Paragraph(f"<b>{data['merchant']['name']}</b>", s_title))
    elements.append(Paragraph(
        data["merchant"]["address"].replace("\n", " | ") + " | " + data["merchant"]["phone"],
        s_center,
    ))
    elements.append(Spacer(1, 0.1 * inch))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor(bc)))
    elements.append(Spacer(1, 0.15 * inch))

    # Receipt info
    info_data = [
        ["Receipt #:", data["receipt_number"], "Date:",
         data["purchase_date"].strftime("%m/%d/%Y %I:%M %p")],
        ["Transaction ID:", data["transaction_id"], "Payment:",
         data["payment_method"]],
    ]
    it = Table(info_data, colWidths=[1.2 * inch, 2.0 * inch, 1.0 * inch, 2.3 * inch])
    it.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    elements.append(it)
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=colors.grey))
    elements.append(Spacer(1, 0.1 * inch))

    # Line items
    header = ["Item", "Qty", "Price", "Total"]
    rows = [header]
    for li in data["line_items"]:
        rows.append([
            li["item"],
            str(li["qty"]),
            f"${li['price']:.2f}",
            f"${li['total']:.2f}",
        ])

    lt = Table(rows, colWidths=[3.0 * inch, 0.6 * inch, 1.2 * inch, 1.2 * inch])
    lt.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(bc)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(lt)
    elements.append(Spacer(1, 0.15 * inch))

    # Totals
    totals_data = [
        ["Subtotal:", f"${data['subtotal']:.2f}"],
        ["Tax:", f"${data['tax_amount']:.2f}"],
        ["TOTAL PAID:", f"${data['total_paid']:.2f}"],
    ]
    tt = Table(totals_data, colWidths=[2.0 * inch, 1.2 * inch])
    tt.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, -1), (-1, -1), 12),
        ("LINEABOVE", (0, -1), (-1, -1), 1.5, colors.HexColor(bc)),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    tw = Table([[None, tt]], colWidths=[3.5 * inch, 3.5 * inch])
    elements.append(tw)
    elements.append(Spacer(1, 0.25 * inch))

    # Return policy
    elements.append(HRFlowable(width="100%", thickness=0.5, color=colors.grey))
    elements.append(Spacer(1, 0.1 * inch))
    elements.append(Paragraph(
        f"Returns accepted within 30 days with receipt. Return by: "
        f"<b>{data['return_by_date'].strftime('%m/%d/%Y')}</b>",
        ParagraphStyle("Return", parent=styles["Normal"], fontSize=8,
                       textColor=colors.grey),
    ))
    elements.append(Paragraph(
        "Thank you for shopping with us!",
        ParagraphStyle("Thanks", parent=styles["Normal"], fontSize=9,
                       alignment=TA_CENTER, textColor=colors.HexColor(bc)),
    ))

    doc.build(elements)


def _ground_truth(data: dict) -> dict:
    """Extract ground truth matching RECEIPT config fields."""
    return {
        "file_name": f"receipt_{data['receipt_num']:02d}.pdf",
        "merchant_name": data["merchant"]["name"],
        "receipt_number": data["receipt_number"],
        "transaction_id": data["transaction_id"],
        "purchase_date": data["purchase_date"].strftime("%Y-%m-%d"),
        "return_by_date": data["return_by_date"].strftime("%Y-%m-%d"),
        "payment_method": data["payment_method"],
        "subtotal": data["subtotal"],
        "tax_amount": data["tax_amount"],
        "total_paid": data["total_paid"],
        "item_count": len(data["line_items"]),
    }


def main():
    script_dir = Path(__file__).parent
    output_dir = script_dir / "sample_documents"
    output_dir.mkdir(exist_ok=True)

    ground_truths = []

    print("Generating 10 sample convenience store receipts...\n")
    for i in range(10):
        receipt_num = i + 1
        data = _generate_receipt_data(receipt_num)
        path = output_dir / f"receipt_{receipt_num:02d}.pdf"
        _build_receipt_pdf(data, str(path))

        gt = _ground_truth(data)
        ground_truths.append(gt)
        print(f"  {path.name} — {data['merchant']['name']:25s} | "
              f"{len(data['line_items']):>2} items | "
              f"${data['total_paid']:>7,.2f} | "
              f"{data['payment_method']}")

    gt_path = output_dir / "receipt_ground_truth.json"
    with open(gt_path, "w") as f:
        json.dump(ground_truths, f, indent=2)

    print(f"\nDone! {len(ground_truths)} receipts generated.")
    print(f"Ground truth saved to: {gt_path}")
    print(f"Files in: {output_dir}/")


if __name__ == "__main__":
    main()
