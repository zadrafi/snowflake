"""
generate_contracts.py — Generate 10 sample vendor service contracts for
the AI_EXTRACT POC.

Creates realistic contract PDFs with milestone payment schedules.
Each contract's ground truth is saved as JSON for validation after extraction.

Usage:
    python generate_contracts.py

Output:
    poc/sample_documents/contract_01.pdf ... contract_10.pdf
    poc/sample_documents/contract_ground_truth.json
"""

import json
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
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_JUSTIFY

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

PARTIES = [
    {
        "name": "Alpine Building Services LLC",
        "address": "400 Park Avenue, Suite 1200\nNew York, NY 10022",
        "contact": "Jennifer Walsh, VP Operations",
        "brand_color": "#1565C0",
    },
    {
        "name": "Metro HVAC Solutions Inc.",
        "address": "88 Industrial Parkway\nSecaucus, NJ 07094",
        "contact": "Robert Chen, Director of Sales",
        "brand_color": "#2E7D32",
    },
    {
        "name": "Tri-State IT Consulting Group",
        "address": "One World Trade Center, Floor 55\nNew York, NY 10007",
        "contact": "Amanda Rodriguez, Managing Partner",
        "brand_color": "#4527A0",
    },
    {
        "name": "Garden State Janitorial Co.",
        "address": "215 Route 17 South\nParamus, NJ 07652",
        "contact": "Michael Torres, Account Manager",
        "brand_color": "#00695C",
    },
    {
        "name": "Empire Security Systems",
        "address": "150 Broadway, 18th Floor\nNew York, NY 10038",
        "contact": "David Kim, Regional Director",
        "brand_color": "#37474F",
    },
]

COUNTERPARTIES = [
    "QuickStop Convenience Stores Inc.",
    "Metro Fresh Markets LLC",
    "Hudson Valley Retail Group",
    "Garden State Food Marts Inc.",
    "Tri-Borough Holdings Corp.",
]

SERVICE_TYPES = [
    ("Facilities Maintenance", ["Initial Assessment", "Equipment Install", "Monthly Service Start", "Quarterly Review"]),
    ("HVAC Preventive Maintenance", ["Site Survey", "Unit Servicing", "Filter Replacement", "Annual Inspection"]),
    ("IT Infrastructure Upgrade", ["Requirements Analysis", "Hardware Procurement", "Installation & Config", "Go-Live & Training"]),
    ("Commercial Cleaning", ["Walkthrough & Setup", "Supplies Procurement", "Service Commencement", "Performance Review"]),
    ("Security System Installation", ["Security Audit", "Equipment Delivery", "Installation", "Monitoring Activation"]),
]

TERMS_OPTIONS = [
    "Net 30", "Net 45", "Net 60", "2/10 Net 30", "Due on Receipt",
    "50% upfront, 50% on completion", "Monthly installments",
]


def _d(val):
    """Round to 2 decimal places."""
    return float(Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _generate_contract_data(contract_num: int) -> dict:
    """Generate one contract's data with ground truth."""
    random.seed(2026_05 + contract_num)

    party = PARTIES[contract_num % len(PARTIES)]
    counterparty = COUNTERPARTIES[contract_num % len(COUNTERPARTIES)]
    service_type, milestones = SERVICE_TYPES[contract_num % len(SERVICE_TYPES)]

    effective_date = datetime(2026, 1, 1) + timedelta(days=random.randint(0, 60))
    expiration_date = effective_date + timedelta(days=random.choice([180, 365, 365, 730]))

    contract_number = f"CTR-{effective_date.year}-{random.randint(1000, 9999)}"
    reference_id = f"REF-{counterparty[:3].upper()}-{random.randint(100, 999)}"
    terms = random.choice(TERMS_OPTIONS)

    # Contract value
    base_value = _d(random.uniform(5000, 150000))
    adjustments = _d(random.choice([0, 0, 0, random.uniform(-2000, 5000)]))
    total_value = _d(base_value + adjustments)

    # Milestone schedule — split total_value across milestones
    milestone_data = []
    remaining = total_value
    milestone_date = effective_date
    for i, ms_name in enumerate(milestones):
        if i == len(milestones) - 1:
            amount = _d(remaining)
        else:
            fraction = random.uniform(0.15, 0.35)
            amount = _d(total_value * fraction)
            remaining -= amount

        status = "Pending"
        if milestone_date < datetime(2026, 3, 1):
            status = random.choice(["Complete", "Complete", "In Progress"])

        milestone_data.append({
            "milestone": ms_name,
            "due_date": milestone_date.strftime("%Y-%m-%d"),
            "amount": amount,
            "status": status,
        })
        milestone_date += timedelta(days=random.randint(30, 90))

    return {
        "contract_num": contract_num,
        "party": party,
        "counterparty": counterparty,
        "service_type": service_type,
        "contract_number": contract_number,
        "reference_id": reference_id,
        "effective_date": effective_date,
        "expiration_date": expiration_date,
        "terms": terms,
        "base_value": base_value,
        "adjustments": adjustments,
        "total_value": total_value,
        "milestones": milestone_data,
    }


def _build_contract_pdf(data: dict, output_path: str):
    """Build a professional contract-style PDF."""
    doc = SimpleDocTemplate(
        output_path, pagesize=letter,
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.6 * inch, bottomMargin=0.6 * inch,
    )
    styles = getSampleStyleSheet()
    bc = data["party"]["brand_color"]

    s_title = ParagraphStyle("Title", parent=styles["Heading1"], fontSize=18,
                             textColor=colors.HexColor(bc), alignment=TA_CENTER)
    s_sub = ParagraphStyle("Sub", parent=styles["Heading3"], fontSize=11,
                           textColor=colors.HexColor(bc))
    s_body = ParagraphStyle("Body", parent=styles["Normal"], fontSize=10,
                            alignment=TA_JUSTIFY, leading=14)
    s_right = ParagraphStyle("Right", parent=styles["Normal"], alignment=TA_RIGHT)
    s_small = ParagraphStyle("Small", parent=styles["Normal"], fontSize=8,
                             textColor=colors.grey)

    elements = []

    # Header
    elements.append(Paragraph("SERVICE AGREEMENT", s_title))
    elements.append(Spacer(1, 0.05 * inch))
    elements.append(Paragraph(
        f"<b>{data['service_type']}</b>",
        ParagraphStyle("ServiceType", parent=styles["Normal"], fontSize=12,
                       alignment=TA_CENTER, textColor=colors.HexColor(bc)),
    ))
    elements.append(Spacer(1, 0.1 * inch))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor(bc)))
    elements.append(Spacer(1, 0.2 * inch))

    # Contract info
    info_data = [
        ["Contract Number:", data["contract_number"],
         "Reference ID:", data["reference_id"]],
        ["Effective Date:", data["effective_date"].strftime("%B %d, %Y"),
         "Expiration Date:", data["expiration_date"].strftime("%B %d, %Y")],
        ["Payment Terms:", data["terms"], "", ""],
    ]
    it = Table(info_data, colWidths=[1.4 * inch, 2.0 * inch, 1.4 * inch, 2.0 * inch])
    it.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(bc)),
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f5f5ff")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
    ]))
    elements.append(it)
    elements.append(Spacer(1, 0.2 * inch))

    # Parties
    elements.append(Paragraph("Parties to this Agreement", s_sub))
    elements.append(Spacer(1, 0.05 * inch))

    parties_data = [
        ["SERVICE PROVIDER:", "CLIENT:"],
        [data["party"]["name"], data["counterparty"]],
        [data["party"]["address"].replace("\n", ", "), ""],
        [f"Contact: {data['party']['contact']}", ""],
    ]
    pt = Table(parties_data, colWidths=[3.5 * inch, 3.5 * inch])
    pt.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eaf6")),
    ]))
    elements.append(pt)
    elements.append(Spacer(1, 0.2 * inch))

    # Contract value
    elements.append(Paragraph("Contract Value", s_sub))
    value_data = [
        ["Base Value:", f"${data['base_value']:,.2f}"],
    ]
    if data["adjustments"] != 0:
        label = "Adjustments:" if data["adjustments"] > 0 else "Discount:"
        value_data.append([label, f"${data['adjustments']:,.2f}"])
    value_data.append(["Total Contract Value:", f"${data['total_value']:,.2f}"])

    vt = Table(value_data, colWidths=[2.0 * inch, 1.5 * inch])
    vt.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, -1), (-1, -1), 11),
        ("LINEABOVE", (0, -1), (-1, -1), 1.5, colors.HexColor(bc)),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    vw = Table([[None, vt]], colWidths=[3.5 * inch, 3.5 * inch])
    elements.append(vw)
    elements.append(Spacer(1, 0.2 * inch))

    # Milestone schedule
    elements.append(Paragraph("Payment Milestone Schedule", s_sub))
    ms_header = ["Milestone", "Due Date", "Amount", "Status"]
    ms_rows = [ms_header]
    for ms in data["milestones"]:
        ms_rows.append([
            ms["milestone"],
            ms["due_date"],
            f"${ms['amount']:,.2f}",
            ms["status"],
        ])

    mt = Table(ms_rows, colWidths=[2.2 * inch, 1.3 * inch, 1.5 * inch, 1.2 * inch])
    mt.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(bc)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (2, 0), (2, -1), "RIGHT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(mt)
    elements.append(Spacer(1, 0.3 * inch))

    # Boilerplate
    elements.append(HRFlowable(width="100%", thickness=0.5, color=colors.grey))
    elements.append(Spacer(1, 0.1 * inch))
    elements.append(Paragraph(
        "This agreement is binding upon execution by both parties. "
        "Either party may terminate with 30 days written notice. "
        "All disputes shall be resolved through binding arbitration "
        "in the State of New York.",
        s_body,
    ))
    elements.append(Spacer(1, 0.3 * inch))

    # Signature lines
    sig_data = [
        ["_" * 35, "_" * 35],
        [f"For: {data['party']['name']}", f"For: {data['counterparty']}"],
        ["Date: ________________", "Date: ________________"],
    ]
    st = Table(sig_data, colWidths=[3.5 * inch, 3.5 * inch])
    st.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    elements.append(st)

    doc.build(elements)


def _ground_truth(data: dict) -> dict:
    """Extract ground truth matching CONTRACT config fields."""
    return {
        "file_name": f"contract_{data['contract_num']:02d}.pdf",
        "party_name": data["party"]["name"],
        "contract_number": data["contract_number"],
        "reference_id": data["reference_id"],
        "effective_date": data["effective_date"].strftime("%Y-%m-%d"),
        "expiration_date": data["expiration_date"].strftime("%Y-%m-%d"),
        "terms": data["terms"],
        "counterparty": data["counterparty"],
        "base_value": data["base_value"],
        "adjustments": data["adjustments"],
        "total_value": data["total_value"],
        "milestone_count": len(data["milestones"]),
    }


def main():
    script_dir = Path(__file__).parent
    output_dir = script_dir / "sample_documents"
    output_dir.mkdir(exist_ok=True)

    ground_truths = []

    print("Generating 10 sample vendor service contracts...\n")
    for i in range(10):
        contract_num = i + 1
        data = _generate_contract_data(contract_num)
        path = output_dir / f"contract_{contract_num:02d}.pdf"
        _build_contract_pdf(data, str(path))

        gt = _ground_truth(data)
        ground_truths.append(gt)
        print(f"  {path.name} — {data['party']['name']:35s} | "
              f"${data['total_value']:>10,.2f} | "
              f"{data['terms']}")

    gt_path = output_dir / "contract_ground_truth.json"
    with open(gt_path, "w") as f:
        json.dump(ground_truths, f, indent=2)

    print(f"\nDone! {len(ground_truths)} contracts generated.")
    print(f"Ground truth saved to: {gt_path}")
    print(f"Files in: {output_dir}/")


if __name__ == "__main__":
    main()
