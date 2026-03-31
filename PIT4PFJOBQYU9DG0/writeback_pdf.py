"""
PDF Writeback Utility
Generates filled PDF templates from AI_EXTRACT extraction results.
"""

import io
import json
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle


def _get_reconciled_data(session, db: str, file_name: str) -> dict:
    safe_name = file_name.replace("'", "''")
    rows = session.sql(f"""
        SELECT * FROM {db}.V_EXTRACTION_RECONCILED
        WHERE file_name = '{safe_name}'
        LIMIT 1
    """).collect()
    if not rows:
        return {}
    return rows[0].asDict()


def _get_line_items(session, db: str, file_name: str) -> list[dict]:
    safe_name = file_name.replace("'", "''")
    rows = session.sql(f"""
        SELECT * FROM {db}.EXTRACTED_TABLE_DATA
        WHERE file_name = '{safe_name}'
        ORDER BY line_number
    """).collect()
    return [row.asDict() for row in rows]


def _clean_json_array(val) -> str:
    if val is None:
        return ''
    s = str(val).strip()
    if s.startswith('['):
        try:
            items = json.loads(s)
            if isinstance(items, list):
                return ', '.join(str(i) for i in items)
        except (json.JSONDecodeError, TypeError):
            pass
    return s


def _fmt_currency(val) -> str:
    if val is None:
        return ""
    try:
        return f"${float(val):,.2f}"
    except (ValueError, TypeError):
        return str(val)


def _fmt_date(val) -> str:
    if val is None:
        return ""
    return str(val)


def generate_invoice_pdf(session, db: str, file_name: str,
                         title: str = "Extracted Invoice Summary") -> bytes:
    data = _get_reconciled_data(session, db, file_name)
    if not data:
        return b""

    line_items = _get_line_items(session, db, file_name)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            leftMargin=0.75*inch, rightMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('DocTitle', parent=styles['Title'],
                                 fontSize=16, spaceAfter=6)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'],
                                    fontSize=9, textColor=colors.grey)
    label_style = ParagraphStyle('FieldLabel', parent=styles['Normal'],
                                 fontSize=8, textColor=colors.grey)
    value_style = ParagraphStyle('FieldValue', parent=styles['Normal'],
                                 fontSize=10, leading=14)
    small_style = ParagraphStyle('SmallValue', parent=styles['Normal'],
                                 fontSize=7, leading=10)
    elements = []

    elements.append(Paragraph(title, title_style))
    elements.append(Paragraph(
        f"Source: {file_name} | Type: {data.get('DOC_TYPE', 'N/A')} | "
        f"Method: {data.get('EXTRACTION_METHOD', 'N/A')} | "
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        subtitle_style
    ))
    elements.append(Spacer(1, 12))

    header_fields = [
        [
            [Paragraph("Vendor / Entity", label_style),
             Paragraph(str(data.get('ENTITY_NAME') or 'N/A'), value_style)],
            [Paragraph("Document #", label_style),
             Paragraph(str(data.get('DOCUMENT_ID') or 'N/A'), value_style)],
            [Paragraph("Reference / PO", label_style),
             Paragraph(str(data.get('REFERENCE_ID') or 'N/A'), value_style)],
        ],
        [
            [Paragraph("Document Date", label_style),
             Paragraph(_fmt_date(data.get('PRIMARY_DATE')), value_style)],
            [Paragraph("Due Date", label_style),
             Paragraph(_fmt_date(data.get('DUE_DATE')), value_style)],
            [Paragraph("Terms", label_style),
             Paragraph(str(data.get('TERMS') or 'N/A'), value_style)],
        ],
        [
            [Paragraph("Recipient", label_style),
             Paragraph(_clean_json_array(data.get('RECIPIENT')) or 'N/A', value_style)],
            [Paragraph("Quality", label_style),
             Paragraph(str(data.get('QUALITY_STATUS') or 'N/A'), value_style)],
            [Paragraph("Completeness", label_style),
             Paragraph(f"{data.get('COMPLETENESS_PCT', 0)}%", value_style)],
        ],
    ]

    for row_fields in header_fields:
        t = Table([[cell for cell in row_fields]],
                  colWidths=[2.3*inch, 2.3*inch, 2.3*inch])
        t.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('LINEBELOW', (0, 0), (-1, -1), 0.5, colors.Color(0.9, 0.9, 0.9)),
        ]))
        elements.append(t)

    elements.append(Spacer(1, 16))

    if line_items:
        total_lines = len(line_items)
        max_lines = 50
        truncated = total_lines > max_lines
        display_items = line_items[:max_lines]

        header_text = f"Line Items ({total_lines})"
        if truncated:
            header_text += f" — showing first {max_lines}"
        elements.append(Paragraph(header_text, ParagraphStyle(
            'SectionHead', parent=styles['Heading2'], fontSize=12, spaceAfter=6)))

        col_keys = [k for k in line_items[0].keys()
                    if k.upper() not in ('FILE_NAME', 'RECORD_ID', 'LINE_ID',
                                         'LINE_NUMBER', 'RAW_LINE_DATA')]
        header_row = [Paragraph(k.replace('_', ' ').title(), label_style) for k in col_keys]
        table_data = [header_row]
        for item in display_items:
            row = []
            for k in col_keys:
                val = item.get(k)
                row.append(Paragraph(str(val) if val is not None else '', value_style))
            table_data.append(row)

        n_cols = len(col_keys)
        col_width = 6.5 * inch / max(n_cols, 1)
        wrapped_data = []
        for row in table_data:
            wrapped_data.append([
                Paragraph(str(cell.text if hasattr(cell, 'text') else cell)[:80], small_style)
                if isinstance(cell, Paragraph) and len(cell.text) > 40
                else cell
                for cell in row
            ])
        table_data = wrapped_data
        t = Table(table_data, colWidths=[col_width] * n_cols)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.Color(0.93, 0.93, 0.93)),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.Color(0.85, 0.85, 0.85)),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(t)
    else:
        elements.append(Paragraph(
            "<i>No line items extracted for this document.</i>", styles['Normal']))

    elements.append(Spacer(1, 16))

    totals_data = [
        ["Subtotal", _fmt_currency(data.get('SUBTOTAL'))],
        ["Tax", _fmt_currency(data.get('TAX'))],
        ["Total", _fmt_currency(data.get('TOTAL_AMOUNT'))],
    ]
    t = Table(totals_data, colWidths=[5*inch, 2*inch])
    t.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('LINEABOVE', (0, -1), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(t)

    doc.build(elements)
    return buf.getvalue()


def generate_batch_pdfs(session, db: str, file_names: list[str]) -> dict[str, bytes]:
    results = {}
    for fn in file_names:
        pdf_bytes = generate_invoice_pdf(session, db, fn)
        if pdf_bytes:
            results[fn] = pdf_bytes
    return results
