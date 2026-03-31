"""
AI_EXTRACT Output Validator
Adapted from PDF bounding-box checker to validate extraction field quality.
Checks: completeness, type conformance, cross-field consistency, value ranges.
"""

from dataclasses import dataclass, field
import json
import re
from datetime import datetime


@dataclass
class FieldResult:
    field_name: str
    value: object
    field_type: str
    doc_type: str
    file_name: str


@dataclass
class ValidationReport:
    file_name: str
    doc_type: str
    total_fields: int = 0
    passed: int = 0
    warnings: list = field(default_factory=list)
    failures: list = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return round(self.passed / self.total_fields * 100, 1) if self.total_fields > 0 else 0.0

    @property
    def is_valid(self) -> bool:
        return len(self.failures) == 0


FIELD_TYPE_MAP = {
    "vendor_name": "VARCHAR", "utility_company": "VARCHAR", "company_name": "VARCHAR",
    "document_number": "VARCHAR", "invoice_number": "VARCHAR", "account_number": "VARCHAR",
    "reference": "VARCHAR", "meter_number": "VARCHAR", "rate_schedule": "VARCHAR",
    "terms": "VARCHAR", "recipient": "VARCHAR", "service_address": "VARCHAR",
    "document_date": "DATE", "invoice_date": "DATE", "due_date": "DATE",
    "billing_period_start": "DATE", "billing_period_end": "DATE",
    "subtotal": "NUMBER", "tax": "NUMBER", "total": "NUMBER", "total_due": "NUMBER",
    "current_charges": "NUMBER", "previous_balance": "NUMBER",
    "kwh_usage": "NUMBER", "demand_kw": "NUMBER",
}

CROSS_FIELD_RULES = [
    {
        "name": "total_gte_subtotal",
        "fields": ["total", "subtotal"],
        "alt_fields": ["total_due", "current_charges"],
        "check": lambda total, sub: total >= sub,
        "message": "Total ({total}) should be >= subtotal ({subtotal})",
    },
    {
        "name": "due_after_doc_date",
        "fields": ["due_date", "document_date"],
        "alt_fields": ["due_date", "billing_period_end"],
        "check": lambda due, doc: due >= doc,
        "message": "Due date ({due_date}) should be on or after document date ({document_date})",
    },
    {
        "name": "billing_period_order",
        "fields": ["billing_period_end", "billing_period_start"],
        "alt_fields": None,
        "check": lambda end, start: end >= start,
        "message": "Billing period end ({billing_period_end}) should be after start ({billing_period_start})",
    },
]

DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d/%m/%Y",
    "%B %d, %Y", "%b %d, %Y", "%m/%d/%y", "%d %B %Y",
]

NUMBER_PATTERN = re.compile(r"[^0-9.\-]")


def _parse_date(val):
    if val is None:
        return None
    s = str(val).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _parse_number(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = NUMBER_PATTERN.sub("", str(val).strip())
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _check_type_conformance(field_name, value, expected_type):
    if value is None:
        return None, None
    if expected_type == "DATE":
        parsed = _parse_date(value)
        if parsed is None:
            return None, f"Cannot parse '{value}' as a date"
        return parsed, None
    if expected_type == "NUMBER":
        parsed = _parse_number(value)
        if parsed is None:
            return None, f"Cannot parse '{value}' as a number"
        if parsed < 0:
            return parsed, f"Negative value {parsed} for {field_name}"
        return parsed, None
    return str(value), None


def _check_value_sanity(field_name, value, field_type):
    warnings = []
    if field_type == "NUMBER":
        num = _parse_number(value)
        if num is not None:
            if "total" in field_name or "charges" in field_name or "balance" in field_name:
                if num > 1_000_000:
                    warnings.append(f"{field_name}={num:,.2f} unusually high (>1M)")
                if num == 0:
                    warnings.append(f"{field_name}=0 may indicate extraction failure")
    if field_type == "DATE":
        dt = _parse_date(value)
        if dt is not None:
            if dt.year < 2000:
                warnings.append(f"{field_name}={value} has year before 2000")
            if dt > datetime(2030, 1, 1):
                warnings.append(f"{field_name}={value} has year after 2030")
    if field_type == "VARCHAR":
        s = str(value).strip()
        if len(s) < 2:
            warnings.append(f"{field_name}='{s}' suspiciously short")
        if len(s) > 500:
            warnings.append(f"{field_name} value is {len(s)} chars (possibly raw text dump)")
    return warnings


def validate_extraction(file_name: str, doc_type: str, raw_extraction: dict,
                        required_fields: list[str] | None = None) -> ValidationReport:
    report = ValidationReport(file_name=file_name, doc_type=doc_type)

    skip_keys = {"_confidence", "_validation_warnings"}
    fields = {k: v for k, v in raw_extraction.items() if k not in skip_keys}

    if required_fields is None:
        required_fields = list(fields.keys())

    report.total_fields = len(required_fields)

    for fname in required_fields:
        value = fields.get(fname)
        expected_type = FIELD_TYPE_MAP.get(fname, "VARCHAR")

        if value is None or str(value).strip().lower() in ("", "null", "none"):
            report.failures.append(f"MISSING: {fname} is null/empty")
            continue

        parsed, type_err = _check_type_conformance(fname, value, expected_type)
        if type_err:
            report.failures.append(f"TYPE_ERROR: {fname} — {type_err}")
            continue

        sanity_warnings = _check_value_sanity(fname, value, expected_type)
        report.warnings.extend(sanity_warnings)

        report.passed += 1

    for rule in CROSS_FIELD_RULES:
        field_a, field_b = rule["fields"]
        val_a = fields.get(field_a)
        val_b = fields.get(field_b)

        if val_a is None or val_b is None:
            alt = rule.get("alt_fields")
            if alt:
                field_a, field_b = alt
                val_a = fields.get(field_a)
                val_b = fields.get(field_b)

        if val_a is None or val_b is None:
            continue

        expected_type_a = FIELD_TYPE_MAP.get(field_a, "VARCHAR")
        expected_type_b = FIELD_TYPE_MAP.get(field_b, "VARCHAR")

        if expected_type_a == "NUMBER":
            val_a = _parse_number(val_a)
            val_b = _parse_number(val_b)
        elif expected_type_a == "DATE":
            val_a = _parse_date(val_a)
            val_b = _parse_date(val_b)

        if val_a is not None and val_b is not None:
            try:
                if not rule["check"](val_a, val_b):
                    report.warnings.append(
                        f"CROSS_FIELD: {rule['name']} — {rule['message']}"
                    )
            except (TypeError, ValueError):
                pass

    return report


def validate_extraction_batch(records: list[dict]) -> list[str]:
    messages = []
    messages.append(f"Validating {len(records)} extraction record(s)")

    reports = []
    for rec in records:
        file_name = rec.get("file_name", "unknown")
        doc_type = rec.get("doc_type", "UNKNOWN")
        raw = rec.get("raw_extraction", {})
        if isinstance(raw, str):
            raw = json.loads(raw)
        report = validate_extraction(file_name, doc_type, raw)
        reports.append(report)

    total_valid = sum(1 for r in reports if r.is_valid)
    total_warnings = sum(len(r.warnings) for r in reports)
    total_failures = sum(len(r.failures) for r in reports)

    messages.append(f"  Valid: {total_valid}/{len(reports)}")
    messages.append(f"  Total warnings: {total_warnings}")
    messages.append(f"  Total failures: {total_failures}")

    for report in reports:
        if not report.is_valid or report.warnings:
            messages.append(f"\n--- {report.file_name} ({report.doc_type}) ---")
            messages.append(f"  Fields: {report.passed}/{report.total_fields} passed ({report.success_rate}%)")
            for f in report.failures:
                messages.append(f"  FAILURE: {f}")
            for w in report.warnings:
                messages.append(f"  WARNING: {w}")
            if len(messages) >= 100:
                messages.append("Aborting further checks; fix failures and try again")
                return messages

    if total_failures == 0 and total_warnings == 0:
        messages.append("SUCCESS: All extraction outputs are valid")

    return messages


def validate_from_snowflake(session, db: str, limit: int = 50) -> list[str]:
    rows = session.sql(f"""
        SELECT e.file_name, r.doc_type, e.raw_extraction
        FROM {db}.EXTRACTED_FIELDS e
        JOIN {db}.RAW_DOCUMENTS r ON r.file_name = e.file_name
        WHERE e.raw_extraction IS NOT NULL
        ORDER BY e.extracted_at DESC
        LIMIT {limit}
    """).collect()

    records = []
    for row in rows:
        raw = row["RAW_EXTRACTION"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        records.append({
            "file_name": row["FILE_NAME"],
            "doc_type": row["DOC_TYPE"],
            "raw_extraction": raw,
        })

    return validate_extraction_batch(records)


def create_extraction_overlay(session, file_name: str, stage: str, db: str,
                              page_number: int = 0, highlight_field: str = None):
    import pypdfium2 as pdfium
    from PIL import Image, ImageDraw, ImageFont

    file_bytes = session.file.get_stream(f"@{stage}/{file_name}").read()
    pdf = pdfium.PdfDocument(file_bytes)
    page = pdf[page_number]
    scale = 2
    bitmap = page.render(scale=scale)
    img = bitmap.to_pil()
    draw = ImageDraw.Draw(img)

    page_width = page.get_width()
    page_height = page.get_height()

    textpage = page.get_textpage()
    full_text = textpage.get_text_range()

    safe_name = file_name.replace("'", "''")
    rows = session.sql(f"""
        SELECT e.raw_extraction
        FROM {db}.EXTRACTED_FIELDS e
        WHERE e.file_name = '{safe_name}'
        LIMIT 1
    """).collect()

    if not rows or not rows[0]["RAW_EXTRACTION"]:
        pdf.close()
        return img

    raw = rows[0]["RAW_EXTRACTION"]
    if isinstance(raw, str):
        raw = json.loads(raw)

    skip_keys = {"_confidence", "_validation_warnings"}
    fields = {k: v for k, v in raw.items() if k not in skip_keys}

    report = validate_extraction(file_name, "UNKNOWN", raw)
    failed_fields = set()
    warning_fields = set()
    for f in report.failures:
        parts = f.split(": ", 1)
        if len(parts) > 1:
            failed_fields.add(parts[1].split(" ")[0])
    for w in report.warnings:
        if "CROSS_FIELD" not in w:
            name = w.split("=")[0] if "=" in w else w.split(" ")[0]
            warning_fields.add(name)

    def find_text_rects(search_val):
        if not search_val or len(str(search_val).strip()) < 2:
            return []
        search_str = str(search_val).strip().replace("$", "").replace(",", "")
        results = []
        try:
            searcher = textpage.search(search_str, match_case=False, match_whole_word=False)
            if searcher:
                while searcher.get_next():
                    idx = searcher.get_charindex()
                    count = searcher.get_result_count()
                    if idx >= 0 and count > 0:
                        char_boxes = []
                        for i in range(count):
                            try:
                                box = textpage.get_charbox(idx + i)
                                if box and len(box) == 4:
                                    char_boxes.append(box)
                            except Exception:
                                pass
                        if char_boxes:
                            left = min(b[0] for b in char_boxes) * scale
                            bottom = min(b[1] for b in char_boxes)
                            right = max(b[2] for b in char_boxes) * scale
                            top = max(b[3] for b in char_boxes)
                            results.append((left, (page_height - top) * scale, right, (page_height - bottom) * scale))
        except Exception:
            text_lower = full_text.lower()
            needle = search_str.lower()
            pos = text_lower.find(needle)
            if pos >= 0:
                char_boxes = []
                for i in range(len(needle)):
                    try:
                        box = textpage.get_charbox(pos + i)
                        if box and len(box) == 4:
                            char_boxes.append(box)
                    except Exception:
                        pass
                if char_boxes:
                    left = min(b[0] for b in char_boxes) * scale
                    bottom = min(b[1] for b in char_boxes)
                    right = max(b[2] for b in char_boxes) * scale
                    top = max(b[3] for b in char_boxes)
                    results.append((left, (page_height - top) * scale, right, (page_height - bottom) * scale))
        return results

    legend_y = 10
    legend_items = [
        ((0, 180, 0), "Passed"),
        ((220, 40, 40), "Failed/Missing"),
        ((240, 160, 0), "Warning"),
    ]
    for lcolor, ltxt in legend_items:
        draw.rectangle([10, legend_y, 24, legend_y + 14], fill=lcolor, outline=lcolor)
        draw.text((30, legend_y), ltxt, fill="black")
        legend_y += 18

    def rects_intersect(r1, r2):
        return not (r1[0] >= r2[2] or r1[2] <= r2[0] or r1[1] >= r2[3] or r1[3] <= r2[1])

    all_field_rects = []
    labeled_fields = []
    trace_log = []
    for field_name, value in fields.items():
        is_selected = highlight_field is None or field_name == highlight_field

        if field_name in failed_fields:
            color = (220, 40, 40) if is_selected else (220, 40, 40, 60)
            outline = "red"
        elif field_name in warning_fields:
            color = (240, 160, 0) if is_selected else (240, 160, 0, 60)
            outline = "orange"
        else:
            color = (0, 180, 0) if is_selected else (0, 180, 0, 60)
            outline = "green"

        if not is_selected:
            outline = "#cccccc"
            color = (180, 180, 180)

        line_width = 4 if is_selected else 1

        display_val = str(value) if value is not None else "NULL"
        rects = find_text_rects(value)
        search_str = str(value).strip().replace("$", "").replace(",", "") if value else ""
        trace_log.append(f"{field_name}: '{search_str[:20]}' -> {len(rects)} hit(s)")

        if rects:
            for idx_r, rect in enumerate(rects):
                l, t, r, b = rect
                pad = 4 if is_selected else 2
                padded = (l - pad, t - pad, r + pad, b + pad)
                draw.rectangle(padded, outline=outline, width=line_width)
                if is_selected:
                    draw.rectangle(
                        [l - pad - 1, t - pad - 16, l - pad + len(field_name) * 7 + 6, t - pad - 1],
                        fill=outline,
                    )
                    draw.text((l - pad + 2, t - pad - 15), field_name, fill="white")
                all_field_rects.append({"rect": padded, "field": field_name})
                trace_log.append(f"  [{idx_r}] box=({int(l)},{int(t)},{int(r)},{int(b)})")
            labeled_fields.append((field_name, display_val, outline, True))
        else:
            labeled_fields.append((field_name, display_val, outline, False))

    overlap_warnings = []
    for i, ri in enumerate(all_field_rects):
        for j in range(i + 1, len(all_field_rects)):
            rj = all_field_rects[j]
            if ri["field"] != rj["field"] and rects_intersect(ri["rect"], rj["rect"]):
                overlap_warnings.append(
                    f"{ri['field']} overlaps {rj['field']}"
                )
                ox = max(ri["rect"][0], rj["rect"][0])
                oy = max(ri["rect"][1], rj["rect"][1])
                draw.text((ox, oy - 10), "OVERLAP", fill=(180, 0, 180))

    sidebar_x = img.width - 310
    sidebar_y = legend_y + 20
    total_sidebar_rows = len(labeled_fields) + len(overlap_warnings) + len(trace_log) + 5
    draw.rectangle(
        [sidebar_x - 5, sidebar_y - 5, img.width - 5, sidebar_y + total_sidebar_rows * 18 + 10],
        fill=(255, 255, 255, 200), outline="gray",
    )
    draw.text((sidebar_x, sidebar_y), f"{report.passed}/{report.total_fields} passed", fill="black")
    sidebar_y += 20

    for fname, fval, fcolor, found in labeled_fields:
        marker = " ~" if not found else ""
        txt = f"{fname}: {fval[:30]}{marker}"
        draw.text((sidebar_x, sidebar_y), txt, fill=fcolor)
        sidebar_y += 18

    if overlap_warnings:
        sidebar_y += 4
        draw.text((sidebar_x, sidebar_y), "Box Overlaps:", fill=(180, 0, 180))
        sidebar_y += 18
        for ow in overlap_warnings[:5]:
            draw.text((sidebar_x, sidebar_y), ow, fill=(180, 0, 180))
            sidebar_y += 18

    sidebar_y += 8
    draw.text((sidebar_x, sidebar_y), "Trace Log:", fill=(100, 100, 100))
    sidebar_y += 16
    for tl in trace_log[:25]:
        draw.text((sidebar_x, sidebar_y), tl[:40], fill=(120, 120, 120))
        sidebar_y += 14

    pdf.close()
    return img


def create_annotated_pdf(session, file_name: str, stage: str, db: str) -> bytes:
    import io
    from pypdf import PdfReader, PdfWriter
    from pypdf.annotations import FreeText

    file_bytes = session.file.get_stream(f"@{stage}/{file_name}").read()

    safe_name = file_name.replace("'", "''")
    rows = session.sql(f"""
        SELECT e.raw_extraction
        FROM {db}.EXTRACTED_FIELDS e
        WHERE e.file_name = '{safe_name}'
        LIMIT 1
    """).collect()

    if not rows or not rows[0]["RAW_EXTRACTION"]:
        return file_bytes

    raw = rows[0]["RAW_EXTRACTION"]
    if isinstance(raw, str):
        raw = json.loads(raw)

    skip_keys = {"_confidence", "_validation_warnings"}
    fields = {k: v for k, v in raw.items() if k not in skip_keys}

    report = validate_extraction(file_name, "UNKNOWN", raw)
    failed_fields = set()
    for f in report.failures:
        parts = f.split(": ", 1)
        if len(parts) > 1:
            failed_fields.add(parts[1].split(" ")[0])

    reader = PdfReader(io.BytesIO(file_bytes))
    writer = PdfWriter()
    writer.append(reader)

    page = reader.pages[0]
    pdf_width = float(page.mediabox.width)
    pdf_height = float(page.mediabox.height)

    margin_left = 10
    margin_top = 10
    row_height = 14
    col_width = min(280, pdf_width / 2 - 20)

    y = pdf_height - margin_top

    for field_name, value in fields.items():
        display_val = str(value) if value is not None else "NULL"
        text = f"{field_name}: {display_val}"[:60]

        if field_name in failed_fields:
            font_color = "ff0000"
        else:
            font_color = "006400"

        rect = (margin_left, y - row_height, margin_left + col_width, y)

        annotation = FreeText(
            text=text,
            rect=rect,
            font="Helvetica",
            font_size="8pt",
            font_color=font_color,
            border_color=None,
            background_color="ffffff",
        )
        writer.add_annotation(page_number=0, annotation=annotation)
        y -= row_height + 2

    summary = f"{report.passed}/{report.total_fields} passed"
    annotation = FreeText(
        text=summary,
        rect=(margin_left, y - row_height, margin_left + col_width, y),
        font="Helvetica",
        font_size="9pt",
        font_color="000000",
        border_color=None,
        background_color="ffff99",
    )
    writer.add_annotation(page_number=0, annotation=annotation)

    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def render_draggable_overlay(fields: dict, report) -> str:
    rows_html = ""
    failed_fields = set()
    warning_fields = set()
    for f in report.failures:
        parts = f.split(": ", 1)
        if len(parts) > 1:
            failed_fields.add(parts[1].split(" ")[0])
    for w in report.warnings:
        if "CROSS_FIELD" not in w:
            name = w.split("=")[0] if "=" in w else w.split(" ")[0]
            warning_fields.add(name)

    skip_keys = {"_confidence", "_validation_warnings"}
    field_count = 0
    for field_name, value in fields.items():
        if field_name in skip_keys:
            continue
        field_count += 1
        display_val = str(value)[:50] if value is not None else "NULL"
        if field_name in failed_fields:
            color = "#ef4444"
            bg = "#fef2f2"
        elif field_name in warning_fields:
            color = "#f59e0b"
            bg = "#fffbeb"
        else:
            color = "#16a34a"
            bg = "#f0fdf4"
        rows_html += (
            f'<div style="padding:3px 8px;margin:2px 0;border-left:3px solid {color};'
            f'background:{bg};font-size:12px;border-radius:0 4px 4px 0;">'
            f'<b style="color:{color}">{field_name}:</b> '
            f'<span style="color:#374151">{display_val}</span></div>'
        )

    summary = f"{report.passed}/{report.total_fields} passed"
    if report.failures:
        summary += f" | {len(report.failures)} failed"
    if report.warnings:
        summary += f" | {len(report.warnings)} warnings"

    panel_height = 60 + field_count * 24

    return f"""<div id="eo" style="
        width:320px;max-height:500px;background:white;border:1px solid #d1d5db;
        border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.15);
        overflow:hidden;font-family:sans-serif;position:absolute;top:0;left:0;
    "><div id="eh" style="
        background:#1e3a5f;color:white;padding:8px 12px;cursor:grab;
        display:flex;justify-content:space-between;align-items:center;
        border-radius:8px 8px 0 0;user-select:none;
    " onmousedown="(function(e){{var el=document.getElementById('eo'),sx=e.clientX-el.offsetLeft,sy=e.clientY-el.offsetTop;document.onmousemove=function(e){{el.style.left=(e.clientX-sx)+'px';el.style.top=(e.clientY-sy)+'px';}};document.onmouseup=function(){{document.onmousemove=null;document.onmouseup=null;}}}})(event)">
        <span style="font-size:13px;font-weight:600;">Extraction Validation</span>
        <span style="cursor:pointer;font-size:18px;opacity:0.8;"
              onclick="document.getElementById('eo').style.display='none'">&times;</span>
    </div><div style="padding:6px 10px;background:#f8fafc;border-bottom:1px solid #e5e7eb;
                font-size:11px;color:#6b7280;">{summary}</div>
    <div style="overflow-y:auto;max-height:420px;padding:6px;">{rows_html}</div></div>""", panel_height
