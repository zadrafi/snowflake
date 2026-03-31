"""
field_highlighter.py — Nanonets-style interactive PDF field highlighting for Streamlit.

Two rendering modes:
  1. Interactive HTML/JS overlay (primary) — rendered via st.components.v1.html()
  2. PIL image overlay (fallback) — rendered via st.image()

Both use pypdfium2 text-search to locate extracted field values on rasterized PDF pages.
No bounding-box coordinates required in raw_extraction — coordinates are computed at render time.

Deploy: PUT this file alongside your other pages on the Streamlit stage.

Usage in a Streamlit page:
    from field_highlighter import render_field_highlight_viewer

    render_field_highlight_viewer(
        session=session,
        file_name="invoice_001.pdf",
        stage=STAGE,
        db=DB,
        raw_extraction=raw_dict,
        doc_type="INVOICE",
        mode="interactive",        # or "image"
        selected_field=None,        # or a specific field name
    )
"""

import base64
import io
import json
import os
import tempfile
from dataclasses import dataclass, field as dc_field
from typing import Optional

import pypdfium2 as pdfium
from PIL import Image, ImageDraw


# ─── Data structures ─────────────────────────────────────────────────────────

FIELD_COLORS = [
    "#3b82f6",  # blue
    "#10b981",  # emerald
    "#f59e0b",  # amber
    "#ef4444",  # red
    "#8b5cf6",  # violet
    "#ec4899",  # pink
    "#06b6d4",  # cyan
    "#f97316",  # orange
    "#14b8a6",  # teal
    "#6366f1",  # indigo
    "#84cc16",  # lime
    "#d946ef",  # fuchsia
]


@dataclass
class FieldBox:
    """A single bounding box for an extracted field on a rendered page."""

    field_name: str
    value: str
    x: float  # px on rendered image
    y: float
    width: float
    height: float
    page: int
    color: str
    status: str  # "pass", "fail", "warn"


# ─── Core: text search → bounding boxes ─────────────────────────────────────


def _search_field_boxes(
    textpage,
    page_height_pts: float,
    scale: float,
    field_name: str,
    value,
    color: str,
    status: str,
    page_idx: int = 0,
    full_text: str = "",
) -> list[FieldBox]:
    """Search for a field value's text on a PDF page and return FieldBox list."""
    if value is None or str(value).strip() == "":
        return []

    search_str = str(value).strip()
    # Strip currency/number formatting for better matching
    clean_str = search_str.replace("$", "").replace(",", "").strip()
    if len(clean_str) < 2:
        return []

    boxes = []

    def _charboxes_to_fieldbox(char_boxes):
        if not char_boxes:
            return None
        left = min(b[0] for b in char_boxes) * scale
        bottom = min(b[1] for b in char_boxes)
        right = max(b[2] for b in char_boxes) * scale
        top = max(b[3] for b in char_boxes)
        y_top = (page_height_pts - top) * scale
        y_bot = (page_height_pts - bottom) * scale
        pad = 3
        return FieldBox(
            field_name=field_name,
            value=search_str[:80],
            x=max(0, left - pad),
            y=max(0, y_top - pad),
            width=(right - left) + 2 * pad,
            height=(y_bot - y_top) + 2 * pad,
            page=page_idx,
            color=color,
            status=status,
        )

    # Strategy 1: pypdfium2 searcher API
    try:
        searcher = textpage.search(clean_str, match_case=False, match_whole_word=False)
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
                    fb = _charboxes_to_fieldbox(char_boxes)
                    if fb:
                        boxes.append(fb)
                        break  # Take first match only
    except Exception:
        pass

    # Strategy 2: fallback string-find in full text
    if not boxes and full_text:
        text_lower = full_text.lower()
        needle = clean_str.lower()
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
            fb = _charboxes_to_fieldbox(char_boxes)
            if fb:
                boxes.append(fb)

    return boxes


def _load_pdf_bytes(session, file_name: str, stage: str) -> bytes:
    """Load PDF bytes from a Snowflake stage.

    Tries session.file.get_stream() first (SiS-native, no temp files).
    Falls back to session.file.get() with a temp directory if get_stream
    is unavailable or raises NotImplementedError.
    """
    # Attempt 1: get_stream (preferred — no disk I/O)
    try:
        return session.file.get_stream(f"@{stage}/{file_name}").read()
    except (NotImplementedError, AttributeError):
        pass

    # Attempt 2: get() to temp dir
    with tempfile.TemporaryDirectory() as tmpdir:
        stage_path = f"@{stage}/{file_name}"
        session.file.get(stage_path, tmpdir)
        local_path = os.path.join(tmpdir, file_name)
        if not os.path.exists(local_path):
            base = os.path.basename(file_name)
            alt = os.path.join(tmpdir, base)
            if os.path.exists(alt):
                local_path = alt
        if not os.path.exists(local_path):
            downloaded = os.listdir(tmpdir)
            if downloaded:
                local_path = os.path.join(tmpdir, downloaded[0])
            else:
                raise FileNotFoundError(
                    f"session.file.get() returned no files for {stage_path}"
                )
        with open(local_path, "rb") as f:
            return f.read()


def compute_field_boxes(
    session,
    file_name: str,
    stage: str,
    raw_extraction: dict,
    failed_fields: set,
    warning_fields: set,
    page_number: int = 0,
    scale: float = 2.0,
) -> tuple[Image.Image, list[FieldBox]]:
    """
    Rasterize a PDF page and compute FieldBox locations for all extracted fields.

    Returns:
        (pil_image, list_of_FieldBox)
    """
    file_bytes = _load_pdf_bytes(session, file_name, stage)
    pdf = pdfium.PdfDocument(file_bytes)
    page = pdf[page_number]

    page_height = page.get_height()
    bitmap = page.render(scale=scale)
    img = bitmap.to_pil()

    textpage = page.get_textpage()
    full_text = textpage.get_text_range()

    skip_keys = {"_confidence", "_validation_warnings"}
    fields = {k: v for k, v in raw_extraction.items() if k not in skip_keys}

    all_boxes: list[FieldBox] = []
    color_idx = 0

    for field_name, value in fields.items():
        if field_name in failed_fields:
            status = "fail"
        elif field_name in warning_fields:
            status = "warn"
        else:
            status = "pass"

        color = FIELD_COLORS[color_idx % len(FIELD_COLORS)]
        color_idx += 1

        found = _search_field_boxes(
            textpage,
            page_height,
            scale,
            field_name,
            value,
            color,
            status,
            page_number,
            full_text,
        )
        if found:
            all_boxes.extend(found)
        else:
            # "not found" placeholder — field panel still shows it
            all_boxes.append(
                FieldBox(
                    field_name=field_name,
                    value=str(value)[:80] if value else "NULL",
                    x=-1,
                    y=-1,
                    width=0,
                    height=0,
                    page=page_number,
                    color=color,
                    status=status,
                )
            )

    pdf.close()
    return img, all_boxes


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _extract_validation_sets(report) -> tuple[set, set]:
    """Pull failed/warning field names from a ValidationReport."""
    failed = set()
    warned = set()
    for f in report.failures:
        parts = f.split(": ", 1)
        if len(parts) > 1:
            failed.add(parts[1].split(" ")[0])
    for w in report.warnings:
        if "CROSS_FIELD" not in w:
            name = w.split("=")[0] if "=" in w else w.split(" ")[0]
            warned.add(name)
    return failed, warned


def _hex_to_rgb(hex_color: str) -> str:
    """Convert '#3b82f6' → '59,130,246'."""
    h = hex_color.lstrip("#")
    return f"{int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)}"


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# ─── Mode B: Interactive HTML/JS overlay ─────────────────────────────────────


def _build_interactive_html(
    img: Image.Image,
    boxes: list[FieldBox],
    selected_field: Optional[str] = None,
    viewer_height: int = 900,
) -> str:
    """
    Self-contained HTML viewer with:
      - PDF page as base64 background image
      - Colored bounding boxes as absolutely-positioned divs
      - Right-side field panel with click/hover interaction
    All CSS/JS inline — no external deps (SiS CSP safe).
    """
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    img_w, img_h = img.size

    max_display_w = 640
    display_scale = min(1.0, max_display_w / img_w)
    display_w = int(img_w * display_scale)
    display_h = int(img_h * display_scale)

    box_divs = []
    panel_rows = []
    fields_seen = {}

    for b in boxes:
        has_box = b.x >= 0 and b.width > 0
        if b.field_name not in fields_seen:
            fields_seen[b.field_name] = {
                "color": b.color,
                "value": b.value,
                "status": b.status,
                "has_box": has_box,
            }
        if has_box:
            fields_seen[b.field_name]["has_box"] = True
            bx = b.x * display_scale
            by = b.y * display_scale
            bw = b.width * display_scale
            bh = b.height * display_scale
            is_sel = selected_field and b.field_name == selected_field
            opacity = "0.35" if not is_sel and selected_field else "0.25"
            border_w = "3" if is_sel else "2"
            z = "20" if is_sel else "10"
            safe = b.field_name.replace("'", "\\'").replace('"', "&quot;")
            box_divs.append(
                f"""
            <div class="fbox" data-field="{safe}"
                 style="left:{bx:.1f}px;top:{by:.1f}px;width:{bw:.1f}px;height:{bh:.1f}px;
                        border:{border_w}px solid {b.color};
                        background:rgba({_hex_to_rgb(b.color)},{opacity});
                        z-index:{z};"
                 onclick="selectField('{safe}')"
                 onmouseenter="hoverField('{safe}',true)"
                 onmouseleave="hoverField('{safe}',false)">
                <span class="fbox-label" style="background:{b.color};">{b.field_name}</span>
            </div>"""
            )

    for fname, info in fields_seen.items():
        s_icon = {"pass": "&#10003;", "fail": "&#10007;", "warn": "&#9888;"}.get(
            info["status"], "·"
        )
        s_cls = info["status"]
        loc = "&#128205;" if info["has_box"] else "&#10060;"
        is_sel = "selected" if selected_field == fname else ""
        safe = fname.replace("'", "\\'").replace('"', "&quot;")
        panel_rows.append(
            f"""
        <div class="fpanel-row {s_cls} {is_sel}" data-field="{safe}"
             onclick="selectField('{safe}')"
             onmouseenter="hoverField('{safe}',true)"
             onmouseleave="hoverField('{safe}',false)">
            <div class="fpanel-color" style="background:{info['color']};"></div>
            <div class="fpanel-content">
                <div class="fpanel-name">{s_icon} {fname} {loc}</div>
                <div class="fpanel-value">{_html_escape(info['value'])}</div>
            </div>
        </div>"""
        )

    total = len(fields_seen)
    located = sum(1 for v in fields_seen.values() if v["has_box"])
    failed = sum(1 for v in fields_seen.values() if v["status"] == "fail")
    warned = sum(1 for v in fields_seen.values() if v["status"] == "warn")

    return f"""<!DOCTYPE html>
<html><head><style>
*{{margin:0;padding:0;box-sizing:border-box}}
html,body{{height:100%;overflow:hidden}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:transparent}}
.viewer-container{{display:flex;gap:12px;width:100%;height:{viewer_height}px}}
.pdf-pane{{flex:1;overflow:auto;border:1px solid #e2e8f0;border-radius:8px;background:#f8fafc;position:relative}}
.pdf-wrap{{position:relative;width:{display_w}px;height:{display_h}px;margin:8px auto}}
.pdf-wrap img{{width:100%;height:100%;display:block;user-select:none;-webkit-user-drag:none}}
.fbox{{position:absolute;cursor:pointer;border-radius:2px;transition:opacity .15s,box-shadow .15s}}
.fbox:hover,.fbox.active{{opacity:1!important;box-shadow:0 0 0 2px rgba(0,0,0,.2),0 2px 8px rgba(0,0,0,.15);z-index:30!important}}
.fbox-label{{position:absolute;top:-20px;left:-1px;font-size:10px;font-weight:600;color:#fff;padding:1px 5px;border-radius:3px 3px 0 0;white-space:nowrap;pointer-events:none;opacity:0;transition:opacity .15s}}
.fbox:hover .fbox-label,.fbox.active .fbox-label{{opacity:1}}
.field-panel{{width:300px;min-width:260px;border:1px solid #e2e8f0;border-radius:8px;background:#fff;display:flex;flex-direction:column;overflow:hidden}}
.fpanel-header{{padding:10px 14px;background:#1e293b;color:#fff;font-size:13px;font-weight:600;letter-spacing:.02em}}
.fpanel-stats{{padding:6px 14px;background:#f1f5f9;border-bottom:1px solid #e2e8f0;font-size:11px;color:#64748b;display:flex;gap:12px;flex-wrap:wrap}}
.fpanel-stats .stat{{display:flex;align-items:center;gap:3px}}
.fpanel-stats .dot{{width:8px;height:8px;border-radius:50%;display:inline-block}}
.fpanel-body{{flex:1;overflow-y:auto;padding:6px}}
.fpanel-row{{display:flex;align-items:flex-start;gap:8px;padding:6px 8px;border-radius:6px;cursor:pointer;transition:background .12s;border:1px solid transparent}}
.fpanel-row:hover{{background:#f1f5f9}}
.fpanel-row.selected,.fpanel-row.highlighted{{background:#eff6ff;border-color:#bfdbfe}}
.fpanel-color{{width:4px;min-height:28px;border-radius:2px;flex-shrink:0;margin-top:2px}}
.fpanel-content{{flex:1;min-width:0}}
.fpanel-name{{font-size:11px;font-weight:600;color:#334155;line-height:1.3}}
.fpanel-value{{font-size:11px;color:#64748b;line-height:1.3;word-break:break-word}}
.fpanel-row.fail .fpanel-name{{color:#dc2626}}
.fpanel-row.warn .fpanel-name{{color:#d97706}}
.fpanel-body::-webkit-scrollbar{{width:6px}}
.fpanel-body::-webkit-scrollbar-thumb{{background:#cbd5e1;border-radius:3px}}
.fpanel-body::-webkit-scrollbar-track{{background:transparent}}
.pdf-pane::-webkit-scrollbar{{width:8px}}
.pdf-pane::-webkit-scrollbar-thumb{{background:#94a3b8;border-radius:4px}}
.pdf-pane::-webkit-scrollbar-track{{background:#f1f5f9}}
</style></head><body>
<div class="viewer-container">
  <div class="pdf-pane" id="pdfPane"><div class="pdf-wrap">
    <img src="data:image/png;base64,{b64}" alt="Document" draggable="false"/>
    {''.join(box_divs)}
  </div></div>
  <div class="field-panel">
    <div class="fpanel-header">Extracted Fields</div>
    <div class="fpanel-stats">
      <div class="stat"><div class="dot" style="background:#10b981"></div>{located}/{total} located</div>
      <div class="stat"><div class="dot" style="background:#ef4444"></div>{failed} failed</div>
      <div class="stat"><div class="dot" style="background:#f59e0b"></div>{warned} warnings</div>
    </div>
    <div class="fpanel-body" id="fieldBody">{''.join(panel_rows)}</div>
  </div>
</div>
<script>
var currentField={json.dumps(selected_field)};
function selectField(n){{currentField=currentField===n?null:n;updateHighlights();scrollToBox(currentField)}}
function hoverField(n,e){{document.querySelectorAll('.fbox[data-field="'+n+'"]').forEach(function(el){{if(e)el.classList.add('active');else if(currentField!==n)el.classList.remove('active')}});document.querySelectorAll('.fpanel-row[data-field="'+n+'"]').forEach(function(el){{if(e)el.classList.add('highlighted');else if(currentField!==n)el.classList.remove('highlighted')}})}}
function updateHighlights(){{document.querySelectorAll('.fbox').forEach(function(el){{el.classList.remove('active');if(currentField){{el.style.opacity=el.getAttribute('data-field')===currentField?'1':'0.12'}}else{{el.style.opacity=''}}}});document.querySelectorAll('.fpanel-row').forEach(function(el){{el.classList.remove('selected','highlighted')}});if(currentField){{document.querySelectorAll('.fbox[data-field="'+currentField+'"]').forEach(function(el){{el.classList.add('active')}});document.querySelectorAll('.fpanel-row[data-field="'+currentField+'"]').forEach(function(el){{el.classList.add('selected')}})}}}}
function scrollToBox(n){{if(!n)return;var pdfPane=document.getElementById('pdfPane');var b=document.querySelector('.fbox[data-field="'+n+'"]');if(b&&pdfPane){{var paneRect=pdfPane.getBoundingClientRect();var boxRect=b.getBoundingClientRect();var scrollTarget=pdfPane.scrollTop+(boxRect.top-paneRect.top)-(paneRect.height/2)+(boxRect.height/2);pdfPane.scrollTo({{top:Math.max(0,scrollTarget),behavior:'smooth'}})}};var fieldBody=document.getElementById('fieldBody');var r=document.querySelector('.fpanel-row[data-field="'+n+'"]');if(r&&fieldBody){{var bodyRect=fieldBody.getBoundingClientRect();var rowRect=r.getBoundingClientRect();var scrollTarget=fieldBody.scrollTop+(rowRect.top-bodyRect.top)-(bodyRect.height/2)+(rowRect.height/2);fieldBody.scrollTo({{top:Math.max(0,scrollTarget),behavior:'smooth'}})}}}}
updateHighlights();
</script></body></html>"""


# ─── Mode A: PIL image overlay (fallback) ───────────────────────────────────


def render_pil_overlay(
    img: Image.Image,
    boxes: list[FieldBox],
    selected_field: Optional[str] = None,
) -> Image.Image:
    """Draw bounding boxes and labels on a PIL image copy."""
    result = img.copy()
    draw = ImageDraw.Draw(result)

    for b in boxes:
        if b.x < 0 or b.width <= 0:
            continue

        is_selected = selected_field is None or b.field_name == selected_field
        line_w = 3 if is_selected else 1
        rgb = tuple(int(b.color.lstrip("#")[i : i + 2], 16) for i in (0, 2, 4))
        outline_color = rgb if is_selected else (180, 180, 180)

        rect = (b.x, b.y, b.x + b.width, b.y + b.height)
        draw.rectangle(rect, outline=outline_color, width=line_w)

        if is_selected:
            label = b.field_name
            lbl_w = len(label) * 7 + 8
            lbl_h = 16
            lbl_rect = (b.x, b.y - lbl_h - 1, b.x + lbl_w, b.y - 1)
            draw.rectangle(lbl_rect, fill=rgb)
            draw.text((b.x + 3, b.y - lbl_h), label, fill="white")

    return result


# ─── Public API ──────────────────────────────────────────────────────────────


def render_field_highlight_viewer(
    session,
    file_name: str,
    stage: str,
    db: str,
    raw_extraction: dict,
    doc_type: str = "INVOICE",
    mode: str = "interactive",
    selected_field: Optional[str] = None,
    page_number: int = 0,
    scale: float = 2.0,
    viewer_height: int = 850,
):
    """
    Render the Nanonets-style field highlighting viewer in Streamlit.

    Args:
        session:          Snowpark session
        file_name:        PDF file name on stage
        stage:            Full stage path (e.g. "MY_DB.MY_SCHEMA.DOCUMENT_STAGE")
        db:               Database.schema prefix (e.g. "MY_DB.MY_SCHEMA")
        raw_extraction:   Dict of extracted fields from raw_extraction VARIANT
        doc_type:         Document type for validation rules
        mode:             "interactive" (HTML/JS) or "image" (PIL fallback)
        selected_field:   Optional field name to pre-highlight
        page_number:      PDF page index (0-based)
        scale:            Render scale for pypdfium2 (2.0 = 144 DPI)
        viewer_height:    Height of the interactive viewer in px
    """
    import streamlit as st
    import streamlit.components.v1 as components

    # Import validate_extraction — lives in both config.py and validate_extraction.py
    try:
        from validate_extraction import validate_extraction
    except ImportError:
        from config import validate_extraction

    report = validate_extraction(file_name, doc_type, raw_extraction)
    failed_fields, warning_fields = _extract_validation_sets(report)

    # Preflight: check file exists on stage
    try:
        stage_check = session.sql(
            f"SELECT RELATIVE_PATH FROM DIRECTORY(@{stage}) "
            f"WHERE RELATIVE_PATH = '{file_name}'"
        ).collect()
        if not stage_check:
            st.warning(
                f"File **{file_name}** not found on stage `@{stage}`. "
                "Re-upload or sync stage files."
            )
            return
    except Exception:
        pass  # DIRECTORY() may fail; let file load raise naturally

    try:
        img, boxes = compute_field_boxes(
            session,
            file_name,
            stage,
            raw_extraction,
            failed_fields,
            warning_fields,
            page_number,
            scale,
        )
    except Exception as e:
        st.error(f"Could not render field highlights: {e}")
        return

    located = sum(1 for b in boxes if b.x >= 0 and b.width > 0)
    total_fields = len(set(b.field_name for b in boxes))

    if mode == "interactive":
        try:
            html = _build_interactive_html(img, boxes, selected_field, viewer_height)
            components.html(html, height=viewer_height + 20, scrolling=True)
        except Exception as e:
            st.warning(
                f"Interactive viewer failed ({e}), falling back to image overlay."
            )
            mode = "image"

    if mode == "image":
        overlay_img = render_pil_overlay(img, boxes, selected_field)
        st.image(overlay_img, use_container_width=True)

        # Field panel below image (since it's not embedded in the HTML)
        with st.expander(
            f"Extracted Fields — {located}/{total_fields} located", expanded=True
        ):
            for b in boxes:
                has_box = b.x >= 0 and b.width > 0
                icon = {"pass": "✅", "fail": "❌", "warn": "⚠️"}.get(b.status, "·")
                loc = "📍" if has_box else "❓"
                st.markdown(
                    f"{icon} **{b.field_name}:** {b.value} {loc}",
                )
