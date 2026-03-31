"""
snip_annotator.py — Snipping tool for PDF field annotation in Streamlit.

Workflow:
  1. Renders the PDF page with a crosshair cursor overlay (HTML/JS)
  2. User draws a rectangle by click-dragging
  3. JS encodes the rectangle coordinates and calls sendPrompt() to pass
     them back to the Streamlit conversation loop
  4. Python extracts text from the snipped region using pypdfium2
  5. User assigns the extracted text to a field name
  6. Correction is saved to INVOICE_REVIEW (append-only audit)

Usage in Document Viewer:
    from snip_annotator import render_snip_mode

    render_snip_mode(
        session=session,
        file_name="invoice_001.pdf",
        stage=STAGE,
        db=DB,
        raw_extraction=raw_dict,
        doc_type="INVOICE",
        record_id=123,
    )
"""

import base64
import io
import json
import os
import tempfile
from typing import Optional

import pypdfium2 as pdfium
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image


# ══════════════════════════════════════════════════════════════════════════════
# PDF TEXT EXTRACTION BY REGION
# ══════════════════════════════════════════════════════════════════════════════

def _load_pdf_bytes(session, file_name: str, stage: str) -> bytes:
    """Load PDF bytes from Snowflake stage."""
    try:
        return session.file.get_stream(f"@{stage}/{file_name}").read()
    except (NotImplementedError, AttributeError):
        pass
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
                raise FileNotFoundError(f"No files for {stage_path}")
        with open(local_path, "rb") as f:
            return f.read()


def extract_text_from_region(
    file_bytes: bytes,
    page_number: int,
    x_norm: float,
    y_norm: float,
    w_norm: float,
    h_norm: float,
) -> str:
    """
    Extract text from a rectangular region of a PDF page.

    Coordinates are normalized (0.0–1.0) relative to the page dimensions.
    This allows the JS overlay (which works in display pixels) to communicate
    regions back to Python regardless of render scale.
    """
    pdf = pdfium.PdfDocument(file_bytes)
    page = pdf[page_number]
    page_w = page.get_width()
    page_h = page.get_height()

    # Convert normalized coords to PDF points
    left = x_norm * page_w
    top = y_norm * page_h
    right = (x_norm + w_norm) * page_w
    bottom = (y_norm + h_norm) * page_h

    # pypdfium2 uses bottom-left origin; page coords have top=0
    # get_text_bounded expects (left, top, right, bottom) in PDF coordinates
    # where top > bottom (PDF y-axis goes up from bottom-left)
    pdf_left = left
    pdf_bottom = page_h - bottom  # convert from top-origin to bottom-origin
    pdf_right = right
    pdf_top = page_h - top

    textpage = page.get_textpage()

    # Try get_text_bounded first
    try:
        text = textpage.get_text_bounded(
            left=pdf_left, bottom=pdf_bottom, right=pdf_right, top=pdf_top
        )
        if text and text.strip():
            pdf.close()
            return text.strip()
    except Exception:
        pass

    # Fallback: iterate characters and check if they're inside the region
    full_text = textpage.get_text_range()
    chars_in_region = []
    for i in range(len(full_text)):
        try:
            box = textpage.get_charbox(i)
            if box and len(box) == 4:
                cx = (box[0] + box[2]) / 2  # char center x
                cy = (box[1] + box[3]) / 2  # char center y (bottom-origin)
                if pdf_left <= cx <= pdf_right and pdf_bottom <= cy <= pdf_top:
                    chars_in_region.append(full_text[i])
        except Exception:
            continue

    pdf.close()
    return "".join(chars_in_region).strip()


# ══════════════════════════════════════════════════════════════════════════════
# SNIP TOOL HTML/JS
# ══════════════════════════════════════════════════════════════════════════════

def _build_snip_html(
    img: Image.Image,
    existing_snips: list[dict],
    input_key: str,
    viewer_height: int = 850,
) -> str:
    """
    Build self-contained HTML with a snipping rectangle tool.

    When the user finishes drawing and clicks Extract Text, JS finds the
    Streamlit text_input identified by `input_key` in the parent DOM,
    sets its value to the normalized coordinates, and dispatches an input
    event to trigger a Streamlit rerun.
    """
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    img_w, img_h = img.size

    max_display_w = 800
    display_scale = min(1.0, max_display_w / img_w)
    display_w = int(img_w * display_scale)
    display_h = int(img_h * display_scale)

    # Render existing snip rectangles
    snip_divs = ""
    for i, snip in enumerate(existing_snips):
        sx = snip["x_norm"] * display_w
        sy = snip["y_norm"] * display_h
        sw = snip["w_norm"] * display_w
        sh = snip["h_norm"] * display_h
        field = snip.get("field", "unassigned")
        color = "#f59e0b" if field == "unassigned" else "#22c55e"
        snip_divs += (
            f'<div class="snip-existing" style="left:{sx:.1f}px;top:{sy:.1f}px;'
            f'width:{sw:.1f}px;height:{sh:.1f}px;border-color:{color};">'
            f'<span class="snip-label" style="background:{color};">{field}</span></div>'
        )

    # The JS uses the input_key to locate the Streamlit text_input in the parent
    # frame's DOM. Streamlit renders inputs with aria-label matching the label text.
    escaped_key = input_key.replace("'", "\\'")

    return f"""<!DOCTYPE html>
<html><head><style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:transparent}}
.snip-container{{position:relative;width:{display_w}px;margin:0 auto;
    border:2px solid #3b82f6;border-radius:8px;overflow:hidden;
    background:#f8fafc;cursor:crosshair;user-select:none}}
.snip-container img{{width:{display_w}px;height:{display_h}px;display:block;
    pointer-events:none;user-select:none;-webkit-user-drag:none}}
.snip-rect{{position:absolute;border:2px dashed #3b82f6;background:rgba(59,130,246,0.12);
    pointer-events:none;z-index:10;display:none}}
.snip-existing{{position:absolute;border:2px solid;background:rgba(0,0,0,0.05);
    pointer-events:none;z-index:5;border-radius:2px}}
.snip-label{{position:absolute;top:-18px;left:-1px;font-size:9px;font-weight:600;
    color:#fff;padding:1px 5px;border-radius:3px 3px 0 0;white-space:nowrap}}
.snip-instructions{{padding:8px 12px;background:#eff6ff;border-bottom:1px solid #bfdbfe;
    font-size:12px;color:#1e40af;text-align:center}}
.snip-confirm{{position:absolute;z-index:20;background:#fff;border:1px solid #d1d5db;
    border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.15);padding:12px;
    display:none;min-width:220px}}
.snip-confirm button{{padding:6px 16px;border:none;border-radius:4px;cursor:pointer;
    font-size:12px;font-weight:600;margin:4px}}
.snip-confirm .btn-confirm{{background:#3b82f6;color:#fff}}
.snip-confirm .btn-confirm:hover{{background:#2563eb}}
.snip-confirm .btn-cancel{{background:#f1f5f9;color:#475569}}
.snip-confirm .btn-cancel:hover{{background:#e2e8f0}}
.snip-confirm .coords{{font-size:10px;color:#94a3b8;margin-top:4px;
    font-family:monospace;user-select:all}}
.snip-status{{padding:6px 12px;font-size:11px;text-align:center;display:none}}
.snip-status.success{{background:#f0fdf4;color:#166534;display:block}}
.snip-status.error{{background:#fef2f2;color:#991b1b;display:block}}
</style></head><body>

<div class="snip-instructions">
    Click and drag to select a region. Release to confirm.
</div>

<div class="snip-container" id="snipContainer">
    <img src="data:image/png;base64,{b64}" alt="Document" />
    <div class="snip-rect" id="snipRect"></div>
    {snip_divs}
    <div class="snip-confirm" id="snipConfirm">
        <div style="font-size:13px;font-weight:600;color:#1e293b;margin-bottom:8px;">
            Region Selected</div>
        <div id="snipPreview" style="font-size:11px;color:#475569;margin-bottom:8px;"></div>
        <div>
            <button class="btn-confirm" onclick="confirmSnip()">Extract Text</button>
            <button class="btn-cancel" onclick="cancelSnip()">Cancel</button>
        </div>
        <div class="coords" id="snipCoords"></div>
    </div>
</div>
<div class="snip-status" id="snipStatus"></div>

<script>
(function() {{
    var container = document.getElementById('snipContainer');
    var rect = document.getElementById('snipRect');
    var confirmEl = document.getElementById('snipConfirm');
    var coordsEl = document.getElementById('snipCoords');
    var previewEl = document.getElementById('snipPreview');
    var statusEl = document.getElementById('snipStatus');

    var drawing = false;
    var startX = 0, startY = 0;
    var endX = 0, endY = 0;
    var imgW = {display_w};
    var imgH = {display_h};

    container.addEventListener('mousedown', function(e) {{
        if (e.target.closest('.snip-confirm')) return;
        var r = container.getBoundingClientRect();
        startX = e.clientX - r.left;
        startY = e.clientY - r.top;
        drawing = true;
        confirmEl.style.display = 'none';
        statusEl.style.display = 'none';
        rect.style.display = 'block';
        rect.style.borderColor = '#3b82f6';
        rect.style.background = 'rgba(59,130,246,0.12)';
        rect.style.left = startX + 'px';
        rect.style.top = startY + 'px';
        rect.style.width = '0px';
        rect.style.height = '0px';
    }});

    container.addEventListener('mousemove', function(e) {{
        if (!drawing) return;
        var r = container.getBoundingClientRect();
        endX = Math.min(Math.max(e.clientX - r.left, 0), imgW);
        endY = Math.min(Math.max(e.clientY - r.top, 0), imgH);
        var x = Math.min(startX, endX);
        var y = Math.min(startY, endY);
        var w = Math.abs(endX - startX);
        var h = Math.abs(endY - startY);
        rect.style.left = x + 'px';
        rect.style.top = y + 'px';
        rect.style.width = w + 'px';
        rect.style.height = h + 'px';
    }});

    container.addEventListener('mouseup', function(e) {{
        if (!drawing) return;
        drawing = false;
        var r = container.getBoundingClientRect();
        endX = Math.min(Math.max(e.clientX - r.left, 0), imgW);
        endY = Math.min(Math.max(e.clientY - r.top, 0), imgH);

        var x = Math.min(startX, endX);
        var y = Math.min(startY, endY);
        var w = Math.abs(endX - startX);
        var h = Math.abs(endY - startY);

        if (w < 5 || h < 5) {{
            rect.style.display = 'none';
            return;
        }}

        confirmEl.style.left = Math.min(x + w + 5, imgW - 240) + 'px';
        confirmEl.style.top = Math.max(y - 10, 0) + 'px';
        confirmEl.style.display = 'block';

        var xn = (x / imgW).toFixed(4);
        var yn = (y / imgH).toFixed(4);
        var wn = (w / imgW).toFixed(4);
        var hn = (h / imgH).toFixed(4);
        coordsEl.textContent = xn + ', ' + yn + ', ' + wn + ', ' + hn;
        previewEl.textContent = Math.round(w) + ' x ' + Math.round(h) + ' px region';

        container.dataset.snipX = xn;
        container.dataset.snipY = yn;
        container.dataset.snipW = wn;
        container.dataset.snipH = hn;
    }});

    function findStreamlitInput(label) {{
        // Strategy 1: search parent document for input with matching aria-label
        try {{
            var parent = window.parent.document;
            var inputs = parent.querySelectorAll('input[aria-label]');
            for (var i = 0; i < inputs.length; i++) {{
                if (inputs[i].getAttribute('aria-label') === label) {{
                    return inputs[i];
                }}
            }}
            // Strategy 2: search by placeholder text
            inputs = parent.querySelectorAll('input[placeholder]');
            for (var i = 0; i < inputs.length; i++) {{
                if (inputs[i].getAttribute('placeholder').indexOf('SNIP_COORDS') >= 0) {{
                    return inputs[i];
                }}
            }}
        }} catch(e) {{
            // Cross-origin blocked
        }}
        return null;
    }}

    function setNativeValue(el, val) {{
        // React overrides input.value setter, so we need the native setter
        var nativeSetter = Object.getOwnPropertyDescriptor(
            window.parent.HTMLInputElement.prototype, 'value'
        ).set;
        nativeSetter.call(el, val);
        el.dispatchEvent(new window.parent.Event('input', {{ bubbles: true }}));
        el.dispatchEvent(new window.parent.Event('change', {{ bubbles: true }}));
    }}

    window.confirmSnip = function() {{
        var xn = container.dataset.snipX;
        var yn = container.dataset.snipY;
        var wn = container.dataset.snipW;
        var hn = container.dataset.snipH;
        var coordStr = xn + ',' + yn + ',' + wn + ',' + hn;

        var input = findStreamlitInput('{escaped_key}');
        if (input) {{
            setNativeValue(input, coordStr);
            confirmEl.style.display = 'none';
            rect.style.borderColor = '#22c55e';
            rect.style.background = 'rgba(34,197,94,0.15)';
            statusEl.className = 'snip-status success';
            statusEl.textContent = 'Coordinates sent! Processing...';
        }} else {{
            // Input not found — show coords for manual copy
            confirmEl.style.display = 'none';
            rect.style.borderColor = '#f59e0b';
            rect.style.background = 'rgba(245,158,11,0.15)';
            statusEl.className = 'snip-status error';
            statusEl.innerHTML = 'Auto-capture unavailable. Copy these coordinates:<br>' +
                '<strong style="font-family:monospace;user-select:all;">' + coordStr + '</strong>' +
                '<br>Paste them into the coordinate input above and click Extract.';
        }}
    }};

    window.cancelSnip = function() {{
        confirmEl.style.display = 'none';
        rect.style.display = 'none';
    }};
}})();
</script>
</body></html>"""


# ══════════════════════════════════════════════════════════════════════════════
# STREAMLIT SNIP MODE RENDERER
# ══════════════════════════════════════════════════════════════════════════════

def render_snip_mode(
    session,
    file_name: str,
    stage: str,
    db: str,
    raw_extraction: dict,
    doc_type: str = "INVOICE",
    record_id: int = None,
    page_number: int = 0,
    scale: float = 2.0,
    viewer_height: int = 850,
):
    """
    Render the snipping annotation tool for a PDF document.

    Two-phase interaction:
      Phase 1: Show snip tool → user draws rectangle → JS sends coords via chat
      Phase 2: Parse coords from session state → extract text → assign to field → save

    Args:
        session:         Snowpark session
        file_name:       PDF file name on stage
        stage:           Full stage path
        db:              Database.schema prefix
        raw_extraction:  Dict of current extracted fields
        doc_type:        Document type
        record_id:       Record ID for writeback (from EXTRACTED_FIELDS)
        page_number:     PDF page index
        scale:           Render scale
        viewer_height:   Viewer height in px
    """

    # ── Session state keys ────────────────────────────────────────────────
    ss_key = f"snip_{file_name}_{page_number}"
    if f"{ss_key}_coords" not in st.session_state:
        st.session_state[f"{ss_key}_coords"] = None
    if f"{ss_key}_text" not in st.session_state:
        st.session_state[f"{ss_key}_text"] = None
    if f"{ss_key}_history" not in st.session_state:
        st.session_state[f"{ss_key}_history"] = []
    if f"{ss_key}_save_result" not in st.session_state:
        st.session_state[f"{ss_key}_save_result"] = None
    if f"{ss_key}_gen" not in st.session_state:
        st.session_state[f"{ss_key}_gen"] = 0

    # ── Show save result ──────────────────────────────────────────────────
    if st.session_state[f"{ss_key}_save_result"]:
        result = st.session_state[f"{ss_key}_save_result"]
        st.success(f"Saved correction: **{result['field']}** = \"{result['value']}\"")
        if st.button("Continue Annotating", key=f"{ss_key}_continue"):
            st.session_state[f"{ss_key}_save_result"] = None
            st.rerun()

    # ── Coordinate receiver (JS writes to this input) ───────────────────
    # This text_input serves as the JS→Python bridge. The HTML component
    # finds it by aria-label in the parent DOM and sets its value when
    # the user clicks "Extract Text" after drawing a rectangle.
    # The generation counter (_gen) forces a fresh widget after each reset,
    # avoiding Streamlit's "cannot modify after instantiation" error.
    coord_label = f"{ss_key}_coord_bridge"
    bridge_key = f"{ss_key}_bridge_{st.session_state[f'{ss_key}_gen']}"

    coord_value = st.text_input(
        coord_label,
        value="",
        placeholder="SNIP_COORDS — draw a rectangle above, or paste: x,y,w,h",
        key=bridge_key,
        label_visibility="collapsed",
    )

    # Parse coordinates if the input has a value (either from JS or manual paste)
    if coord_value and coord_value.strip() and st.session_state[f"{ss_key}_coords"] is None:
        try:
            parts = [float(p.strip()) for p in coord_value.replace("SNIP:", "").split(",")]
            if len(parts) == 4 and all(0 <= p <= 1.5 for p in parts):
                st.session_state[f"{ss_key}_coords"] = {
                    "x": parts[0], "y": parts[1],
                    "w": parts[2], "h": parts[3],
                }
                st.rerun()
        except (ValueError, TypeError):
            pass  # Not valid coords yet — user may still be typing

    # ── Render the PDF with snip overlay ──────────────────────────────────
    try:
        file_bytes = _load_pdf_bytes(session, file_name, stage)
        pdf = pdfium.PdfDocument(file_bytes)
        page = pdf[page_number]
        bitmap = page.render(scale=scale)
        img = bitmap.to_pil()
        pdf.close()
    except Exception as e:
        st.error(f"Could not load PDF: {e}")
        return

    existing_snips = st.session_state[f"{ss_key}_history"]
    html = _build_snip_html(img, existing_snips, coord_label, viewer_height)
    components.html(html, height=viewer_height + 50, scrolling=True)

    # ── Phase 2: Process snipped region ───────────────────────────────────
    coords = st.session_state[f"{ss_key}_coords"]

    if coords:
        st.divider()
        st.markdown("##### Snipped Region")

        x_n, y_n, w_n, h_n = coords["x"], coords["y"], coords["w"], coords["h"]
        st.caption(f"Region: ({x_n:.4f}, {y_n:.4f}) — {w_n:.4f} × {h_n:.4f}")

        # Extract text from region
        if st.session_state[f"{ss_key}_text"] is None:
            with st.spinner("Extracting text from selected region..."):
                try:
                    file_bytes = _load_pdf_bytes(session, file_name, stage)
                    extracted = extract_text_from_region(
                        file_bytes, page_number, x_n, y_n, w_n, h_n,
                    )
                    st.session_state[f"{ss_key}_text"] = extracted
                except Exception as e:
                    st.error(f"Text extraction failed: {e}")
                    st.session_state[f"{ss_key}_text"] = ""

        extracted_text = st.session_state[f"{ss_key}_text"]

        if extracted_text:
            st.text_area(
                "Extracted Text (editable)",
                value=extracted_text,
                height=80,
                key=f"{ss_key}_edit_text",
            )

            # ── Auto-map: spatial proximity + pattern matching ─────
            skip = {"_confidence", "_validation_warnings"}
            fields_raw = {k: v for k, v in raw_extraction.items() if k not in skip}

            # Get validation failures
            try:
                from validate_extraction import validate_extraction as _ve
                report = _ve(file_name, doc_type, raw_extraction)
                failed_set = set()
                for failure in report.failures:
                    parts = failure.split(": ", 1)
                    if len(parts) > 1:
                        failed_set.add(parts[1].split(" ")[0])
            except Exception:
                failed_set = set()

            # Classify each field
            field_status = {}
            for fname, val in fields_raw.items():
                is_empty = val is None or str(val).strip().lower() in ("", "null", "none")
                if is_empty:
                    field_status[fname] = "missing"
                elif fname in failed_set:
                    field_status[fname] = "failed"
                else:
                    field_status[fname] = "ok"

            # ── Spatial proximity scoring ──────────────────────────
            snip_cx = x_n + w_n / 2
            snip_cy = y_n + h_n / 2
            spatial_scores = {}

            try:
                fb_search = _load_pdf_bytes(session, file_name, stage)
                pdf_s = pdfium.PdfDocument(fb_search)
                page_s = pdf_s[page_number]
                pw = page_s.get_width()
                ph = page_s.get_height()
                tp_s = page_s.get_textpage()

                for fname in fields_raw:
                    label_text = fname.replace("_", " ").title()
                    variants = [label_text, label_text.upper(), fname, label_text.rstrip("s")]
                    best_dist = 999.0
                    for variant in variants:
                        if len(variant) < 2:
                            continue
                        try:
                            searcher = tp_s.search(variant, match_case=False, match_whole_word=False)
                            if searcher and searcher.get_next():
                                idx_s = searcher.get_charindex()
                                cnt = searcher.get_result_count()
                                if idx_s >= 0 and cnt > 0:
                                    cbs = []
                                    for ci in range(cnt):
                                        try:
                                            b = tp_s.get_charbox(idx_s + ci)
                                            if b and len(b) == 4:
                                                cbs.append(b)
                                        except Exception:
                                            pass
                                    if cbs:
                                        lcx = ((min(b[0] for b in cbs) + max(b[2] for b in cbs)) / 2) / pw
                                        lcy = 1.0 - ((min(b[1] for b in cbs) + max(b[3] for b in cbs)) / 2) / ph
                                        d = ((snip_cx - lcx) ** 2 + (snip_cy - lcy) ** 2) ** 0.5
                                        if d < best_dist:
                                            best_dist = d
                        except Exception:
                            continue
                    spatial_scores[fname] = best_dist
                pdf_s.close()
            except Exception:
                for fname in fields_raw:
                    spatial_scores[fname] = 1.0

            # ── Pattern matching scoring ──────────────────────────
            import re as _re
            _DATE_PAT = _re.compile(
                r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})|(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})|'
                r'([A-Z][a-z]{2,8}\s+\d{1,2},?\s+\d{4})'
            )
            _NUMBER_PAT = _re.compile(r'^[\$\s]*[\d,]+\.?\d*$')
            _CURRENCY_PAT = _re.compile(r'[\$\u00a3\u20ac]|^\d[\d,]*\.\d{2}$')

            try:
                from validate_extraction import FIELD_TYPE_MAP
            except ImportError:
                try:
                    from config import FIELD_TYPE_MAP
                except ImportError:
                    FIELD_TYPE_MAP = {}

            txt = extracted_text.strip()
            is_date = bool(_DATE_PAT.search(txt))
            is_num = bool(_NUMBER_PAT.match(txt.replace("$", "").replace(",", "").strip()))
            is_cur = bool(_CURRENCY_PAT.search(txt))

            pattern_scores = {}
            for fname in fields_raw:
                etype = FIELD_TYPE_MAP.get(fname, "VARCHAR")
                if etype == "DATE" and is_date:
                    pattern_scores[fname] = 0.0
                elif etype == "NUMBER" and (is_num or is_cur):
                    pattern_scores[fname] = 0.0
                elif etype == "VARCHAR" and not is_date and not is_num:
                    pattern_scores[fname] = 0.1
                elif etype in ("DATE", "NUMBER"):
                    pattern_scores[fname] = 0.8
                else:
                    pattern_scores[fname] = 0.5

            # ── Combined scoring ──────────────────────────────────
            candidates = []
            for fname in fields_raw:
                st_val = field_status[fname]
                bonus = {"missing": -0.5, "failed": -0.3, "ok": 0.0}[st_val]
                sp = spatial_scores.get(fname, 1.0)
                pt = pattern_scores.get(fname, 0.5)
                score = (sp * 0.60) + (pt * 0.25) + (bonus * 0.15 + 0.15)
                candidates.append({
                    "field": fname, "score": round(score, 4),
                    "spatial": round(sp, 4), "pattern": round(pt, 2),
                    "status": st_val,
                    "current": str(fields_raw.get(fname, ""))[:40],
                })
            candidates.sort(key=lambda c: c["score"])

            # ── Display ranked candidates ─────────────────────────
            st.markdown("##### Auto-Mapped Field")
            top = candidates[0]
            confidence = max(0, min(1, 1.0 - top["score"]))
            badge_map = {"missing": "\U0001f534 MISSING", "failed": "\U0001f7e1 FAILED", "ok": "\u2705 OK"}
            icon_map = {"missing": "\U0001f534", "failed": "\U0001f7e1", "ok": "\u2705"}

            st.success(
                f"**Best match: {top['field']}** ({badge_map[top['status']]}) \u2014 "
                f"confidence: {confidence:.0%}"
            )

            display_options = []
            for c in candidates:
                conf = max(0, min(1, 1.0 - c["score"]))
                cur = f' \u2014 "{c["current"]}"' if c["current"] and c["status"] == "ok" else ""
                display_options.append(
                    f'{icon_map[c["status"]]} {c["field"]} ({badge_map[c["status"]]}, {conf:.0%}{cur})'
                )
            field_lookup = {opt: candidates[i]["field"] for i, opt in enumerate(display_options)}

            assign_label = st.selectbox(
                "Confirm field assignment",
                display_options,
                index=0,
                key=f"{ss_key}_assign",
            )
            assign_field = field_lookup[assign_label]

            with st.expander("Match reasoning", expanded=False):
                import pandas as pd
                top_5 = candidates[:min(5, len(candidates))]
                reason_df = pd.DataFrame(top_5)
                reason_df.columns = ["Field", "Score", "Spatial", "Pattern", "Status", "Current"]
                st.dataframe(reason_df, hide_index=True, use_container_width=True)
                st.caption(
                    "Score = spatial distance (60%) + type pattern (25%) + "
                    "priority bonus (15%). Lower = better match."
                )

            current_val = raw_extraction.get(assign_field)
            fst = field_status.get(assign_field, "ok")
            if fst == "missing":
                st.info(f"**{assign_field}** is currently empty \u2014 this snip will fill it.")
            elif fst == "failed":
                st.warning(f"**{assign_field}** failed validation. Current: \"{current_val}\"")
            elif current_val:
                st.caption(f"Current value: **{current_val}** \u2014 this snip will overwrite it.")

            # Get the edited text (user may have corrected it)
            final_text = st.session_state.get(f"{ss_key}_edit_text", extracted_text)

            ac1, ac2, ac3 = st.columns(3)

            with ac1:
                if st.button("Save Correction", type="primary", key=f"{ss_key}_save"):
                    corrections = {assign_field: final_text}
                    corrections_json = json.dumps(corrections)

                    try:
                        # 1. Append correction to INVOICE_REVIEW (audit trail)
                        session.sql(
                            f"""
                            INSERT INTO {db}.INVOICE_REVIEW (
                                record_id, file_name, review_status,
                                reviewer_notes, corrections
                            ) SELECT ?, ?, 'CORRECTED',
                                     ?, PARSE_JSON(?)
                            """,
                            params=[
                                int(record_id) if record_id else 0,
                                str(file_name),
                                f"Snip annotation: {assign_field} corrected",
                                corrections_json,
                            ],
                        ).collect()

                        # 2. Update raw_extraction in EXTRACTED_FIELDS so Document
                        #    Viewer (which reads raw_extraction directly) shows the
                        #    correction immediately without requiring the view.
                        try:
                            session.sql(
                                f"""
                                UPDATE {db}.EXTRACTED_FIELDS
                                SET raw_extraction = OBJECT_INSERT(
                                    COALESCE(raw_extraction, OBJECT_CONSTRUCT()),
                                    ?, ?, TRUE
                                )
                                WHERE record_id = ?
                                """,
                                params=[
                                    str(assign_field),
                                    str(final_text),
                                    int(record_id) if record_id else 0,
                                ],
                            ).collect()
                        except Exception:
                            pass  # Non-fatal: view will still pick it up from INVOICE_REVIEW

                        st.session_state[f"{ss_key}_history"].append({
                            "x_norm": x_n, "y_norm": y_n,
                            "w_norm": w_n, "h_norm": h_n,
                            "field": assign_field,
                            "value": final_text,
                        })

                        st.session_state[f"{ss_key}_save_result"] = {
                            "field": assign_field, "value": final_text,
                        }
                        st.session_state[f"{ss_key}_coords"] = None
                        st.session_state[f"{ss_key}_text"] = None
                        st.session_state[f"{ss_key}_gen"] += 1
                        st.rerun()

                    except Exception as e:
                        st.error(f"Could not save correction: {e}")

            with ac2:
                if st.button("Snip Another Region", key=f"{ss_key}_another"):
                    st.session_state[f"{ss_key}_coords"] = None
                    st.session_state[f"{ss_key}_text"] = None
                    st.session_state[f"{ss_key}_gen"] += 1
                    st.rerun()

            with ac3:
                if st.button("Cancel", key=f"{ss_key}_cancel"):
                    st.session_state[f"{ss_key}_coords"] = None
                    st.session_state[f"{ss_key}_text"] = None
                    st.session_state[f"{ss_key}_gen"] += 1
                    st.rerun()

        else:
            st.warning("No text found in the selected region. Try selecting a larger area.")
            if st.button("Try Again", key=f"{ss_key}_retry"):
                st.session_state[f"{ss_key}_coords"] = None
                st.session_state[f"{ss_key}_text"] = None
                st.rerun()

    # ── Annotation history ────────────────────────────────────────────────
    history = st.session_state[f"{ss_key}_history"]
    if history:
        st.divider()
        st.markdown("##### Annotation History (this session)")
        import pandas as pd
        hist_df = pd.DataFrame([
            {"Field": s["field"], "Value": s["value"][:50],
             "Region": f"({s['x_norm']:.3f}, {s['y_norm']:.3f})"}
            for s in history
        ])
        st.dataframe(hist_df, hide_index=True, use_container_width=True)
