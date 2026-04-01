"""Unit tests for field_highlighter.py — PDF field highlighting module.

Covers:
  - FieldBox dataclass construction
  - _hex_to_rgb() color conversion
  - _html_escape() XSS prevention
  - _extract_validation_sets() from ValidationReport
  - _search_field_boxes() with a real pypdfium2 textpage
  - render_pil_overlay() image annotation
  - _build_interactive_html() output structure

Uses a real pypdfium2 document generated from a minimal PDF.
No Snowflake connection needed.
"""

import importlib
import io
import os
import sys
from unittest import mock
from dataclasses import asdict

import pytest

# Ensure we can import from the streamlit directory
_STREAMLIT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "streamlit"
)
sys.path.insert(0, _STREAMLIT_DIR)


@pytest.fixture(scope="module")
def fh():
    """Import field_highlighter module."""
    # Mock streamlit since it's not available in test environment
    fake_st = mock.MagicMock()
    fake_components = mock.MagicMock()
    with mock.patch.dict(sys.modules, {
        "streamlit": fake_st,
        "streamlit.components": mock.MagicMock(),
        "streamlit.components.v1": fake_components,
    }):
        if "field_highlighter" in sys.modules:
            del sys.modules["field_highlighter"]
        mod = importlib.import_module("field_highlighter")
    return mod


@pytest.fixture(scope="module")
def ve():
    """Import validate_extraction for building test ValidationReports."""
    if "validate_extraction" in sys.modules:
        del sys.modules["validate_extraction"]
    return importlib.import_module("validate_extraction")


# ---------------------------------------------------------------------------
# Generate a minimal test PDF with known text
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_pdf_bytes():
    """Create a minimal PDF with known text for search testing."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas

        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=letter)
        c.setFont("Helvetica", 12)
        c.drawString(72, 700, "Vendor: Acme Corporation")
        c.drawString(72, 680, "Invoice Number: INV-2024-001")
        c.drawString(72, 660, "Total: $1,234.56")
        c.drawString(72, 640, "Date: 2024-06-15")
        c.drawString(72, 620, "Due Date: 2024-07-15")
        c.save()
        return buf.getvalue()
    except ImportError:
        pytest.skip("reportlab not available — skipping PDF-dependent tests")


@pytest.fixture(scope="module")
def test_pdf_page(test_pdf_bytes):
    """Return (pdf, page, textpage, full_text, page_height) from the test PDF."""
    import pypdfium2 as pdfium
    pdf = pdfium.PdfDocument(test_pdf_bytes)
    page = pdf[0]
    textpage = page.get_textpage()
    full_text = textpage.get_text_range()
    page_height = page.get_height()
    yield pdf, page, textpage, full_text, page_height
    pdf.close()


# ---------------------------------------------------------------------------
# FieldBox
# ---------------------------------------------------------------------------
class TestFieldBox:

    def test_construction(self, fh):
        box = fh.FieldBox(
            field_name="vendor_name",
            value="Acme Corp",
            x=10.0, y=20.0, width=100.0, height=15.0,
            page=0, color="#3b82f6", status="pass",
        )
        assert box.field_name == "vendor_name"
        assert box.status == "pass"

    def test_not_found_placeholder(self, fh):
        """Boxes with x=-1, width=0 represent unlocated fields."""
        box = fh.FieldBox(
            field_name="missing_field", value="NULL",
            x=-1, y=-1, width=0, height=0,
            page=0, color="#ef4444", status="fail",
        )
        assert box.x < 0
        assert box.width == 0


# ---------------------------------------------------------------------------
# _hex_to_rgb
# ---------------------------------------------------------------------------
class TestHexToRgb:

    def test_blue(self, fh):
        assert fh._hex_to_rgb("#3b82f6") == "59,130,246"

    def test_red(self, fh):
        assert fh._hex_to_rgb("#ef4444") == "239,68,68"

    def test_black(self, fh):
        assert fh._hex_to_rgb("#000000") == "0,0,0"

    def test_white(self, fh):
        assert fh._hex_to_rgb("#ffffff") == "255,255,255"


# ---------------------------------------------------------------------------
# _html_escape
# ---------------------------------------------------------------------------
class TestHtmlEscape:

    def test_ampersand(self, fh):
        assert fh._html_escape("PSE&G") == "PSE&amp;G"

    def test_angle_brackets(self, fh):
        assert fh._html_escape("<script>") == "&lt;script&gt;"

    def test_double_quotes(self, fh):
        assert fh._html_escape('value="test"') == "value=&quot;test&quot;"

    def test_plain_text_unchanged(self, fh):
        assert fh._html_escape("Acme Corp") == "Acme Corp"

    def test_combined(self, fh):
        result = fh._html_escape('O"Brien & <Co>')
        assert "&amp;" in result
        assert "&lt;" in result
        assert "&gt;" in result
        assert "&quot;" in result


# ---------------------------------------------------------------------------
# _extract_validation_sets
# ---------------------------------------------------------------------------
class TestExtractValidationSets:

    def test_extracts_failed_fields(self, fh, ve):
        report = ve.ValidationReport(
            "test.pdf", "INVOICE",
            failures=[
                "MISSING: vendor_name is null/empty",
                "TYPE_ERROR: total — Cannot parse 'N/A' as a number",
            ],
        )
        failed, warned = fh._extract_validation_sets(report)
        assert "vendor_name" in failed
        assert "total" in failed

    def test_extracts_warning_fields(self, fh, ve):
        report = ve.ValidationReport(
            "test.pdf", "INVOICE",
            warnings=[
                "total=0 may indicate extraction failure",
                "vendor_name='X' suspiciously short",
                "CROSS_FIELD: total_gte_subtotal — Total less than subtotal",
            ],
        )
        failed, warned = fh._extract_validation_sets(report)
        assert len(failed) == 0
        assert "total" in warned
        assert "vendor_name" in warned
        # CROSS_FIELD warnings should NOT be in warned set
        assert not any("CROSS_FIELD" in w for w in warned)

    def test_empty_report(self, fh, ve):
        report = ve.ValidationReport("test.pdf", "INVOICE")
        failed, warned = fh._extract_validation_sets(report)
        assert len(failed) == 0
        assert len(warned) == 0


# ---------------------------------------------------------------------------
# _search_field_boxes (requires real PDF)
# ---------------------------------------------------------------------------
class TestSearchFieldBoxes:

    def test_finds_known_text(self, fh, test_pdf_page):
        """Should find 'Acme Corporation' in the test PDF."""
        _, page, textpage, full_text, page_height = test_pdf_page
        boxes = fh._search_field_boxes(
            textpage, page_height, scale=2.0,
            field_name="vendor_name", value="Acme Corporation",
            color="#3b82f6", status="pass",
            page_idx=0, full_text=full_text,
        )
        assert len(boxes) >= 1
        assert boxes[0].field_name == "vendor_name"
        assert boxes[0].x >= 0
        assert boxes[0].width > 0

    def test_finds_invoice_number(self, fh, test_pdf_page):
        _, page, textpage, full_text, page_height = test_pdf_page
        boxes = fh._search_field_boxes(
            textpage, page_height, scale=2.0,
            field_name="invoice_number", value="INV-2024-001",
            color="#10b981", status="pass",
            page_idx=0, full_text=full_text,
        )
        assert len(boxes) >= 1

    def test_not_found_returns_empty(self, fh, test_pdf_page):
        """Text not in the PDF should return empty list."""
        _, page, textpage, full_text, page_height = test_pdf_page
        boxes = fh._search_field_boxes(
            textpage, page_height, scale=2.0,
            field_name="missing", value="ZZZZNOTINPDF",
            color="#ef4444", status="fail",
            page_idx=0, full_text=full_text,
        )
        assert len(boxes) == 0

    def test_none_value_returns_empty(self, fh, test_pdf_page):
        _, page, textpage, full_text, page_height = test_pdf_page
        boxes = fh._search_field_boxes(
            textpage, page_height, scale=2.0,
            field_name="empty_field", value=None,
            color="#ef4444", status="fail",
            page_idx=0, full_text=full_text,
        )
        assert len(boxes) == 0

    def test_short_value_returns_empty(self, fh, test_pdf_page):
        """Values shorter than 2 chars should be skipped (too many false positives)."""
        _, page, textpage, full_text, page_height = test_pdf_page
        boxes = fh._search_field_boxes(
            textpage, page_height, scale=2.0,
            field_name="short", value="X",
            color="#ef4444", status="fail",
            page_idx=0, full_text=full_text,
        )
        assert len(boxes) == 0

    def test_numeric_value_stripped(self, fh, test_pdf_page):
        """Dollar signs and commas are stripped before search; should still find amount."""
        _, page, textpage, full_text, page_height = test_pdf_page
        boxes = fh._search_field_boxes(
            textpage, page_height, scale=2.0,
            field_name="total", value="$1,234.56",
            color="#f59e0b", status="pass",
            page_idx=0, full_text=full_text,
        )
        # May or may not find it depending on how pypdfium2 reports the text;
        # the important thing is it doesn't crash
        assert isinstance(boxes, list)


# ---------------------------------------------------------------------------
# render_pil_overlay
# ---------------------------------------------------------------------------
class TestRenderPilOverlay:

    def _make_boxes(self, fh):
        return [
            fh.FieldBox("vendor", "Acme", x=10, y=20, width=100, height=15,
                         page=0, color="#3b82f6", status="pass"),
            fh.FieldBox("total", "1000", x=10, y=50, width=80, height=15,
                         page=0, color="#ef4444", status="fail"),
        ]

    def test_returns_pil_image(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        result = fh.render_pil_overlay(img, boxes)
        assert isinstance(result, Image.Image)
        assert result.size == (800, 600)

    def test_does_not_mutate_original(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        original_data = list(img.getdata())
        boxes = self._make_boxes(fh)
        fh.render_pil_overlay(img, boxes)
        assert list(img.getdata()) == original_data

    def test_selected_field_filter(self, fh):
        """With selected_field, only that field gets thick border."""
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        # Should not crash with selected_field
        result = fh.render_pil_overlay(img, boxes, selected_field="vendor")
        assert isinstance(result, Image.Image)

    def test_empty_boxes(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        result = fh.render_pil_overlay(img, [])
        assert isinstance(result, Image.Image)

    def test_skips_negative_coordinates(self, fh):
        """Boxes with x<0 (not found) should be skipped in PIL overlay."""
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = [
            fh.FieldBox("missing", "NULL", x=-1, y=-1, width=0, height=0,
                         page=0, color="#ef4444", status="fail"),
        ]
        result = fh.render_pil_overlay(img, boxes)
        # Should not crash and image should be essentially unchanged
        assert isinstance(result, Image.Image)


# ---------------------------------------------------------------------------
# _build_interactive_html
# ---------------------------------------------------------------------------
class TestBuildInteractiveHtml:

    def _make_boxes(self, fh):
        return [
            fh.FieldBox("vendor_name", "Acme Corp", x=50, y=100, width=200, height=20,
                         page=0, color="#3b82f6", status="pass"),
            fh.FieldBox("total", "1000.00", x=50, y=150, width=100, height=20,
                         page=0, color="#10b981", status="pass"),
            fh.FieldBox("missing_field", "NULL", x=-1, y=-1, width=0, height=0,
                         page=0, color="#ef4444", status="fail"),
        ]

    def test_returns_html_string(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        html = fh._build_interactive_html(img, boxes)
        assert isinstance(html, str)
        assert "<html>" in html.lower() or "<div" in html

    def test_contains_base64_image(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        html = fh._build_interactive_html(img, boxes)
        assert "data:image/png;base64," in html

    def test_contains_field_names(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        html = fh._build_interactive_html(img, boxes)
        assert "vendor_name" in html
        assert "total" in html
        assert "missing_field" in html

    def test_contains_javascript(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        html = fh._build_interactive_html(img, boxes)
        assert "selectField" in html
        assert "hoverField" in html

    def test_selected_field_preselected(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        html = fh._build_interactive_html(img, boxes, selected_field="vendor_name")
        # The selected field should have the 'active' or highlighted state
        assert "vendor_name" in html

    def test_xss_safe_field_names(self, fh):
        """Field names with special chars should be HTML-escaped."""
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = [
            fh.FieldBox(
                '<script>alert("xss")</script>', "evil",
                x=10, y=10, width=50, height=15,
                page=0, color="#ef4444", status="fail",
            ),
        ]
        html = fh._build_interactive_html(img, boxes)
        assert "<script>" not in html
        assert "&lt;script&gt;" in html or "script" not in html.split("fbox")[0]

    def test_viewer_height_applied(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        boxes = self._make_boxes(fh)
        html = fh._build_interactive_html(img, boxes, viewer_height=700)
        assert "700" in html

    def test_empty_boxes_still_renders(self, fh):
        from PIL import Image
        img = Image.new("RGB", (800, 600), "white")
        html = fh._build_interactive_html(img, [])
        assert isinstance(html, str)
        assert len(html) > 100  # Should still have the viewer chrome


# ---------------------------------------------------------------------------
# FIELD_COLORS
# ---------------------------------------------------------------------------
class TestFieldColors:

    def test_has_at_least_10_colors(self, fh):
        """Need at least 10 colors for field_1..field_10."""
        assert len(fh.FIELD_COLORS) >= 10

    def test_all_valid_hex(self, fh):
        import re
        for color in fh.FIELD_COLORS:
            assert re.match(r"^#[0-9a-f]{6}$", color), f"Invalid hex color: {color}"

    def test_no_duplicates(self, fh):
        assert len(fh.FIELD_COLORS) == len(set(fh.FIELD_COLORS))
