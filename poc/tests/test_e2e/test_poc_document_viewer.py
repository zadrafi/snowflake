"""E2E: POC Document Viewer — document list, filters, drill-down detail."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, assert_no_exceptions


pytestmark = pytest.mark.e2e

VIEWER_PATH = "/Document_Viewer"


def _navigate(page, app_url):
    """Navigate to the Document Viewer page with retry."""
    for attempt in range(3):
        page.goto(f"{app_url}{VIEWER_PATH}", wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        if page.locator('[data-testid="stDataFrame"]').count() > 0:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


class TestDocumentViewerSmoke:
    """Core Document Viewer smoke tests."""

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate(page, app_url)
        assert_no_exceptions(page)

    @pytest.mark.smoke
    def test_title_renders(self, page, app_url):
        _navigate(page, app_url)
        title = page.locator("h1")
        assert title.count() >= 1
        assert "Document Viewer" in title.first.inner_text()

    @pytest.mark.smoke
    def test_document_table_loads(self, page, app_url):
        _navigate(page, app_url)
        tables = page.locator('[data-testid="stDataFrame"]')
        assert tables.count() >= 1, "No document table found"


class TestDocumentViewerFilters:
    """Verify filter controls work."""

    def test_doc_type_filter_exists(self, page, app_url):
        """Document Viewer should have a Document Type selectbox."""
        _navigate(page, app_url)
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        assert selectboxes.count() >= 1, "No filter selectboxes found"

    def test_sender_filter_exists(self, page, app_url):
        _navigate(page, app_url)
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        assert selectboxes.count() >= 2, "Expected at least 2 filter selectboxes"

    def test_status_filter_exists(self, page, app_url):
        _navigate(page, app_url)
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        assert selectboxes.count() >= 3, "Expected at least 3 filter selectboxes (doc type + sender + status)"

    def test_document_count_label(self, page, app_url):
        _navigate(page, app_url)
        # Should show "Documents (N results)"
        subheaders = page.locator('[data-testid="stMarkdown"] h3, [data-testid="stSubheader"]')
        found = False
        for i in range(subheaders.count()):
            text = subheaders.nth(i).inner_text()
            if "Documents" in text and "results" in text:
                found = True
                break
        # Also check the broader page content
        if not found:
            page_text = page.inner_text("body")
            assert "Documents" in page_text and "results" in page_text


class TestDocumentViewerDetail:
    """Verify document drill-down shows extracted fields."""

    def test_document_detail_section_exists(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Document Detail" in page_text, "Document Detail section not found"

    def test_select_document_dropdown(self, page, app_url):
        _navigate(page, app_url)
        # There should be a selectbox for choosing a specific document
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        # At least 4: doc type filter, sender filter, status filter, document selector
        assert selectboxes.count() >= 4, (
            f"Expected >=4 selectboxes (3 filters + doc selector), got {selectboxes.count()}"
        )

    def test_extracted_fields_visible(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        # Should show "Extracted Fields" section
        assert "Extracted Fields" in page_text, "Extracted Fields section not found"

    def test_extracted_line_items_visible(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Line Items" in page_text, "Line Items section not found"  # "Extracted Line Items"

    def test_metric_cards_in_detail(self, page, app_url):
        """The detail section should show Subtotal, Tax, Total metrics."""
        _navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 3, f"Expected >=3 metric cards, got {metrics.count()}"


class TestDocumentViewerLineItemWriteback:
    """E2E tests for line item editing and writeback via LINE_ITEM_REVIEW."""

    def _navigate_to_line_items(self, page, app_url):
        _navigate(page, app_url)
        editors = page.locator('[data-testid="stDataFrame"]')
        if editors.count() < 2:
            pytest.skip("Line item editor not found (need >=2 data frames)")
        editors.nth(1).scroll_into_view_if_needed()
        page.wait_for_timeout(1000)
        return editors.nth(1)

    def test_line_item_editor_visible(self, page, app_url):
        """Line item st.data_editor should render for the default document."""
        line_editor = self._navigate_to_line_items(page, app_url)
        cells = line_editor.locator('[role="gridcell"]')
        assert cells.count() >= 1, "Line item editor has no cells"
        assert_no_exceptions(page)

    def test_line_item_no_changes_caption(self, page, app_url):
        """Fresh load should show 'No pending changes' caption."""
        self._navigate_to_line_items(page, app_url)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        page_text = page.inner_text("body")
        assert "no pending changes" in page_text.lower() or "No pending changes" in page_text, (
            "Expected 'No pending changes' caption on fresh load"
        )

    @pytest.mark.slow
    def test_edit_line_item_and_save(self, page, app_url, sf_cursor):
        """Edit a Description cell, save, verify confirmation, verify DB, clean up."""
        line_editor = self._navigate_to_line_items(page, app_url)

        canvas = line_editor.locator("canvas").first
        cbox = canvas.bounding_box()
        if not cbox:
            pytest.skip("Line item canvas has no bounding box")

        page.mouse.click(cbox["x"] + 50, cbox["y"] + 50)
        page.wait_for_timeout(300)
        page.keyboard.press("Home")
        page.wait_for_timeout(200)

        found_desc = False
        for _ in range(10):
            selected = line_editor.locator('[aria-selected="true"]')
            if selected.count() > 0:
                colindex = selected.first.get_attribute("aria-colindex")
                headers = line_editor.locator('[role="columnheader"]')
                for hi in range(headers.count()):
                    if headers.nth(hi).get_attribute("aria-colindex") == colindex:
                        if "description" in headers.nth(hi).inner_text().lower():
                            found_desc = True
                        break
            if found_desc:
                break
            page.keyboard.press("ArrowRight")
            page.wait_for_timeout(200)

        if not found_desc:
            pytest.skip("Description column not found in line item editor")

        page.keyboard.press("Enter")
        page.wait_for_timeout(500)
        page.keyboard.press("Control+a")
        page.keyboard.type("E2E Test Description")
        page.keyboard.press("Escape")
        page.wait_for_timeout(500)

        page.locator("h1").first.click()
        page.wait_for_timeout(1000)

        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        save_btn = page.locator('button:has-text("Save")')
        if save_btn.count() == 0:
            pytest.skip("Save button not found — line item edit may not have registered")

        save_btn.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page)

        page_text = page.inner_text("body")
        assert "Saved" in page_text or "audit" in page_text.lower() or "Continue" in page_text, (
            "Expected post-save confirmation for line item correction"
        )
        assert "Validation failed" not in page_text
        assert_no_exceptions(page)

        sf_cursor.execute(
            "SELECT COUNT(*) FROM LINE_ITEM_REVIEW "
            "WHERE corrected_col_1 = 'E2E Test Description' "
            "AND reviewed_at > DATEADD('minute', -2, CURRENT_TIMESTAMP())"
        )
        recent = sf_cursor.fetchone()[0]
        assert recent >= 1, "Expected line item audit row in LINE_ITEM_REVIEW"

        sf_cursor.execute(
            "DELETE FROM LINE_ITEM_REVIEW "
            "WHERE corrected_col_1 = 'E2E Test Description' "
            "AND reviewed_at > DATEADD('minute', -2, CURRENT_TIMESTAMP())"
        )
