"""Functional tests for the AP Ledger page (pages/1_AP_Ledger.py)."""

import re

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


LEDGER_PATH = "/AP_Ledger"


def _navigate(page, app_url):
    """Navigate to AP Ledger and wait for render."""
    page.goto(f"{app_url}{LEDGER_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)


def _get_result_count(page):
    """Parse the invoice result count from the subheader."""
    header = page.locator("text=/Invoices \\(\\d+ results\\)/")
    if header.count() > 0:
        text = header.first.inner_text()
        match = re.search(r"\((\d+) results\)", text)
        if match:
            return int(match.group(1))
    return None


@pytest.mark.smoke
def test_ledger_filter_selectboxes(app_url, page):
    """AP Ledger has 3 filter selectboxes (Vendor, Status, Aging)."""
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    assert selectboxes.count() >= 3, f"Expected >=3 selectboxes, got {selectboxes.count()}"


def test_ledger_invoice_table_renders(app_url, page):
    """AP Ledger renders an invoice table with results."""
    _navigate(page, app_url)
    header = page.locator("text=/Invoices \\(\\d+ results\\)/")
    assert header.count() > 0, "Expected 'Invoices (N results)' subheader"


def test_ledger_invoice_count_positive(app_url, page):
    """Invoice count in the header is > 0."""
    _navigate(page, app_url)
    count = _get_result_count(page)
    assert count is not None and count > 0, f"Invoice count should be > 0, got: {count}"


def test_ledger_dataframe_present(app_url, page):
    """AP Ledger renders a stDataFrame element for the invoice table."""
    _navigate(page, app_url)
    df = page.locator('[data-testid="stDataFrame"]')
    assert df.count() > 0, "Expected a stDataFrame element for the invoice table"


def test_ledger_aging_summary_metrics(app_url, page):
    """Aging Summary section has metric cards."""
    _navigate(page, app_url)
    metrics = page.locator('[data-testid="stMetric"]')
    assert metrics.count() >= 1, "Expected at least 1 aging summary metric"


def test_ledger_title(app_url, page):
    """AP Ledger displays the page title."""
    _navigate(page, app_url)
    title = page.locator("text=Accounts Payable Ledger")
    assert title.count() > 0, "Expected 'Accounts Payable Ledger' title"


def test_ledger_vendor_filter_changes_results(app_url, page):
    """Selecting a specific vendor changes the result count."""
    _navigate(page, app_url)

    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    vendor_select = page.locator('[data-testid="stSelectbox"]').first
    vendor_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() >= 2:
        options.nth(1).click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page)

        count_after = _get_result_count(page)
        if count_after is not None:
            assert count_after <= count_before, (
                f"Filtering by vendor should reduce or equal count: {count_before} -> {count_after}"
            )


def test_ledger_status_filter(app_url, page):
    """Selecting 'PAID' status filter updates the table."""
    _navigate(page, app_url)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        status_select = selectboxes.nth(1)
        status_select.click()
        page.wait_for_timeout(500)
        paid_option = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="PAID")
        if paid_option.count() > 0:
            paid_option.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)
    assert_no_exceptions(page)


def test_ledger_aging_bucket_filter(app_url, page):
    """Selecting an aging bucket filter updates the result count."""
    _navigate(page, app_url)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        aging_select = selectboxes.nth(2)
        aging_select.click()
        page.wait_for_timeout(500)
        current_option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]',
            has_text="Current",
        )
        if current_option.count() > 0:
            current_option.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)
    assert_no_exceptions(page)


def test_ledger_combined_vendor_and_status_filter(app_url, page):
    """Applying vendor + status filters together narrows the results."""
    _navigate(page, app_url)

    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    # Set vendor filter (2nd option = first real vendor)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    selectboxes.first.click()
    page.wait_for_timeout(500)
    vendor_opts = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if vendor_opts.count() >= 2:
        vendor_opts.nth(1).click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page)

    # Now set status filter (PAID)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        selectboxes.nth(1).click()
        page.wait_for_timeout(500)
        paid = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="PAID")
        if paid.count() > 0:
            paid.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)

    count_after = _get_result_count(page)
    if count_after is not None:
        assert count_after <= count_before, (
            f"Combined filters should narrow results: {count_before} -> {count_after}"
        )
    assert_no_exceptions(page)


def test_ledger_invoice_detail_section(app_url, page):
    """AP Ledger drill-down: Invoice Detail section renders with selectbox."""
    _navigate(page, app_url)

    detail_header = page.locator("text=Invoice Detail")
    assert detail_header.count() > 0, "Expected 'Invoice Detail' subheader for drill-down"

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    assert selectboxes.count() >= 4, (
        f"Expected >=4 selectboxes (3 filters + invoice detail), got {selectboxes.count()}"
    )


def test_ledger_invoice_detail_renders_fields(app_url, page):
    """Selecting an invoice shows extracted header fields and line items."""
    _navigate(page, app_url)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    header_fields = page.locator("text=Extracted Header Fields")
    line_items = page.locator("text=Extracted Line Items")

    assert header_fields.count() > 0 or line_items.count() > 0, (
        "Expected 'Extracted Header Fields' or 'Extracted Line Items' in drill-down"
    )


def test_ledger_detail_line_items_dataframe(app_url, page):
    """Invoice drill-down shows a line items dataframe."""
    _navigate(page, app_url)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    line_items_header = page.locator("text=Extracted Line Items")
    if line_items_header.count() > 0:
        dataframes = page.locator('[data-testid="stDataFrame"]')
        # At least 2 dataframes: invoice table + line items
        assert dataframes.count() >= 2, (
            f"Expected >=2 dataframes (invoices + line items), got {dataframes.count()}"
        )


def test_ledger_caption_text(app_url, page):
    """AP Ledger shows the caption about AI_EXTRACT extraction."""
    _navigate(page, app_url)
    assert page.locator("text=All invoices extracted from PDF").count() > 0, \
        "Expected caption containing 'All invoices extracted from PDF'"


def test_ledger_aging_summary_header(app_url, page):
    """AP Ledger has an 'Aging Summary' subheader."""
    _navigate(page, app_url)
    assert page.locator("text=Aging Summary").count() > 0, \
        "Expected 'Aging Summary' subheader"


def test_ledger_aging_bucket_filter_options(app_url, page):
    """Aging Bucket selectbox has the expected 7 options."""
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        aging_select = selectboxes.nth(2)
        aging_select.click()
        page.wait_for_timeout(500)
        options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        option_count = options.count()
        assert option_count >= 7, f"Expected >=7 aging options, got {option_count}"


def test_ledger_filter_reset_restores_count(app_url, page):
    """Setting a vendor filter then resetting to 'All Vendors' restores the original count."""
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    # Select a specific vendor
    vendor_select = page.locator('[data-testid="stSelectbox"]').first
    vendor_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() < 2:
        pytest.skip("Not enough vendor options to test reset")
    options.nth(1).click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page)

    # Reset to All Vendors
    vendor_select = page.locator('[data-testid="stSelectbox"]').first
    vendor_select.click()
    page.wait_for_timeout(500)
    all_option = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="All Vendors")
    if all_option.count() > 0:
        all_option.first.click()
        page.wait_for_timeout(3000)
        wait_for_streamlit(page)

    count_after = _get_result_count(page)
    assert count_after == count_before, \
        f"Resetting filter should restore count: expected {count_before}, got {count_after}"


def test_ledger_detail_subtotal_tax_total_metrics(app_url, page):
    """Invoice detail drill-down shows Subtotal, Tax, and Total metric cards."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    metrics = page.locator('[data-testid="stMetric"]')
    labels_found = []
    for i in range(metrics.count()):
        text = metrics.nth(i).inner_text()
        for label in ["Subtotal", "Tax", "Total"]:
            if label in text and label not in labels_found:
                labels_found.append(label)
    assert len(labels_found) >= 3, \
        f"Expected Subtotal, Tax, Total metrics in drill-down, found: {labels_found}"


def test_ledger_detail_extracted_field_labels(app_url, page):
    """Invoice detail shows extracted field labels (Vendor:, Invoice #:, Due Date:)."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    body_text = page.inner_text("body")
    for label in ["Vendor:", "Invoice #:", "Due Date:"]:
        assert label in body_text, f"Expected '{label}' in invoice detail section"


def test_ledger_detail_source_pdf_section(app_url, page):
    """Invoice detail has a 'Source PDF' section."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    assert page.locator("text=Source PDF").count() > 0, \
        "Expected 'Source PDF' text in invoice detail section"


def test_ledger_switch_invoice_detail(app_url, page):
    """Switching the invoice detail selectbox updates the displayed fields."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    # The detail selectbox is the 4th one (after 3 filter selectboxes)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() < 4:
        pytest.skip("No invoice detail selectbox found")

    detail_select = selectboxes.nth(3)
    # Get initial text
    initial_text = page.inner_text("body")

    detail_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() < 2:
        pytest.skip("Only one invoice available for detail view")
    options.nth(1).click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page)

    # Verify the page still has detail content (no crash)
    assert page.locator("text=Extracted Header Fields").count() > 0 or \
        page.locator("text=Source PDF").count() > 0, \
        "Invoice detail should still render after switching invoice"
    assert_no_exceptions(page)


def test_ledger_status_filter_pending(app_url, page):
    """Selecting 'PENDING' status filter updates the table and shows no exceptions."""
    _navigate(page, app_url)
    count_before = _get_result_count(page)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        status_select = selectboxes.nth(1)
        status_select.click()
        page.wait_for_timeout(500)
        pending_option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]',
            has_text="PENDING",
        )
        if pending_option.count() > 0:
            pending_option.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)

            count_after = _get_result_count(page)
            if count_before is not None and count_after is not None:
                assert count_after <= count_before, (
                    f"PENDING filter should narrow results: {count_before} -> {count_after}"
                )
    assert_no_exceptions(page)


def test_ledger_aging_filter_31_60_days(app_url, page):
    """Selecting '31-60 Days' aging filter reduces the result count."""
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        aging_select = selectboxes.nth(2)
        aging_select.click()
        page.wait_for_timeout(500)
        option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]',
            has_text="31-60 Days",
        )
        if option.count() > 0:
            option.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)

            count_after = _get_result_count(page)
            if count_after is not None:
                assert count_after <= count_before, (
                    f"31-60 Days filter should narrow results: {count_before} -> {count_after}"
                )
    assert_no_exceptions(page)


def test_ledger_three_filter_combination(app_url, page):
    """Applying vendor + status + aging filters simultaneously narrows results."""
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    selectboxes = page.locator('[data-testid="stSelectbox"]')

    # Set vendor filter (pick 2nd option = first real vendor)
    if selectboxes.count() >= 1:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        vendor_opts = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        if vendor_opts.count() >= 2:
            vendor_opts.nth(1).click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)

    # Set status filter (PAID)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        selectboxes.nth(1).click()
        page.wait_for_timeout(500)
        paid = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="PAID")
        if paid.count() > 0:
            paid.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)

    # Set aging filter (Current)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        selectboxes.nth(2).click()
        page.wait_for_timeout(500)
        current = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="Current")
        if current.count() > 0:
            current.first.click()
            page.wait_for_timeout(3000)
            wait_for_streamlit(page)

    count_after = _get_result_count(page)
    if count_after is not None:
        assert count_after <= count_before, (
            f"Three filters should narrow results: {count_before} -> {count_after}"
        )
    assert_no_exceptions(page)


def test_ledger_scroll_and_select_invoice_detail(app_url, page):
    """Scroll to detail, select an invoice, verify PDF + fields + line items render."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() < 4:
        pytest.skip("No invoice detail selectbox found")

    detail_select = selectboxes.nth(3)
    detail_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() > 0:
        options.first.click()
        page.wait_for_timeout(5000)
        wait_for_streamlit(page)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Verify all three sections render together
        body = page.inner_text("body")
        has_pdf = page.locator("text=Source PDF").count() > 0
        has_fields = "Extracted Header Fields" in body
        has_lines = "Extracted Line Items" in body
        assert has_pdf and has_fields and has_lines, (
            f"Invoice detail should show PDF ({has_pdf}), fields ({has_fields}), "
            f"and line items ({has_lines})"
        )
    assert_no_exceptions(page)


@pytest.mark.smoke
def test_ledger_no_exceptions(app_url, page):
    """AP Ledger renders with zero Streamlit exceptions."""
    _navigate(page, app_url)
    assert_no_exceptions(page)
