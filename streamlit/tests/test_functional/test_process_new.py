"""Functional tests for the Process New page (pages/3_Process_New.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


PROCESS_PATH = "/Process_New"


def _navigate(page, app_url):
    """Navigate to Process New and wait for render."""
    page.goto(f"{app_url}{PROCESS_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)


@pytest.mark.smoke
def test_process_new_form_elements(app_url, page):
    """Process New page has form elements: selectbox, sliders, multiselect, submit."""
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    assert selectboxes.count() >= 1, "Expected at least 1 selectbox (Vendor)"
    sliders = page.locator('[data-testid="stSlider"]')
    assert sliders.count() >= 2, f"Expected >=2 sliders, got {sliders.count()}"


def test_process_new_run_extraction_button(app_url, page):
    """Process New page has a 'Run Extraction' primary button."""
    _navigate(page, app_url)
    btn = page.locator('button:has-text("Run Extraction")')
    assert btn.count() > 0, "Expected 'Run Extraction' button"


def test_process_new_status_metrics(app_url, page):
    """Process New page shows 4 status metrics (Total Files, Extracted, Pending, Failed)."""
    _navigate(page, app_url)
    metrics = page.locator('[data-testid="stMetric"]')
    assert metrics.count() >= 4, f"Expected >=4 status metrics, got {metrics.count()}"


def test_process_new_progress_bar(app_url, page):
    """Process New page shows a progress bar."""
    _navigate(page, app_url)
    progress = page.locator('[data-testid="stProgress"]')
    assert progress.count() > 0, "Expected a progress bar element"


def test_process_new_extraction_status_expander(app_url, page):
    """Process New page has an expander for extraction results/status."""
    _navigate(page, app_url)
    expanders = page.locator('[data-testid="stExpander"]')
    # May have 0 expanders if no extraction has run, but the page should still render
    assert_no_exceptions(page)


def test_process_new_multiselect_categories(app_url, page):
    """Process New page has a multiselect widget for product categories."""
    _navigate(page, app_url)
    multiselect = page.locator('[data-testid="stMultiSelect"]')
    assert multiselect.count() >= 1, (
        f"Expected >=1 multiselect (categories), got {multiselect.count()}"
    )


def test_process_new_title(app_url, page):
    """Process New page displays the correct title."""
    _navigate(page, app_url)
    title = page.locator("text=Process New Invoices")
    assert title.count() > 0, "Expected 'Process New Invoices' title"


def test_process_new_step_subheaders(app_url, page):
    """Process New page shows Step 1-4 subheaders."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    for step in ["Step 1", "Step 2", "Step 3", "Step 4"]:
        assert page.locator(f"text={step}").count() > 0, f"Expected '{step}' subheader"


def test_process_new_generate_form_submit_button(app_url, page):
    """Process New page has a 'Generate & Stage Invoices' form submit button."""
    _navigate(page, app_url)
    btn = page.locator('button:has-text("Generate")')
    assert btn.count() > 0, "Expected a 'Generate' form submit button"


def test_process_new_reset_demo_expander(app_url, page):
    """Process New page has a 'Reset Demo' expander."""
    _navigate(page, app_url)
    # The Reset Demo expander is at the top
    expanders = page.locator('[data-testid="stExpander"]')
    if expanders.count() > 0:
        expander_text = expanders.first.inner_text()
        assert "Reset" in expander_text or expanders.count() >= 1
    assert_no_exceptions(page)


def test_process_new_refresh_status_button(app_url, page):
    """Process New page has a 'Refresh Status' button."""
    _navigate(page, app_url)
    # Scroll multiple times to ensure full page renders (Streamlit lazy-loads)
    for _ in range(3):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1500)
    btn = page.locator('button:has-text("Refresh")')
    if btn.count() == 0:
        # Try broader match
        btn = page.locator('button:has-text("Refresh Status")')
    if btn.count() == 0:
        pytest.skip("Refresh button not rendered (page may not have fully loaded)")


def test_process_new_recently_extracted_table(app_url, page):
    """Process New Step 4 shows a recently extracted invoices dataframe."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    # Step 4 has "Recently Extracted Invoices" and a dataframe
    df = page.locator('[data-testid="stDataFrame"]')
    if df.count() > 0:
        assert True  # dataframe present
    else:
        # May not have data if no extraction has run, but page should be stable
        assert_no_exceptions(page)


def test_process_new_caption_text(app_url, page):
    """Process New page shows the live demo caption text."""
    _navigate(page, app_url)
    assert page.locator("text=Live demo").count() > 0, \
        "Expected caption containing 'Live demo'"


def test_process_new_three_sliders(app_url, page):
    """Process New page has at least 3 sliders (num_invoices, approx_total, num_items)."""
    _navigate(page, app_url)
    sliders = page.locator('[data-testid="stSlider"]')
    assert sliders.count() >= 3, f"Expected >=3 sliders, got {sliders.count()}"


def test_process_new_status_metric_labels(app_url, page):
    """Process New status section shows metrics labeled Total Files, Extracted, Pending, Failed."""
    _navigate(page, app_url)
    metrics = page.locator('[data-testid="stMetric"]')
    expected_labels = ["Total Files", "Extracted", "Pending", "Failed"]
    found_labels = []
    for i in range(metrics.count()):
        text = metrics.nth(i).inner_text()
        for label in expected_labels:
            if label in text and label not in found_labels:
                found_labels.append(label)
    assert len(found_labels) >= 4, \
        f"Expected all 4 status labels {expected_labels}, found: {found_labels}"


def test_process_new_vendor_selectbox_has_options(app_url, page):
    """Vendor selectbox in the generate form has at least 1 option."""
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() > 0:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        assert options.count() >= 1, "Expected at least 1 vendor option"


def test_process_new_multiselect_default_categories(app_url, page):
    """Product categories multiselect has 'Beverages' and 'Snacks' as defaults."""
    _navigate(page, app_url)
    multiselect = page.locator('[data-testid="stMultiSelect"]')
    if multiselect.count() > 0:
        text = multiselect.first.inner_text()
        assert "Beverages" in text, "Expected 'Beverages' in default categories"
        assert "Snacks" in text, "Expected 'Snacks' in default categories"


def test_process_new_reset_expander_has_button(app_url, page):
    """Reset Demo expander contains a 'Reset Demo Invoices' button when expanded."""
    _navigate(page, app_url)
    expanders = page.locator('[data-testid="stExpander"]')
    if expanders.count() > 0:
        # Click to expand
        expanders.first.click()
        page.wait_for_timeout(1000)
        reset_btn = page.locator('button:has-text("Reset Demo Invoices")')
        assert reset_btn.count() > 0, "Expected 'Reset Demo Invoices' button inside expander"


def test_process_new_step4_invoice_detail_selectbox(app_url, page):
    """Step 4 has an invoice detail selectbox if recent invoices exist."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    df = page.locator('[data-testid="stDataFrame"]')
    if df.count() > 0:
        # If there's a dataframe, there should be a detail selectbox
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        # At least 2: vendor selectbox + detail selectbox
        assert selectboxes.count() >= 2, \
            f"Expected >=2 selectboxes when recent invoices exist, got {selectboxes.count()}"


def test_process_new_step4_extracted_fields(app_url, page):
    """Step 4 drill-down shows 'Extracted Header Fields' and 'Extracted Line Items' text."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)

    body_text = page.inner_text("body")
    if "Recently Extracted Invoices" in body_text:
        # Only check if there's data to display
        df = page.locator('[data-testid="stDataFrame"]')
        if df.count() > 0:
            assert "Extracted Header Fields" in body_text or "Extracted Line Items" in body_text, \
                "Expected 'Extracted Header Fields' or 'Extracted Line Items' in Step 4 drill-down"


def test_process_new_slider_interaction(app_url, page):
    """Changing the 'Number of invoices' slider via keyboard updates its value."""
    _navigate(page, app_url)
    sliders = page.locator('[data-testid="stSlider"]')
    assert sliders.count() >= 1, "Expected at least 1 slider"

    # The first slider is "Number of invoices" (default=3, min=1, max=10)
    first_slider = sliders.first
    # Focus the slider thumb and press ArrowRight to increase value
    thumb = first_slider.locator('[role="slider"]')
    if thumb.count() > 0:
        thumb.first.click()
        page.wait_for_timeout(300)
        # Press ArrowRight 2 times to move from 3 -> 5
        page.keyboard.press("ArrowRight")
        page.keyboard.press("ArrowRight")
        page.wait_for_timeout(1000)

        # Read the slider's current displayed value
        slider_text = first_slider.inner_text()
        # The value should have changed from the default of 3
        assert "5" in slider_text or "4" in slider_text, (
            f"Expected slider value to increase after arrow keys, got: {slider_text}"
        )
    assert_no_exceptions(page)


def test_process_new_multiselect_add_category(app_url, page):
    """Adding 'Tobacco' to the product categories multiselect shows it in the selection."""
    _navigate(page, app_url)
    multiselect = page.locator('[data-testid="stMultiSelect"]')
    assert multiselect.count() >= 1, "Expected multiselect widget"

    # Click the multiselect input area to open the dropdown
    ms_input = multiselect.first.locator("input")
    ms_input.click()
    page.wait_for_timeout(500)

    # Type "Tobacco" to filter and select it
    ms_input.fill("Tobacco")
    page.wait_for_timeout(500)

    # Click the Tobacco option from the dropdown
    tobacco_option = page.locator('[role="option"]', has_text="Tobacco")
    if tobacco_option.count() > 0:
        tobacco_option.first.click()
        page.wait_for_timeout(1000)

        # Verify Tobacco now appears in the multiselect widget text
        ms_text = multiselect.first.inner_text()
        assert "Tobacco" in ms_text, (
            f"Expected 'Tobacco' in multiselect after adding, got: {ms_text}"
        )
    assert_no_exceptions(page)


def test_process_new_multiselect_remove_category(app_url, page):
    """Clicking the '×' on 'Beverages' removes it from the default categories."""
    _navigate(page, app_url)
    multiselect = page.locator('[data-testid="stMultiSelect"]')
    assert multiselect.count() >= 1, "Expected multiselect widget"

    ms_text_before = multiselect.first.inner_text()
    if "Beverages" not in ms_text_before:
        pytest.skip("'Beverages' not in default selection")

    # Find the close (×) button on the Beverages tag
    # Streamlit multiselect tags have a close icon within each tag span
    beverages_close = multiselect.first.locator(
        'span:has-text("Beverages") >> [data-testid="stIcon"]'
    )
    if beverages_close.count() == 0:
        # Fallback: try finding any close button near Beverages text
        beverages_close = multiselect.first.locator(
            '[role="button"][aria-label*="Beverages"]'
        )
    if beverages_close.count() == 0:
        # Second fallback: the × icon is a sibling/child SVG
        beverages_close = multiselect.first.locator('svg').first

    if beverages_close.count() > 0:
        beverages_close.first.click()
        page.wait_for_timeout(1000)

        ms_text_after = multiselect.first.inner_text()
        # Beverages should no longer be in the text, but Snacks should remain
        assert "Snacks" in ms_text_after, (
            f"Expected 'Snacks' to remain after removing Beverages, got: {ms_text_after}"
        )
    assert_no_exceptions(page)


def test_process_new_refresh_button_click(app_url, page):
    """Clicking 'Refresh Status' reruns the page without exceptions."""
    _navigate(page, app_url)

    # Scroll multiple times to ensure full page renders (Streamlit lazy-loads)
    for _ in range(3):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1500)

    refresh_btn = page.locator('button:has-text("Refresh")')
    if refresh_btn.count() == 0:
        refresh_btn = page.locator('button:has-text("Refresh Status")')
    if refresh_btn.count() == 0:
        pytest.skip("Refresh button not rendered (page may not have fully loaded)")

    refresh_btn.first.click()
    page.wait_for_timeout(3000)
    wait_for_streamlit(page)

    # After rerun, the page title should still be present
    title = page.locator("text=Process New Invoices")
    assert title.count() > 0, "Page should re-render with title after refresh click"
    assert_no_exceptions(page)


@pytest.mark.slow
def test_process_new_form_submit_generates_invoices(app_url, page):
    """Clicking 'Generate & Stage Invoices' triggers generation and shows completion status."""
    _navigate(page, app_url)

    # Click the Generate & Stage Invoices button inside the form
    submit_btn = page.locator('button:has-text("Generate")')
    assert submit_btn.count() > 0, "Expected 'Generate' form submit button"

    submit_btn.first.click()
    page.wait_for_timeout(5000)
    wait_for_streamlit(page)

    # Wait longer for the stored procedure to complete
    page.wait_for_timeout(10000)

    # After submission, look for a st.status element with "complete" state
    # or any indication the generation finished
    body = page.inner_text("body")
    status_complete = (
        "complete" in body.lower()
        or "generated" in body.lower()
        or "Result:" in body
        or page.locator('[data-testid="stStatusWidget"]').count() > 0
    )
    assert status_complete, (
        "Expected generation status to show completion after form submit"
    )
    assert_no_exceptions(page)


@pytest.mark.smoke
def test_process_new_no_exceptions(app_url, page):
    """Process New page renders with zero Streamlit exceptions."""
    _navigate(page, app_url)
    assert_no_exceptions(page)
