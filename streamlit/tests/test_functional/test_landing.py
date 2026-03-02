"""Functional tests for the Landing page (streamlit_app.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions, get_metric_value


@pytest.mark.smoke
def test_landing_title(app_url, page):
    """Landing page displays the main title."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    title = page.locator("text=AI-Powered Invoice Processing")
    assert title.count() > 0, "Expected title 'AI-Powered Invoice Processing'"


def test_landing_pipeline_stats_metrics(app_url, page):
    """Landing page renders 4 Live Pipeline Stats metric cards."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    metrics = page.locator('[data-testid="stMetric"]')
    assert metrics.count() >= 4, f"Expected >=4 metrics, got {metrics.count()}"


def test_landing_graphviz_chart(app_url, page):
    """Landing page contains a Graphviz architecture chart."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    chart = page.locator('[data-testid="stGraphVizChart"]')
    assert chart.count() > 0, "Expected a Graphviz chart on the landing page"


def test_landing_business_value_sections(app_url, page):
    """Landing page has 'Why This Matters' with Problem/Solution columns."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    assert page.locator("text=Why This Matters").count() > 0, "Expected 'Why This Matters' header"
    assert page.locator("text=The Problem").count() > 0, "Expected 'The Problem' subheader"
    assert page.locator("text=The Solution").count() > 0, "Expected 'The Solution' subheader"


def test_landing_key_technologies_section(app_url, page):
    """Landing page shows all 6 key technology subheaders."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    assert page.locator("text=Key Technologies").count() > 0, "Expected 'Key Technologies' header"
    for tech in ["Cortex AI_EXTRACT", "Streams + Tasks", "Streamlit Container Runtime",
                 "Inline PDF Rendering", "Analytical Views", "PDF Generation (UDTF)"]:
        assert page.locator(f"text={tech}").count() > 0, f"Missing technology: {tech}"


def test_landing_sidebar_navigation_guide(app_url, page):
    """Landing page sidebar contains the Navigation Guide."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    sidebar = page.locator('[data-testid="stSidebar"]')
    sidebar_text = sidebar.inner_text()
    assert "Navigation Guide" in sidebar_text, "Expected 'Navigation Guide' in sidebar"
    for item in ["Dashboard", "AP Ledger", "Analytics", "Process New"]:
        assert item in sidebar_text, f"Sidebar missing '{item}'"


def test_landing_invoices_extracted_metric_positive(app_url, page):
    """'Invoices Extracted' metric shows a value > 0."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Invoices Extracted")
    assert val is not None and val > 0, f"Invoices Extracted should be > 0, got {val}"


def test_landing_vendors_identified_metric_positive(app_url, page):
    """'Vendors Identified' metric shows a value > 0."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Vendors Identified")
    assert val is not None and val > 0, f"Vendors Identified should be > 0, got {val}"


def test_landing_line_items_parsed_metric_positive(app_url, page):
    """'Line Items Parsed' metric shows a value > 0."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Line Items Parsed")
    assert val is not None and val > 0, f"Line Items Parsed should be > 0, got {val}"


def test_landing_source_pdfs_metric_positive(app_url, page):
    """'Source PDFs on Stage' metric shows a value > 0."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Source PDFs on Stage")
    assert val is not None and val > 0, f"Source PDFs on Stage should be > 0, got {val}"


def test_landing_architecture_header(app_url, page):
    """Landing page displays the 'Architecture' section header."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    assert page.locator("text=Architecture").count() > 0, "Expected 'Architecture' header"


def test_landing_live_pipeline_stats_header(app_url, page):
    """Landing page displays the 'Live Pipeline Stats' section header."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    assert page.locator("text=Live Pipeline Stats").count() > 0, "Expected 'Live Pipeline Stats' header"


def test_landing_caption_text(app_url, page):
    """Landing page shows the introductory caption text."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    assert page.locator("text=End-to-end accounts payable automation").count() > 0, \
        "Expected caption containing 'End-to-end accounts payable automation'"


@pytest.mark.smoke
def test_landing_no_exceptions(app_url, page):
    """Landing page renders with zero Streamlit exceptions."""
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    assert_no_exceptions(page)
