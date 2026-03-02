"""Functional tests for the Dashboard page (pages/0_Dashboard.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions, get_metric_value


DASHBOARD_PATH = "/Dashboard"


@pytest.mark.smoke
def test_dashboard_primary_kpi_metrics(app_url, page):
    """Dashboard shows 4 primary KPI metric cards."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    metrics = page.locator('[data-testid="stMetric"]')
    assert metrics.count() >= 4, f"Expected >=4 primary metrics, got {metrics.count()}"


def test_dashboard_secondary_metrics(app_url, page):
    """Dashboard shows 3 secondary metric cards (7 total)."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    metrics = page.locator('[data-testid="stMetric"]')
    assert metrics.count() >= 7, f"Expected >=7 total metrics, got {metrics.count()}"


def test_dashboard_recent_invoices_table(app_url, page):
    """Dashboard contains a Recently Processed Invoices dataframe."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    df = page.locator('[data-testid="stDataFrame"]')
    assert df.count() > 0, "Expected a dataframe for recent invoices"


def test_dashboard_total_invoices_positive(app_url, page):
    """Total Invoices metric shows a value > 0."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    total = get_metric_value(page, "Total Invoices")
    assert total is not None and total > 0, (
        f"Total Invoices metric should be > 0, got {total}"
    )


def test_dashboard_active_vendors_positive(app_url, page):
    """Active Vendors metric shows a value > 0."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    vendors = get_metric_value(page, "Active Vendors")
    assert vendors is not None and vendors > 0, (
        f"Active Vendors metric should be > 0, got {vendors}"
    )


def test_dashboard_title(app_url, page):
    """Dashboard page displays 'Accounts Payable' title."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    title = page.locator("text=Accounts Payable")
    assert title.count() > 0, "Expected 'Accounts Payable' title on Dashboard"


def test_dashboard_total_spend_metric_present(app_url, page):
    """Dashboard has a 'Total Spend' metric card."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Total Spend")
    assert val is not None, "Expected 'Total Spend' metric to be present"


def test_dashboard_outstanding_metric_present(app_url, page):
    """Dashboard has an 'Outstanding' metric card."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Outstanding")
    assert val is not None, "Expected 'Outstanding' metric to be present"


def test_dashboard_overdue_metric_present(app_url, page):
    """Dashboard has an 'Overdue' metric card."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Overdue")
    assert val is not None, "Expected 'Overdue' metric to be present"


def test_dashboard_avg_days_to_pay_metric(app_url, page):
    """Dashboard has an 'Avg Days to Pay' metric card."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Avg Days to Pay")
    assert val is not None, "Expected 'Avg Days to Pay' metric to be present"


def test_dashboard_recent_invoices_subheader(app_url, page):
    """Dashboard has 'Recently Processed Invoices' subheader."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    header = page.locator("text=Recently Processed Invoices")
    assert header.count() > 0, "Expected 'Recently Processed Invoices' subheader"


def test_dashboard_caption_text(app_url, page):
    """Dashboard shows the 'Powered by Snowflake AI_EXTRACT' caption."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    assert page.locator("text=Powered by Snowflake AI_EXTRACT").count() > 0, \
        "Expected caption 'Powered by Snowflake AI_EXTRACT'"


def test_dashboard_extraction_pipeline_metric_label(app_url, page):
    """Dashboard has an 'Extraction Pipeline' metric card."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    metrics = page.locator('[data-testid="stMetric"]')
    found = False
    for i in range(metrics.count()):
        if "Extraction Pipeline" in metrics.nth(i).inner_text():
            found = True
            break
    assert found, "Expected 'Extraction Pipeline' metric label"


def test_dashboard_overdue_delta_text(app_url, page):
    """Overdue metric shows delta text containing 'invoices'."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    metrics = page.locator('[data-testid="stMetric"]')
    found = False
    for i in range(metrics.count()):
        text = metrics.nth(i).inner_text()
        if "Overdue" in text and "invoices" in text.lower():
            found = True
            break
    assert found, "Expected Overdue metric with 'invoices' delta text"


def test_dashboard_total_spend_positive(app_url, page):
    """Total Spend metric shows a value > 0."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    val = get_metric_value(page, "Total Spend")
    assert val is not None and val > 0, f"Total Spend should be > 0, got {val}"


def test_dashboard_dividers_present(app_url, page):
    """Dashboard has at least 2 divider elements."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    dividers = page.locator("hr")
    assert dividers.count() >= 2, f"Expected >=2 dividers, got {dividers.count()}"


@pytest.mark.smoke
def test_dashboard_no_exceptions(app_url, page):
    """Dashboard renders with zero Streamlit exceptions."""
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    assert_no_exceptions(page)
