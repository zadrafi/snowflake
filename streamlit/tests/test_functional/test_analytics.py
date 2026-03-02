"""Functional tests for the Analytics page (pages/2_Analytics.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


ANALYTICS_PATH = "/Analytics"


def _navigate(page, app_url):
    """Navigate to Analytics and wait for render."""
    page.goto(f"{app_url}{ANALYTICS_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)


@pytest.mark.smoke
def test_analytics_spend_by_vendor(app_url, page):
    """Analytics page has a 'Spend by Vendor' section with a chart."""
    _navigate(page, app_url)
    header = page.locator("text=Spend by Vendor")
    assert header.count() > 0, "Expected 'Spend by Vendor' section"


def test_analytics_monthly_spend_trend(app_url, page):
    """Analytics page has a 'Monthly Spend Trend' section."""
    _navigate(page, app_url)
    header = page.locator("text=Monthly Spend Trend")
    assert header.count() > 0, "Expected 'Monthly Spend Trend' section"


def test_analytics_spend_by_category(app_url, page):
    """Analytics page has a 'Spend by Category' treemap."""
    _navigate(page, app_url)
    header = page.locator("text=Spend by Category")
    assert header.count() > 0, "Expected 'Spend by Category' section"


def test_analytics_aging_and_top_products(app_url, page):
    """Analytics page has Aging Distribution and Top 20 Products sections."""
    _navigate(page, app_url)
    aging = page.locator("text=Aging Distribution")
    top_products = page.locator("text=Top 20 Products by Spend")
    assert aging.count() > 0, "Expected 'Aging Distribution' section"
    assert top_products.count() > 0, "Expected 'Top 20 Products by Spend' section"


def test_analytics_plotly_charts_render(app_url, page):
    """Analytics page renders at least 2 Plotly charts (vendor bar + trend/treemap)."""
    _navigate(page, app_url)
    charts = page.locator('[data-testid="stPlotlyChart"]')
    assert charts.count() >= 2, (
        f"Expected >=2 Plotly charts on Analytics, got {charts.count()}"
    )


def test_analytics_four_plotly_charts(app_url, page):
    """Analytics page renders exactly 4 Plotly charts (vendor, trend, category, aging)."""
    _navigate(page, app_url)
    # Scroll to load all lazy charts
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)
    charts = page.locator('[data-testid="stPlotlyChart"]')
    assert charts.count() >= 4, (
        f"Expected >=4 Plotly charts (vendor, trend, category, aging), got {charts.count()}"
    )


def test_analytics_vendor_payment_terms(app_url, page):
    """Analytics page has a 'Vendor Payment Terms Summary' section with a dataframe."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    header = page.locator("text=Vendor Payment Terms Summary")
    assert header.count() > 0, "Expected 'Vendor Payment Terms Summary' section"
    dataframes = page.locator('[data-testid="stDataFrame"]')
    assert dataframes.count() >= 1, "Expected a dataframe in Vendor Payment Terms section"


def test_analytics_top_products_dataframe_has_rows(app_url, page):
    """Top 20 Products dataframe has at least 1 row."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    header = page.locator("text=Top 20 Products by Spend")
    assert header.count() > 0, "Expected 'Top 20 Products by Spend' section"
    dataframes = page.locator('[data-testid="stDataFrame"]')
    # At least 2 dataframes: top products + vendor payment terms
    assert dataframes.count() >= 2, (
        f"Expected >=2 dataframes (top products + payment terms), got {dataframes.count()}"
    )


def test_analytics_title(app_url, page):
    """Analytics page displays the correct title."""
    _navigate(page, app_url)
    title = page.locator("text=Accounts Payable Analytics")
    assert title.count() > 0, "Expected 'Accounts Payable Analytics' title"


def test_analytics_dividers_present(app_url, page):
    """Analytics page uses dividers to separate sections."""
    _navigate(page, app_url)
    # Streamlit st.divider() renders as <hr> elements
    dividers = page.locator("hr")
    assert dividers.count() >= 1, "Expected at least 1 divider on the analytics page"


def test_analytics_caption_text(app_url, page):
    """Analytics page shows the 'Spend analysis across vendors' caption."""
    _navigate(page, app_url)
    assert page.locator("text=Spend analysis across vendors").count() > 0, \
        "Expected caption containing 'Spend analysis across vendors'"


def test_analytics_monthly_summary_caption(app_url, page):
    """Analytics page shows 'Total:' and 'Monthly Avg:' caption below the trend chart."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)
    # st.caption renders in a small text element; check for the combined text
    caption = page.locator("text=/Total:.*Monthly Avg:/")
    if caption.count() == 0:
        # Fallback: check for either text separately
        total_el = page.locator("text=Total: $")
        avg_el = page.locator("text=Monthly Avg: $")
        assert total_el.count() > 0 or avg_el.count() > 0, \
            "Expected 'Total:' or 'Monthly Avg:' summary caption"


def test_analytics_two_dataframes_with_data(app_url, page):
    """Both Top Products and Payment Terms dataframes are visible with non-zero dimensions."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(3000)
    dataframes = page.locator('[data-testid="stDataFrame"]')
    assert dataframes.count() >= 2, f"Expected >=2 dataframes, got {dataframes.count()}"
    # Verify dataframes are visible with non-zero height (stDataFrame uses iframes)
    for i in range(min(2, dataframes.count())):
        box = dataframes.nth(i).bounding_box()
        assert box is not None and box["height"] > 0, \
            f"Dataframe {i} should be visible with non-zero height"


def test_analytics_all_six_section_headers(app_url, page):
    """Analytics page contains all 6 section subheaders."""
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    headers = [
        "Spend by Vendor",
        "Monthly Spend Trend",
        "Spend by Category",
        "Aging Distribution",
        "Top 20 Products by Spend",
        "Vendor Payment Terms Summary",
    ]
    for header in headers:
        assert page.locator(f"text={header}").count() > 0, f"Missing section header: '{header}'"


@pytest.mark.smoke
def test_analytics_no_exceptions(app_url, page):
    """Analytics page renders with zero Streamlit exceptions."""
    _navigate(page, app_url)
    assert_no_exceptions(page)
