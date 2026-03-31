"""Shared pytest fixtures for AI_EXTRACT test suite.

Provides:
  - sf_cursor:        Snowflake connector cursor (session-scoped)
  - sf_session:       Snowpark session (session-scoped)
  - sf_conn_factory:  Factory for independent Snowflake connections (concurrent tests)
  - admin_session:    Snowpark session with ACCOUNTADMIN (RBAC inspection)
  - app_url / page:   Playwright fixtures for E2E tests

All Snowflake fixtures skip automatically when POC_CONNECTION is not set.
Playwright fixtures skip when STREAMLIT_URL is not set.
"""

import os
import re

import pytest


# ---------------------------------------------------------------------------
# Environment config
# ---------------------------------------------------------------------------
_CONNECTION = os.environ.get("POC_CONNECTION")
_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")


def _skip_if_no_connection():
    if not _CONNECTION:
        pytest.skip("POC_CONNECTION not set — skipping Snowflake tests")


# ---------------------------------------------------------------------------
# Snowflake connector cursor
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def sf_cursor():
    """Snowflake connector cursor — skips if POC_CONNECTION not set."""
    _skip_if_no_connection()

    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=_CONNECTION)
    cur = conn.cursor()

    cur.execute(f"USE ROLE {_ROLE}")
    cur.execute(f"USE DATABASE {_DB}")
    cur.execute(f"USE SCHEMA {_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {_WH}")

    yield cur

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Snowpark session
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def sf_session():
    """Snowpark session — skips if POC_CONNECTION not set."""
    _skip_if_no_connection()

    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streamlit"))
    from config import get_session
    return get_session()


# ---------------------------------------------------------------------------
# Admin session (ACCOUNTADMIN for RBAC/grants inspection)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def admin_session():
    """Snowpark session with ACCOUNTADMIN — for grants metadata queries."""
    _skip_if_no_connection()

    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streamlit"))
    from config import get_session
    sess = get_session()
    sess.sql("USE ROLE ACCOUNTADMIN").collect()
    return sess


# ---------------------------------------------------------------------------
# Connection factory (for concurrent/load tests)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def sf_conn_factory():
    """Factory that returns independent Snowflake connections.

    Each call creates a new connection — needed for true concurrency
    in load/stress tests (test_load_stress.py).
    Connections are tracked and closed after the test session.
    """
    _skip_if_no_connection()

    import snowflake.connector
    _connections = []

    def _factory():
        conn = snowflake.connector.connect(connection_name=_CONNECTION)
        cur = conn.cursor()
        cur.execute(f"USE ROLE {_ROLE}")
        cur.execute(f"USE DATABASE {_DB}")
        cur.execute(f"USE SCHEMA {_SCHEMA}")
        cur.execute(f"USE WAREHOUSE {_WH}")
        _connections.append(conn)
        return conn

    yield _factory

    for conn in _connections:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Playwright / E2E fixtures (for test_data_pipeline.py)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def app_url():
    """Streamlit app URL — skips if STREAMLIT_URL not set."""
    url = os.environ.get("STREAMLIT_URL")
    if not url:
        pytest.skip("STREAMLIT_URL not set — skipping E2E tests")
    return url.rstrip("/")


@pytest.fixture(scope="function")
def page():
    """Playwright browser page — skips if playwright not installed."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        pytest.skip("playwright not installed — pip install playwright && playwright install")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        pg = context.new_page()
        yield pg
        context.close()
        browser.close()


# ---------------------------------------------------------------------------
# E2E helpers (importable by test_data_pipeline.py etc.)
# ---------------------------------------------------------------------------
def wait_for_streamlit(page, selectors='[data-testid="stMetric"]', timeout=15000):
    """Wait for Streamlit to finish rendering."""
    try:
        page.wait_for_selector(selectors, timeout=timeout)
    except Exception:
        pass
    page.wait_for_timeout(1000)


def assert_no_exceptions(page):
    """Check that no Streamlit exception banner is visible."""
    exc = page.locator('[data-testid="stException"]')
    assert exc.count() == 0, f"Streamlit exception: {exc.first.inner_text()}"


def get_metric_value(page, label):
    """Extract a numeric metric value from a Streamlit st.metric widget."""
    metrics = page.locator('[data-testid="stMetric"]')
    for i in range(metrics.count()):
        metric = metrics.nth(i)
        lbl = metric.locator('[data-testid="stMetricLabel"]')
        if lbl.count() > 0 and label.lower() in lbl.inner_text().lower():
            val_el = metric.locator('[data-testid="stMetricValue"]')
            if val_el.count() > 0:
                text = val_el.inner_text().replace(",", "").replace("$", "").strip()
                match = re.search(r"[\d.]+", text)
                if match:
                    return float(match.group()) if "." in match.group() else int(match.group())
    return None
