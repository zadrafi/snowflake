"""Shared fixtures for POC E2E Streamlit tests."""

import os
import time
import threading
import urllib.request
import urllib.error

import pytest


APP_PORT = 8504
APP_URL = f"http://localhost:{APP_PORT}"
SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "test-results")

CONNECTION_NAME = os.environ.get("POC_CONNECTION", "default")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
POC_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")
FQ = f"{POC_DB}.{POC_SCHEMA}"


def _server_is_up():
    try:
        resp = urllib.request.urlopen(APP_URL, timeout=5)
        return resp.status == 200
    except (urllib.error.URLError, ConnectionError, OSError):
        return False


@pytest.fixture(scope="session")
def app_url():
    """Return the base URL of the already-running POC Streamlit server."""
    if not _server_is_up():
        deadline = time.time() + 30
        while time.time() < deadline:
            if _server_is_up():
                break
            time.sleep(1)
        else:
            raise RuntimeError("POC Streamlit server not reachable from worker")
    return APP_URL


@pytest.fixture(scope="session")
def sf_cursor():
    """Session-scoped Snowflake cursor for DB verification in E2E tests."""
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute(f"USE ROLE {POC_ROLE}")
    cur.execute(f"USE DATABASE {POC_DB}")
    cur.execute(f"USE SCHEMA {POC_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {POC_WH}")
    yield cur
    cur.close()
    conn.close()


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Widen viewport to 1920x1080 so more Glide Data Grid columns are visible."""
    return {**browser_context_args, "viewport": {"width": 1920, "height": 1080}}


@pytest.fixture(scope="session", autouse=True)
def _guard_invoice_data(sf_cursor):
    """Skip entire E2E session if no INVOICE data exists on this cloud."""
    sf_cursor.execute(
        f"SELECT COUNT(*) FROM {FQ}.V_DOCUMENT_SUMMARY WHERE doc_type = 'INVOICE'"
    )
    count = sf_cursor.fetchone()[0]
    if count == 0:
        pytest.skip("No INVOICE data on this cloud — E2E tests require seed data")


# ---------------------------------------------------------------------------
# Pytest hooks — screenshot on failure (with hard timeout to prevent hangs)
# ---------------------------------------------------------------------------
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call" and report.failed:
        page = item.funcargs.get("page")
        if page is not None:
            os.makedirs(SCREENSHOT_DIR, exist_ok=True)
            name = item.nodeid.replace("/", "_").replace("::", "__").replace(" ", "_")
            path = os.path.join(SCREENSHOT_DIR, f"{name}.png")

            def _take_screenshot():
                try:
                    page.screenshot(path=path, full_page=True, timeout=5_000)
                except Exception:
                    pass

            t = threading.Thread(target=_take_screenshot, daemon=True)
            t.start()
            t.join(timeout=8)
