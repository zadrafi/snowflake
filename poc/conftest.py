"""Root conftest for POC tests.

Provides a shared Snowflake connection fixture using the aws_spcs connection,
and manages Streamlit server lifecycle for E2E tests.
"""

import os
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error

import pytest
import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONNECTION_NAME = os.environ.get("POC_CONNECTION", "aws_spcs")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")

APP_PORT = 8504  # Use 8504 to avoid conflict with main app on 8503
APP_URL = f"http://localhost:{APP_PORT}"
STARTUP_TIMEOUT = 60
_APP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "streamlit")


# ---------------------------------------------------------------------------
# Snowflake connection fixture
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def sf_conn():
    """Return a Snowflake connection configured for the POC database."""
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    conn.cursor().execute(f"USE DATABASE {POC_DB}")
    conn.cursor().execute(f"USE SCHEMA {POC_SCHEMA}")
    conn.cursor().execute(f"USE WAREHOUSE {POC_WH}")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sf_cursor(sf_conn):
    """Return a cursor from the shared Snowflake connection."""
    return sf_conn.cursor()


# ---------------------------------------------------------------------------
# Streamlit server lifecycle (for E2E tests)
# ---------------------------------------------------------------------------
def _server_is_up():
    try:
        resp = urllib.request.urlopen(APP_URL, timeout=5)
        return resp.status == 200
    except (urllib.error.URLError, ConnectionError, OSError):
        return False


def _port_is_free():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", APP_PORT))
            return True
        except OSError:
            return False


def _kill_port_holders():
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", f":{APP_PORT}"],
            stderr=subprocess.DEVNULL, text=True,
        )
        for pid_str in out.strip().split():
            try:
                os.kill(int(pid_str), 9)
            except (ProcessLookupError, OSError, ValueError):
                pass
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    deadline = time.time() + 10
    while time.time() < deadline:
        if _port_is_free():
            return
        time.sleep(0.5)


_LAUNCHER = """\
import os, subprocess, sys
if os.fork() == 0:
    os.setsid()
    subprocess.Popen(
        sys.argv[1:],
        cwd=os.environ["APP_DIR"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    os._exit(0)
"""


def _start_server():
    env = os.environ.copy()
    env["APP_DIR"] = _APP_DIR
    subprocess.run(
        [sys.executable, "-c", _LAUNCHER,
         "uv", "run", "streamlit", "run", "streamlit_app.py",
         "--server.port", str(APP_PORT),
         "--server.headless", "true",
         "--browser.gatherUsageStats", "false"],
        env=env,
        check=True,
    )
    deadline = time.time() + STARTUP_TIMEOUT
    while time.time() < deadline:
        if _server_is_up():
            return
        time.sleep(1)
    raise RuntimeError(f"POC Streamlit did not start within {STARTUP_TIMEOUT}s")


def pytest_configure(config):
    """Start POC Streamlit server before workers spawn (controller only).

    Only starts if E2E tests are being collected (test_e2e in testpaths).
    """
    if hasattr(config, "workerinput"):
        return

    # Check if any E2E tests are selected
    args = config.invocation_params.args
    has_e2e = any("test_e2e" in str(a) for a in args) if args else False

    # Also check if running all tests (no specific path = run everything)
    if not args or not has_e2e:
        # If no specific args or no e2e in args, check testpaths
        testpaths = config.getini("testpaths")
        if testpaths and all("test_e2e" not in tp for tp in testpaths):
            return

    if _server_is_up():
        return

    _kill_port_holders()
    _start_server()
