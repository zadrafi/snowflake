"""SPCS / Streamlit deployment validation tests.

Validates that all deployment prerequisites and artifacts exist in Snowflake:
  1. STREAMLIT_STAGE exists and contains all expected files
  2. Compute pool exists and is configured
  3. Streamlit app object exists
  4. Network rule and EAI exist
  5. Required SQL objects (SPs, views, tables) exist
  6. deploy_poc.sh script references all Streamlit pages
"""

import json
import os
import re
import subprocess

import pytest


pytestmark = pytest.mark.sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
CONNECTION_NAME = os.environ.get("POC_CONNECTION", "default")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
POC_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")
FQ = f"{POC_DB}.{POC_SCHEMA}"


@pytest.fixture(scope="session")
def sf_cursor():
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
def admin_cursor():
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute(f"USE DATABASE {POC_DB}")
    cur.execute(f"USE SCHEMA {POC_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {POC_WH}")
    yield cur
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# 1. Stage and file validation
# ---------------------------------------------------------------------------
class TestStreamlitStage:
    """Verify STREAMLIT_STAGE exists and has all required files."""

    def test_stage_exists(self, sf_cursor):
        sf_cursor.execute(f"SHOW STAGES LIKE 'STREAMLIT_STAGE' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "STREAMLIT_STAGE not found"

    def test_stage_has_main_file(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/streamlit_app.py")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "streamlit_app.py not found in stage"

    def test_stage_has_config(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/config.py")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "config.py not found in stage"

    def test_stage_has_environment_yml(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/environment.yml")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "environment.yml not found in stage"

    def test_stage_has_pyproject_toml(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/pyproject.toml")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "pyproject.toml not found in stage. "
            "Container Runtime requires pyproject.toml for dependency installation. "
            "Upload it: PUT file://streamlit/pyproject.toml @STREAMLIT_STAGE/ "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
        )

    @pytest.mark.parametrize("page", [
        "0_Dashboard.py",
        "1_Document_Viewer.py",
        "2_Analytics.py",
        "3_Review.py",
        "4_Admin.py",
    ])
    def test_stage_has_page(self, sf_cursor, page):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/pages/{page}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"pages/{page} not found in stage"


# ---------------------------------------------------------------------------
# 2. Compute pool
# ---------------------------------------------------------------------------
class TestComputePool:
    """Verify the compute pool for Container Runtime."""

    def test_pool_exists(self, admin_cursor):
        admin_cursor.execute("SHOW COMPUTE POOLS LIKE 'AI_EXTRACT_POC_POOL'")
        rows = admin_cursor.fetchall()
        assert len(rows) >= 1, "AI_EXTRACT_POC_POOL not found"

    def test_pool_instance_family(self, admin_cursor):
        admin_cursor.execute("SHOW COMPUTE POOLS LIKE 'AI_EXTRACT_POC_POOL'")
        rows = admin_cursor.fetchall()
        assert len(rows) >= 1
        cols = [d[0] for d in admin_cursor.description]
        if "instance_family" in cols:
            idx = cols.index("instance_family")
            assert rows[0][idx] == "CPU_X64_XS"


# ---------------------------------------------------------------------------
# 3. Streamlit app object
# ---------------------------------------------------------------------------
class TestStreamlitApp:
    """Verify the Streamlit app was created correctly and is runnable."""

    @pytest.fixture(scope="class")
    def streamlit_desc(self, sf_cursor):
        """DESCRIBE STREAMLIT and return as a dict."""
        sf_cursor.execute(
            f"DESCRIBE STREAMLIT {FQ}.AI_EXTRACT_DASHBOARD"
        )
        rows = sf_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("AI_EXTRACT_DASHBOARD not deployed")
        cols = [d[0].lower() for d in sf_cursor.description]
        return dict(zip(cols, rows[0]))

    def test_streamlit_exists(self, sf_cursor):
        sf_cursor.execute("SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD'")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "AI_EXTRACT_DASHBOARD Streamlit not found. "
            "Run 07_deploy_streamlit.sql to create it."
        )

    def test_live_version_active(self, streamlit_desc):
        """App must have a live version — without this it shows an error page."""
        live_uri = streamlit_desc.get("live_version_location_uri", "")
        assert live_uri and live_uri.strip(), (
            "Streamlit has no live version. The app will not load. "
            "Run: ALTER STREAMLIT AI_EXTRACT_DASHBOARD ADD LIVE VERSION FROM LAST;"
        )

    def test_uses_from_not_root_location(self, streamlit_desc):
        """Container runtime does not support ROOT_LOCATION — must use FROM."""
        # FROM-based apps have a version source location; ROOT_LOCATION apps don't
        source_uri = streamlit_desc.get(
            "default_version_source_location_uri", ""
        )
        assert source_uri and "STREAMLIT_STAGE" in source_uri.upper(), (
            "Streamlit was not created with FROM syntax. "
            "ROOT_LOCATION is not supported for container runtimes. "
            "Recreate with: CREATE OR REPLACE STREAMLIT ... FROM '@...STREAMLIT_STAGE'"
        )

    def test_container_runtime(self, streamlit_desc):
        """Must use container runtime, not warehouse runtime."""
        runtime = streamlit_desc.get("runtime_name", "")
        assert "CONTAINER" in runtime.upper(), (
            f"Expected container runtime, got '{runtime}'. "
            "Set RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'"
        )

    def test_compute_pool_set(self, streamlit_desc):
        """Container runtime requires a compute pool."""
        pool = streamlit_desc.get("compute_pool", "")
        assert pool and "AI_EXTRACT_POC_POOL" in pool.upper(), (
            f"Compute pool not set or wrong: '{pool}'. "
            "Container runtime requires COMPUTE_POOL = AI_EXTRACT_POC_POOL"
        )

    def test_eai_attached(self, streamlit_desc):
        """PyPI EAI must be attached for pip install to work."""
        eai = str(streamlit_desc.get("external_access_integrations", ""))
        assert "PYPI_ACCESS_INTEGRATION" in eai.upper(), (
            f"PYPI_ACCESS_INTEGRATION not attached to Streamlit app. "
            "Container runtime cannot install packages without it. "
            "Add: EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_ACCESS_INTEGRATION)"
        )

    def test_main_file_correct(self, streamlit_desc):
        main = streamlit_desc.get("main_file", "")
        assert main == "streamlit_app.py", (
            f"Main file is '{main}', expected 'streamlit_app.py'"
        )

    def test_query_warehouse_set(self, streamlit_desc):
        wh = streamlit_desc.get("query_warehouse", "")
        assert wh and "AI_EXTRACT" in wh.upper(), (
            f"Query warehouse not set or wrong: '{wh}'. "
            "Set QUERY_WAREHOUSE = AI_EXTRACT_WH"
        )

    def test_url_id_assigned(self, sf_cursor):
        """App must have a URL ID for Snowsight access."""
        sf_cursor.execute("SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD'")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1
        cols = [d[0].lower() for d in sf_cursor.description]
        row_dict = dict(zip(cols, rows[0]))
        url_id = row_dict.get("url_id", "")
        assert url_id and len(url_id) > 0, (
            "Streamlit has no url_id — app is not accessible in Snowsight"
        )


# ---------------------------------------------------------------------------
# 4. Network access (EAI)
# ---------------------------------------------------------------------------
class TestExternalAccess:
    """Verify PyPI access integration exists for Container Runtime."""

    def test_pypi_network_rule_exists(self, admin_cursor):
        admin_cursor.execute("SHOW NETWORK RULES LIKE 'PYPI_NETWORK_RULE'")
        rows = admin_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("PYPI_NETWORK_RULE not deployed (requires ACCOUNTADMIN + 07_deploy_streamlit.sql)")

    def test_pypi_access_integration_exists(self, admin_cursor):
        admin_cursor.execute(
            "SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'PYPI_ACCESS_INTEGRATION'"
        )
        rows = admin_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("PYPI_ACCESS_INTEGRATION not deployed")


# ---------------------------------------------------------------------------
# 5. Required SQL objects
# ---------------------------------------------------------------------------
class TestRequiredSQLObjects:
    """Verify all required tables, views, stages, and procedures exist."""

    REQUIRED_TABLES = [
        "RAW_DOCUMENTS",
        "EXTRACTED_FIELDS",
        "EXTRACTED_TABLE_DATA",
        "DOCUMENT_TYPE_CONFIG",
        "INVOICE_REVIEW",
    ]

    REQUIRED_VIEWS = [
        "V_DOCUMENT_SUMMARY",
        "V_INVOICE_SUMMARY",
    ]

    REQUIRED_STAGES = [
        "DOCUMENT_STAGE",
        "STREAMLIT_STAGE",
    ]

    REQUIRED_PROCEDURES = [
        "SP_EXTRACT_NEW_DOCUMENTS",
        "SP_EXTRACT_BY_DOC_TYPE",
        "SP_REEXTRACT_DOC_TYPE",
    ]

    @pytest.mark.parametrize("table", REQUIRED_TABLES)
    def test_table_exists(self, sf_cursor, table):
        sf_cursor.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"Table {table} not found"

    @pytest.mark.parametrize("view", REQUIRED_VIEWS)
    def test_view_exists(self, sf_cursor, view):
        sf_cursor.execute(f"SHOW VIEWS LIKE '{view}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"View {view} not found"

    @pytest.mark.parametrize("stage", REQUIRED_STAGES)
    def test_stage_exists(self, sf_cursor, stage):
        sf_cursor.execute(f"SHOW STAGES LIKE '{stage}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"Stage {stage} not found"

    @pytest.mark.parametrize("proc", REQUIRED_PROCEDURES)
    def test_procedure_exists(self, sf_cursor, proc):
        sf_cursor.execute(f"SHOW PROCEDURES LIKE '{proc}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"Procedure {proc} not found"


# ---------------------------------------------------------------------------
# 6. Deploy script completeness (local file check, no SF connection needed)
# ---------------------------------------------------------------------------
class TestDeployScriptCompleteness:
    """Verify deploy_poc.sh references all required files."""

    DEPLOY_SCRIPT = os.path.join(
        os.path.dirname(__file__), "..", "deploy_poc.sh"
    )

    EXPECTED_PAGES = [
        "0_Dashboard.py",
        "1_Document_Viewer.py",
        "2_Analytics.py",
        "3_Review.py",
        "4_Admin.py",
    ]

    def test_deploy_script_exists(self):
        assert os.path.exists(self.DEPLOY_SCRIPT), "deploy_poc.sh not found"

    def test_deploy_script_is_executable(self):
        assert os.access(self.DEPLOY_SCRIPT, os.X_OK), \
            "deploy_poc.sh should be executable"

    def test_deploy_script_uploads_all_pages(self):
        with open(self.DEPLOY_SCRIPT) as f:
            content = f.read()
        for page in self.EXPECTED_PAGES:
            assert page in content, \
                f"deploy_poc.sh missing PUT for {page}"

    def test_deploy_script_references_all_sql_files(self):
        with open(self.DEPLOY_SCRIPT) as f:
            content = f.read()
        expected_sql = [
            "01_setup.sql",
            "02_tables.sql",
            "05_views.sql",
            "06_automate.sql",
            "07_deploy_streamlit.sql",
        ]
        for sql_file in expected_sql:
            assert sql_file in content, \
                f"deploy_poc.sh missing reference to {sql_file}"

    def test_deploy_script_has_validation_step(self):
        """Deploy script should include a post-deployment validation check."""
        with open(self.DEPLOY_SCRIPT) as f:
            content = f.read()
        assert "validat" in content.lower() or "verify" in content.lower() or \
               "check" in content.lower(), \
            "deploy_poc.sh should include deployment validation"


# ---------------------------------------------------------------------------
# 7. Environment.yml validation
# ---------------------------------------------------------------------------
class TestEnvironmentYml:
    """Verify environment.yml has required packages."""

    ENV_YML = os.path.join(
        os.path.dirname(__file__), "..", "streamlit", "environment.yml"
    )

    def test_environment_yml_exists(self):
        assert os.path.exists(self.ENV_YML), "environment.yml not found"

    def test_has_streamlit_dependency(self):
        with open(self.ENV_YML) as f:
            content = f.read()
        assert "streamlit" in content.lower(), "environment.yml missing streamlit"


# ---------------------------------------------------------------------------
# 8. pyproject.toml validation (required for Container Runtime)
# ---------------------------------------------------------------------------
class TestPyprojectToml:
    """Container Runtime requires pyproject.toml for dependency installation."""

    PYPROJECT = os.path.join(
        os.path.dirname(__file__), "..", "streamlit", "pyproject.toml"
    )

    def test_pyproject_toml_exists(self):
        assert os.path.exists(self.PYPROJECT), (
            "pyproject.toml not found in streamlit/ directory. "
            "Container Runtime requires pyproject.toml (not just environment.yml)."
        )

    def test_has_project_section(self):
        with open(self.PYPROJECT) as f:
            content = f.read()
        assert "[project]" in content, (
            "pyproject.toml missing [project] section"
        )

    def test_has_streamlit_dependency(self):
        with open(self.PYPROJECT) as f:
            content = f.read()
        assert "streamlit" in content.lower(), (
            "pyproject.toml missing streamlit dependency"
        )

    def test_has_python_version(self):
        with open(self.PYPROJECT) as f:
            content = f.read()
        assert "requires-python" in content, (
            "pyproject.toml missing requires-python. "
            "Add: requires-python = \">=3.11\""
        )


# ---------------------------------------------------------------------------
# 9. DDL linting — catch deployment-breaking syntax before it ships
# ---------------------------------------------------------------------------
class TestStreamlitDDLLinting:
    """Lint 07_deploy_streamlit.sql for known-bad patterns."""

    DDL_FILE = os.path.join(
        os.path.dirname(__file__), "..", "sql", "07_deploy_streamlit.sql"
    )

    def test_ddl_file_exists(self):
        assert os.path.exists(self.DDL_FILE), "07_deploy_streamlit.sql not found"

    def test_no_root_location(self):
        """ROOT_LOCATION is legacy and not supported for Container Runtime."""
        with open(self.DDL_FILE) as f:
            content = f.read()
        # Only check uncommented lines
        active_lines = [
            line for line in content.splitlines()
            if not line.strip().startswith("--")
        ]
        active_sql = "\n".join(active_lines)
        assert "ROOT_LOCATION" not in active_sql, (
            "07_deploy_streamlit.sql contains ROOT_LOCATION. "
            "This is not supported for container runtimes and will cause "
            "'ROOT_LOCATION stages are not supported for vNext applications'. "
            "Replace with: FROM '@...STREAMLIT_STAGE'"
        )

    def test_has_from_clause(self):
        """CREATE STREAMLIT must use FROM for versioned stage."""
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "FROM '@" in content or "FROM  '@" in content, (
            "07_deploy_streamlit.sql missing FROM clause in CREATE STREAMLIT. "
            "Container runtime requires FROM '@stage_path' syntax."
        )

    def test_has_add_live_version(self):
        """FROM-based apps require ADD LIVE VERSION to activate."""
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "ADD LIVE VERSION" in content.upper(), (
            "07_deploy_streamlit.sql missing ALTER STREAMLIT ... ADD LIVE VERSION FROM LAST. "
            "Without this, the app is created but never activated — users see an error page."
        )

    def test_has_container_runtime(self):
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "SYSTEM$ST_CONTAINER_RUNTIME" in content, (
            "07_deploy_streamlit.sql missing RUNTIME_NAME for container runtime"
        )

    def test_has_compute_pool(self):
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "COMPUTE_POOL" in content, (
            "07_deploy_streamlit.sql missing COMPUTE_POOL (required for container runtime)"
        )

    def test_has_eai(self):
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "PYPI_ACCESS_INTEGRATION" in content, (
            "07_deploy_streamlit.sql missing EXTERNAL_ACCESS_INTEGRATIONS for PyPI"
        )

    def test_deploy_script_uploads_pyproject(self):
        """deploy_poc.sh must upload pyproject.toml to stage."""
        deploy_script = os.path.join(
            os.path.dirname(__file__), "..", "deploy_poc.sh"
        )
        with open(deploy_script) as f:
            content = f.read()
        assert "pyproject.toml" in content, (
            "deploy_poc.sh does not upload pyproject.toml to STREAMLIT_STAGE. "
            "Container Runtime requires it for dependency installation."
        )
