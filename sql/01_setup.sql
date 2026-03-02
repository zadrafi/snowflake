-- =============================================================================
-- 01_setup.sql — Create database, schema, warehouse, stage, and compute pool
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Database and schema
CREATE DATABASE IF NOT EXISTS AP_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS AP_DEMO_DB.AP;

-- Warehouse for extraction and queries
CREATE WAREHOUSE IF NOT EXISTS AP_DEMO_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE DATABASE AP_DEMO_DB;
USE SCHEMA AP;
USE WAREHOUSE AP_DEMO_WH;

-- Internal stage for PDF invoices (SSE encryption required for AI_EXTRACT)
CREATE STAGE IF NOT EXISTS INVOICE_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for PDF invoices to be processed by AI_EXTRACT';

-- Internal stage for Streamlit app files
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit in Snowflake app files';

-- Compute pool for Streamlit container runtime
CREATE COMPUTE POOL IF NOT EXISTS AP_DEMO_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_SUSPEND_SECS = 300
    AUTO_RESUME = TRUE;

-- Grant usage so the Streamlit app can query
GRANT USAGE ON DATABASE AP_DEMO_DB TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA AP_DEMO_DB.AP TO ROLE PUBLIC;
GRANT USAGE ON WAREHOUSE AP_DEMO_WH TO ROLE PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA AP_DEMO_DB.AP TO ROLE PUBLIC;
GRANT SELECT ON ALL VIEWS IN SCHEMA AP_DEMO_DB.AP TO ROLE PUBLIC;
GRANT READ ON STAGE AP_DEMO_DB.AP.INVOICE_STAGE TO ROLE PUBLIC;
