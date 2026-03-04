"""
Runtime environment config — reads CURRENT_DATABASE() and CURRENT_SCHEMA()
at startup so the same source code works in any Snowflake account.

Usage in any page:
    from config import DB, STAGE
    conn.query(f"SELECT * FROM {DB}.EXTRACTED_FIELDS")
    stage_path = f"@{STAGE}/{file_name}"
"""

import streamlit as st

_conn = st.connection("snowflake")
_ctx = _conn.query(
    "SELECT CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS sch", ttl=600
).iloc[0]

DB = f"{_ctx['DB']}.{_ctx['SCH']}"
STAGE = f"{_ctx['DB']}.{_ctx['SCH']}.DOCUMENT_STAGE"
