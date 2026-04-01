"""
Page 7: Claude PDF Analysis — Compare Cortex AI_EXTRACT vs Anthropic Claude
extraction on the same documents, side by side.

Gracefully handles missing SP / tables with setup guides.
"""

import json
import streamlit as st
import pandas as pd
from config import (
    DB, STAGE, get_session, get_doc_types, get_doc_type_config,
    _parse_variant,
    inject_custom_css, sidebar_branding, render_nav_bar,
)

st.set_page_config(page_title="Claude PDF Analysis", page_icon="🤖", layout="wide")
inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

st.title("Claude PDF Analysis")
st.caption(
    "Compare Cortex AI_EXTRACT vs Anthropic Claude extractions side by side — "
    "validate accuracy, catch edge cases, build confidence."
)


# ══════════════════════════════════════════════════════════════════════════════
# AVAILABILITY CHECKS
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=600, show_spinner=False)
def _check_sp_exists(_session, db: str) -> bool:
    """Check if the Claude extraction SP is deployed and callable."""
    try:
        result = _session.sql(
            f"SHOW PROCEDURES LIKE 'CLAUDE_EXTRACT_FROM_STAGE' IN {db}"
        ).collect()
        return len(result) > 0
    except Exception:
        return False


@st.cache_data(ttl=600, show_spinner=False)
def _check_table_exists(_session, full_table: str) -> bool:
    try:
        _session.sql(f"SELECT 1 FROM {full_table} LIMIT 0").collect()
        return True
    except Exception:
        return False


claude_sp_ready = _check_sp_exists(session, DB)
conversations_table = f"{DB}.ANTHROPIC_CONVERSATIONS"
conversations_ready = _check_table_exists(session, conversations_table)


# ══════════════════════════════════════════════════════════════════════════════
# SETUP GUIDES
# ══════════════════════════════════════════════════════════════════════════════

SP_SETUP_SQL = f"""
-- ══════════════════════════════════════════════════════════════
-- Claude extraction SP — calls Anthropic API via external access
-- Prerequisites:
--   1. External access integration for api.anthropic.com
--   2. Secret with your Anthropic API key
--   3. USAGE grant on both to the SP owner role
-- ══════════════════════════════════════════════════════════════

-- Step 1: Network rule
CREATE OR REPLACE NETWORK RULE {DB}.ANTHROPIC_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.anthropic.com');

-- Step 2: Secret (replace with your key)
CREATE OR REPLACE SECRET {DB}.ANTHROPIC_API_KEY
    TYPE = GENERIC_STRING
    SECRET_STRING = 'sk-ant-your-key-here';

-- Step 3: External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ANTHROPIC_ACCESS
    ALLOWED_NETWORK_RULES = ({DB}.ANTHROPIC_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = ({DB}.ANTHROPIC_API_KEY)
    ENABLED = TRUE;

-- Step 4: Stored procedure
CREATE OR REPLACE PROCEDURE {DB}.CLAUDE_EXTRACT_FROM_STAGE(
    STAGE_PATH VARCHAR,
    EXTRACTION_PROMPT VARCHAR,
    DOC_TYPE VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (ANTHROPIC_ACCESS)
SECRETS = ('api_key' = {DB}.ANTHROPIC_API_KEY)
AS
$$
import _snowflake
import json
import base64
import requests

def run(session, stage_path: str, extraction_prompt: str, doc_type: str):
    # Read PDF from stage
    pdf_bytes = session.file.get_stream(stage_path).read()
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode('utf-8')

    api_key = _snowflake.get_generic_secret_string('api_key')

    response = requests.post(
        'https://api.anthropic.com/v1/messages',
        headers={{
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
        }},
        json={{
            'model': 'claude-sonnet-4-20250514',
            'max_tokens': 4096,
            'messages': [{{
                'role': 'user',
                'content': [
                    {{
                        'type': 'document',
                        'source': {{
                            'type': 'base64',
                            'media_type': 'application/pdf',
                            'data': pdf_b64,
                        }},
                    }},
                    {{
                        'type': 'text',
                        'text': extraction_prompt + '\\n\\nReturn ONLY valid JSON with no markdown formatting.',
                    }},
                ],
            }}],
        }},
        timeout=120,
    )
    response.raise_for_status()
    data = response.json()

    text = data['content'][0]['text']
    # Strip markdown fences if present
    if text.startswith('```'):
        text = text.split('\\n', 1)[1] if '\\n' in text else text[3:]
        if text.endswith('```'):
            text = text[:-3]
        text = text.strip()

    try:
        extraction = json.loads(text)
    except json.JSONDecodeError:
        extraction = {{'raw_text': text}}

    return {{
        'extraction': extraction,
        'doc_type': doc_type,
        'model': data.get('model', 'unknown'),
        'input_tokens': data.get('usage', {{}}).get('input_tokens', 0),
        'output_tokens': data.get('usage', {{}}).get('output_tokens', 0),
    }}
$$;

-- Step 5: Grant (adjust role as needed)
-- GRANT USAGE ON PROCEDURE {DB}.CLAUDE_EXTRACT_FROM_STAGE(VARCHAR, VARCHAR, VARCHAR)
--   TO ROLE your_app_role;
"""

CONVERSATIONS_TABLE_SQL = f"""
-- Claude API usage tracking table
CREATE TABLE IF NOT EXISTS {DB}.ANTHROPIC_CONVERSATIONS (
    conversation_id VARCHAR DEFAULT UUID_STRING(),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    model           VARCHAR,
    role            VARCHAR,       -- 'user' or 'assistant'
    input_tokens    NUMBER,
    output_tokens   NUMBER,
    doc_type        VARCHAR,
    file_name       VARCHAR,
    extraction      VARIANT
);
"""


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════

st.divider()

tab_compare, tab_adhoc, tab_history, tab_setup = st.tabs([
    "📊 Compare Extractions",
    "📄 Ad-hoc Analysis",
    "📈 Usage History",
    "🔧 Setup",
])


# ── Tab 1: Side-by-side comparison ────────────────────────────────────────────

with tab_compare:
    st.subheader("Side-by-Side: Cortex vs Claude")

    if not claude_sp_ready:
        st.warning(
            "The `CLAUDE_EXTRACT_FROM_STAGE` stored procedure is not available. "
            "Go to the **Setup** tab for deployment instructions."
        )
        st.info("Cortex-only comparison is still available below — you can review AI_EXTRACT results for any document.")
        st.divider()

    doc_types = get_doc_types(session)
    sel_type = st.selectbox("Document Type", doc_types, key="cmp_type")

    try:
        docs_df = session.sql(
            f"""
            SELECT DISTINCT rd.file_name
            FROM {DB}.RAW_DOCUMENTS rd
                JOIN {DB}.EXTRACTED_FIELDS ef ON rd.file_name = ef.file_name
            WHERE rd.doc_type = ?
            ORDER BY rd.file_name
            LIMIT 50
            """,
            params=[sel_type],
        ).to_pandas()
    except Exception as e:
        st.error(f"Could not load documents: {e}")
        docs_df = pd.DataFrame()

    if docs_df.empty:
        st.info("No extracted documents found for this type.")
    else:
        sel_doc = st.selectbox("Select Document", docs_df["FILE_NAME"].tolist(), key="cmp_doc")

        if st.button("Run Comparison", type="primary"):
            cfg = get_doc_type_config(session, sel_type)
            prompt = cfg.get("extraction_prompt", "Extract all fields from this document as JSON.") if cfg else "Extract all fields."

            col1, col2 = st.columns(2)

            # ── Cortex results ────────────────────────────────────────
            cortex_data = {}
            with col1:
                st.markdown("#### ❄️ Cortex AI_EXTRACT")
                with st.spinner("Loading Cortex results..."):
                    try:
                        cortex_df = session.sql(
                            f"SELECT raw_extraction FROM {DB}.EXTRACTED_FIELDS WHERE file_name = ? LIMIT 1",
                            params=[sel_doc],
                        ).to_pandas()
                        if not cortex_df.empty and cortex_df["RAW_EXTRACTION"].iloc[0]:
                            cortex_data = _parse_variant(cortex_df["RAW_EXTRACTION"].iloc[0]) or {}
                            # Remove internal keys for display
                            display_cortex = {k: v for k, v in cortex_data.items()
                                              if k not in ("_confidence", "_validation_warnings")}
                            st.json(display_cortex)
                        else:
                            st.warning("No Cortex extraction found for this document.")
                    except Exception as e:
                        st.error(f"Cortex query failed: {e}")

            # ── Claude results ────────────────────────────────────────
            claude_data = {}
            with col2:
                st.markdown("#### 🤖 Claude")
                if not claude_sp_ready:
                    st.info("Claude SP not deployed. See the Setup tab.")
                else:
                    with st.spinner("Calling Claude API..."):
                        try:
                            stage_path = f"@{STAGE}/{sel_doc}"
                            claude_result = session.sql(
                                f"CALL {DB}.CLAUDE_EXTRACT_FROM_STAGE(?, ?, ?)",
                                params=[stage_path, prompt, sel_type],
                            ).collect()
                            if claude_result:
                                raw = claude_result[0][0]
                                if isinstance(raw, str):
                                    raw = json.loads(raw)
                                claude_data = raw.get("extraction", raw)
                                if isinstance(claude_data, dict):
                                    st.json(claude_data)
                                else:
                                    st.markdown(str(claude_data))
                                    claude_data = {}
                                in_tok = raw.get("input_tokens", 0)
                                out_tok = raw.get("output_tokens", 0)
                                st.caption(f"Tokens: {in_tok + out_tok:,} ({in_tok:,} in / {out_tok:,} out)")
                            else:
                                st.warning("Claude returned no result.")
                        except Exception as e:
                            st.error(f"Claude extraction failed: {e}")

            # ── Field comparison ──────────────────────────────────────
            if cortex_data and claude_data:
                st.divider()
                st.subheader("Field-by-Field Comparison")

                # Flatten and remove internal keys
                cortex_flat = {k: v for k, v in cortex_data.items()
                               if k not in ("_confidence", "_validation_warnings")}
                claude_flat = claude_data if isinstance(claude_data, dict) else {}

                all_keys = sorted(set(list(cortex_flat.keys()) + list(claude_flat.keys())))
                rows = []
                for k in all_keys:
                    cv = str(cortex_flat.get(k, "—"))
                    clv = str(claude_flat.get(k, "—"))
                    match = "✅" if cv.strip().lower() == clv.strip().lower() else "⚠️"
                    rows.append({"Field": k, "Cortex": cv, "Claude": clv, "Match": match})

                matched = sum(1 for r in rows if r["Match"] == "✅")
                mc1, mc2 = st.columns(2)
                mc1.metric("Agreement", f"{matched}/{len(rows)} fields")
                mc2.metric("Match Rate", f"{matched / max(len(rows), 1) * 100:.0f}%")

                st.dataframe(
                    pd.DataFrame(rows),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Match": st.column_config.TextColumn("Match", width="small"),
                    },
                )


# ── Tab 2: Ad-hoc Claude Analysis ────────────────────────────────────────────

with tab_adhoc:
    st.subheader("Ask Claude About a Document")

    if not claude_sp_ready:
        st.warning(
            "The `CLAUDE_EXTRACT_FROM_STAGE` stored procedure is not deployed. "
            "Go to the **Setup** tab for instructions."
        )
    else:
        doc_types2 = get_doc_types(session)
        sel_type2 = st.selectbox("Document Type", doc_types2, key="adhoc_type")

        try:
            docs_df2 = session.sql(
                f"SELECT file_name FROM {DB}.RAW_DOCUMENTS WHERE doc_type = ? ORDER BY created_at DESC LIMIT 50",
                params=[sel_type2],
            ).to_pandas()
        except Exception:
            docs_df2 = pd.DataFrame()

        if docs_df2.empty:
            st.info("No documents found for this type.")
        else:
            sel_doc2 = st.selectbox("Select Document", docs_df2["FILE_NAME"].tolist(), key="adhoc_doc")
            custom_prompt = st.text_area(
                "Custom Prompt",
                value=(
                    "Analyze this document and provide:\n"
                    "1. A brief summary\n"
                    "2. Key data points as JSON\n"
                    "3. Any anomalies or items needing human review"
                ),
                height=120,
            )

            if st.button("Analyze with Claude", type="primary"):
                with st.spinner("Claude is reading the document..."):
                    try:
                        stage_path = f"@{STAGE}/{sel_doc2}"
                        result = session.sql(
                            f"CALL {DB}.CLAUDE_EXTRACT_FROM_STAGE(?, ?, ?)",
                            params=[stage_path, custom_prompt, sel_type2],
                        ).collect()
                        if result:
                            raw = result[0][0]
                            if isinstance(raw, str):
                                raw = json.loads(raw)
                            extraction = raw.get("extraction", raw)
                            if isinstance(extraction, dict):
                                st.json(extraction)
                            else:
                                st.markdown(str(extraction))
                            c1, c2 = st.columns(2)
                            c1.metric("Input Tokens", f"{raw.get('input_tokens', 0):,}")
                            c2.metric("Output Tokens", f"{raw.get('output_tokens', 0):,}")
                        else:
                            st.error("Claude returned no result.")
                    except Exception as e:
                        st.error(f"Analysis failed: {e}")


# ── Tab 3: Usage History ──────────────────────────────────────────────────────

with tab_history:
    st.subheader("Claude API Usage Tracking")

    if not conversations_ready:
        st.info(
            f"The `{conversations_table}` table does not exist yet. "
            "Go to the **Setup** tab to create it."
        )
        st.caption(
            "Once created, Claude extractions will log token usage and cost here. "
            "You can also track usage via the Cost page's infrastructure section."
        )
    else:
        try:
            cost_df = session.sql(f"""
                SELECT
                    DATE_TRUNC('day', created_at) AS day,
                    model,
                    COUNT(DISTINCT conversation_id) AS calls,
                    SUM(input_tokens)  AS input_tokens,
                    SUM(output_tokens) AS output_tokens,
                    ROUND(SUM(input_tokens) * 3.0 / 1000000
                        + SUM(output_tokens) * 15.0 / 1000000, 4) AS cost_usd
                FROM {conversations_table}
                WHERE role = 'assistant'
                GROUP BY day, model
                ORDER BY day DESC
            """).to_pandas()

            if cost_df.empty:
                st.info("No Claude API usage recorded yet.")
            else:
                c1, c2, c3 = st.columns(3)
                c1.metric("Total Spend", f"${cost_df['COST_USD'].sum():.4f}")
                c2.metric("Total Calls", f"{int(cost_df['CALLS'].sum()):,}")
                c3.metric("Total Tokens", f"{int(cost_df['INPUT_TOKENS'].sum() + cost_df['OUTPUT_TOKENS'].sum()):,}")

                st.bar_chart(cost_df.set_index("DAY")["COST_USD"])
                st.dataframe(cost_df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Could not load usage data: {e}")


# ── Tab 4: Setup ──────────────────────────────────────────────────────────────

with tab_setup:
    st.subheader("Setup Guide")
    st.caption("Deploy the Claude extraction SP and tracking table")

    # Status checks
    st.markdown("##### Current Status")
    sc1, sc2 = st.columns(2)
    with sc1:
        if claude_sp_ready:
            st.success("✅ `CLAUDE_EXTRACT_FROM_STAGE` — deployed")
        else:
            st.error("❌ `CLAUDE_EXTRACT_FROM_STAGE` — not found")
    with sc2:
        if conversations_ready:
            st.success(f"✅ `{conversations_table}` — exists")
        else:
            st.error(f"❌ `{conversations_table}` — not found")

    st.divider()

    # SP setup
    st.markdown("##### 1. Claude Extraction Stored Procedure")
    st.markdown(
        "This SP reads a PDF from your Snowflake stage, sends it to the Anthropic API "
        "via external access, and returns structured JSON. Requires a network rule, "
        "API key secret, and external access integration."
    )

    with st.expander("View SP Creation SQL", expanded=not claude_sp_ready):
        st.code(SP_SETUP_SQL, language="sql")
        st.warning(
            "Replace `sk-ant-your-key-here` with your actual Anthropic API key. "
            "Adjust the role grants as needed for your environment."
        )

    st.divider()

    # Conversations table setup
    st.markdown("##### 2. API Usage Tracking Table")
    st.markdown(
        "Optional table to log Claude API token usage and costs. "
        "The SP above can be extended to INSERT into this table after each call."
    )

    with st.expander("View Table Creation SQL", expanded=not conversations_ready):
        st.code(CONVERSATIONS_TABLE_SQL, language="sql")

    st.divider()

    # Troubleshooting
    st.markdown("##### Troubleshooting")

    with st.expander("SP exists but calls fail"):
        st.markdown("""
**Common failure modes:**

1. **`External access integration not found`** — The integration name in the SP must match exactly.
   Check with: `SHOW EXTERNAL ACCESS INTEGRATIONS;`

2. **`Secret not found`** — The secret must be in the same schema as the SP, or use a fully qualified name.
   Check with: `SHOW SECRETS IN {DB};`

3. **`Network policy blocks egress`** — Your account may have a network policy that blocks outbound HTTPS.
   The network rule must allow `api.anthropic.com:443`.

4. **`401 Unauthorized`** — The API key in the secret is invalid or expired.
   Update with: `ALTER SECRET {DB}.ANTHROPIC_API_KEY SET SECRET_STRING = 'sk-ant-new-key';`

5. **`Timeout`** — Large PDFs may exceed the 120-second timeout. Try with a smaller test document first.

6. **`Permission denied`** — The role running the SP needs:
   - `USAGE` on the external access integration
   - `READ` on the secret
   - `USAGE` on the stage
        """)

    with st.expander("Test the SP manually"):
        st.code(f"""
-- Quick test with a known document
CALL {DB}.CLAUDE_EXTRACT_FROM_STAGE(
    '@{STAGE}/your_test_file.pdf',
    'Extract vendor name, invoice number, and total amount as JSON.',
    'INVOICE'
);
        """, language="sql")


render_nav_bar()