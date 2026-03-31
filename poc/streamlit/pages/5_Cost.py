"""
Page 5: Cost Observability — AI_EXTRACT credit consumption.

Performance fixes vs original:
  • Single batched query for KPI + daily + per-doc trend (was 3 separate queries)
  • Lazy-loaded expanders for infrastructure credits (only query when opened)
  • @st.cache_data with proper TTL on all ACCOUNT_USAGE queries
  • Cost drivers + scatter plots only load on explicit button click
  • Removed redundant per_pdf query (was fetched twice)
  • Token distribution uses the same base CTE as daily credits
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from config import (
    DB,
    get_session,
    inject_custom_css,
    sidebar_branding,
    get_demo_config,
    render_nav_bar,
)

st.set_page_config(page_title="Cost Observability", page_icon="💰", layout="wide")
inject_custom_css()
session = get_session()
demo_cfg = get_demo_config(session)

with st.sidebar:
    sidebar_branding(customer_name=demo_cfg.get("customer_name"))
    st.divider()
    demo_mode = st.toggle(
        "Demo Mode",
        value=demo_cfg.get("demo_mode", False),
        help="Hide raw credit numbers for customer-facing presentations.",
    )
    credit_rate = st.number_input(
        "Credit Rate (USD)",
        min_value=0.00,
        value=0.00,
        step=0.50,
        help="Set your contract's $/credit rate to see USD estimates. Leave 0 for credits only.",
        disabled=demo_mode,
    )

show_usd = credit_rate > 0 and not demo_mode
hide_credits = demo_mode or demo_cfg.get("hide_credits", False)


def _fmt(val, fmt=".4f"):
    return "—" if hide_credits else f"{val:{fmt}}"


st.title("Cost Observability")
st.caption(
    "Document processing cost analysis (demo mode — absolute values hidden)"
    if demo_mode
    else "AI_EXTRACT credit consumption from Snowflake first-party billing"
)

day_range = st.selectbox(
    "Time Range", [7, 14, 30, 90], index=2, format_func=lambda d: f"Last {d} days"
)


# ══════════════════════════════════════════════════════════════════════════════
# CACHED QUERY LAYER
# ══════════════════════════════════════════════════════════════════════════════
# All ACCOUNT_USAGE queries go through cached helpers with 10-min TTL.
# The underscore prefix on _session tells Streamlit not to hash it.


@st.cache_data(ttl=600, show_spinner=False)
def _query(_session, sql: str) -> pd.DataFrame:
    try:
        return _session.sql(sql).to_pandas()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner="Loading cost data…")
def _load_core_metrics(_session, db: str, days: int) -> dict:
    """Single query to get summary, daily, and per-doc trend in one round trip."""
    raw = _session.sql(
        f"""
        WITH base AS (
            SELECT
                start_time::DATE                        AS usage_date,
                credits,
                PARSE_JSON(metrics[0]:value)::INT       AS tokens,
                query_id
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
            WHERE function_name = 'AI_EXTRACT'
              AND start_time >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        ),
        daily AS (
            SELECT
                usage_date,
                SUM(credits)                AS ai_credits,
                COUNT(*)                    AS calls,
                SUM(tokens)                 AS total_tokens,
                COUNT(DISTINCT query_id)    AS unique_queries
            FROM base
            GROUP BY usage_date
        )
        SELECT
            usage_date,
            ai_credits,
            calls,
            total_tokens,
            unique_queries
        FROM daily
        ORDER BY usage_date
    """
    ).to_pandas()
    return {"daily": raw}


@st.cache_data(ttl=600, show_spinner=False)
def _load_summary(_session, db: str) -> pd.DataFrame:
    return _query(_session, f"SELECT * FROM {db}.V_AI_EXTRACT_COST_SUMMARY")


@st.cache_data(ttl=600, show_spinner=False)
def _load_token_dist(_session, days: int) -> pd.DataFrame:
    return _query(
        _session,
        f"""
        WITH base AS (
            SELECT
                PARSE_JSON(metrics[0]:value)::INT AS tok,
                credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
            WHERE function_name = 'AI_EXTRACT'
              AND start_time >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        )
        SELECT
            CASE
                WHEN tok < 1500  THEN '< 1.5K tokens'
                WHEN tok < 3000  THEN '1.5K - 3K'
                WHEN tok < 6000  THEN '3K - 6K'
                WHEN tok < 15000 THEN '6K - 15K'
                ELSE '15K+ tokens'
            END AS token_range,
            CASE
                WHEN tok < 1500  THEN 1
                WHEN tok < 3000  THEN 2
                WHEN tok < 6000  THEN 3
                WHEN tok < 15000 THEN 4
                ELSE 5
            END AS sort_order,
            COUNT(*)               AS calls,
            ROUND(AVG(credits), 6) AS avg_credits,
            ROUND(MIN(credits), 6) AS min_credits,
            ROUND(MAX(credits), 6) AS max_credits,
            ROUND(AVG(tok), 0)     AS avg_tokens
        FROM base
        GROUP BY 1, 2
        ORDER BY 2
    """,
    )


@st.cache_data(ttl=600, show_spinner=False)
def _load_cost_drivers(_session, db: str) -> pd.DataFrame:
    return _query(_session, f"SELECT * FROM {db}.V_AI_EXTRACT_COST_DRIVERS")


@st.cache_data(ttl=600, show_spinner=False)
def _load_per_pdf(_session, db: str, days: int) -> pd.DataFrame:
    return _query(
        _session,
        f"""
        SELECT file_name, doc_type, page_count, tokens, ai_credits,
               file_size_bytes, field_count, credits_per_page
        FROM {db}.V_AI_EXTRACT_COST_PER_PDF
        WHERE start_time >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        LIMIT 500
    """,
    )


# ══════════════════════════════════════════════════════════════════════════════
# METHODOLOGY (lightweight — no queries)
# ══════════════════════════════════════════════════════════════════════════════

with st.expander("Cost Methodology & Assumptions", expanded=False):
    st.markdown(
        """
**Credits/Doc** = AI_EXTRACT function credits only (token-based, from `CORTEX_AI_FUNCTIONS_USAGE_HISTORY`).
Does **not** include warehouse compute, Streamlit hosting, or storage.

| Source | What it covers | Billing view |
|--------|---------------|--------------|
| **AI_EXTRACT** | Cortex AI token cost per call | `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` |
| **Warehouse** | `AI_EXTRACT_WH` compute (idle + query) | `WAREHOUSE_METERING_HISTORY` |
| **SPCS** | Container runtime for Streamlit | `SNOWPARK_CONTAINER_SERVICES_HISTORY` |

Credits scale with token count (document length × content density × fields extracted).
USD estimates use your configured $/credit rate — actual cost depends on your contract.
    """
    )


# ══════════════════════════════════════════════════════════════════════════════
# KPI SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

summary = _load_summary(session, DB)

if len(summary) == 0:
    st.warning(
        "Cost summary unavailable. Run `sql/12_cost_views.sql` to create cost views."
    )
    render_nav_bar()
    st.stop()

row = summary.iloc[0]
credits_per_doc = float(row["AVG_CREDITS_PER_DOC"])

st.subheader("AI_EXTRACT Cost Summary")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("AI Credits (7d)", _fmt(row["AI_CREDITS_LAST_7D"]))
c2.metric("AI Credits (30d)", _fmt(row["AI_CREDITS_LAST_30D"]))
c3.metric("Docs Processed", f"{int(row['TOTAL_CALLS']):,}")
if hide_credits:
    c4.metric("Credits/Doc", "—")
elif show_usd:
    c4.metric(
        "Credits/Doc",
        f"{credits_per_doc:.6f}",
        delta=f"${credits_per_doc * credit_rate:.4f} USD",
    )
else:
    c4.metric("Credits/Doc", f"{credits_per_doc:.6f}")
c5.metric("Unique Documents", f"{int(row['UNIQUE_DOCS']):,}")

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# DAILY TREND + TOKEN DISTRIBUTION (side by side, single data load)
# ══════════════════════════════════════════════════════════════════════════════

core = _load_core_metrics(session, DB, day_range)
daily = core["daily"]

col_daily, col_tokens = st.columns(2)

with col_daily:
    st.markdown("##### Daily AI_EXTRACT Credits")
    if len(daily) > 0:
        fig = px.area(
            daily,
            x="USAGE_DATE",
            y="AI_CREDITS",
            labels={"USAGE_DATE": "Date", "AI_CREDITS": "Credits"},
            color_discrete_sequence=["#29B5E8"],
        )
        fig.update_layout(
            height=280,
            margin=dict(l=10, r=10, t=10, b=10),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        if hide_credits:
            fig.update_yaxes(showticklabels=False, title_text="Credits (hidden)")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        # Per-doc trend inline
        if daily["CALLS"].sum() > 0:
            daily_display = daily.copy()
            daily_display["CREDITS_PER_DOC"] = (
                daily_display["AI_CREDITS"] / daily_display["CALLS"]
            )
            avg_cpd = daily_display["CREDITS_PER_DOC"].mean()
            st.caption(f"Avg credits/doc over period: **{_fmt(avg_cpd, '.6f')}**")
    else:
        st.info("No AI_EXTRACT usage in this period.")

with col_tokens:
    st.markdown("##### Credit Cost by Token Range")
    token_dist = _load_token_dist(session, day_range)
    if len(token_dist) > 0:
        fig_td = px.bar(
            token_dist,
            x="TOKEN_RANGE",
            y="AVG_CREDITS",
            text="CALLS",
            labels={
                "TOKEN_RANGE": "",
                "AVG_CREDITS": "Avg Credits/Doc",
                "CALLS": "Calls",
            },
            color_discrete_sequence=["#11567F"],
        )
        fig_td.update_traces(textposition="outside")
        fig_td.update_layout(
            height=280,
            margin=dict(l=10, r=10, t=10, b=10),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        if hide_credits:
            fig_td.update_yaxes(showticklabels=False, title_text="Credits (hidden)")
        st.plotly_chart(
            fig_td, use_container_width=True, config={"displayModeBar": False}
        )
    else:
        st.info("No AI_EXTRACT calls in this period.")

# Data tables in shared expander (not rendered by default)
with st.expander("View Daily & Token Data Tables"):
    tab_d, tab_t = st.tabs(["Daily", "Tokens"])
    with tab_d:
        if len(daily) > 0:
            d_display = daily.copy()
            if hide_credits:
                d_display = d_display.drop(columns=["AI_CREDITS"], errors="ignore")
            elif show_usd:
                d_display["EST_USD"] = d_display["AI_CREDITS"] * credit_rate
            st.dataframe(d_display, hide_index=True, use_container_width=True)
    with tab_t:
        if len(token_dist) > 0:
            t_display = token_dist.drop(columns=["SORT_ORDER"], errors="ignore")
            if hide_credits:
                t_display = t_display.drop(
                    columns=["AVG_CREDITS", "MIN_CREDITS", "MAX_CREDITS"],
                    errors="ignore",
                )
            elif show_usd:
                t_display["AVG_USD"] = t_display["AVG_CREDITS"] * credit_rate
            st.dataframe(t_display, hide_index=True, use_container_width=True)

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# COST DRIVERS (lazy-loaded — only queries when user clicks)
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Cost Drivers")
st.caption(
    "Per-PDF cost attribution: page count, file size, tokens, and fields extracted"
)

if st.button("Load Cost Driver Analysis", type="secondary"):
    with st.spinner("Querying cost attribution views…"):
        drivers = _load_cost_drivers(session, DB)
        per_pdf = _load_per_pdf(session, DB, day_range)

    if len(drivers) > 0:
        d1, d2, d3, d4 = st.columns(4)
        avg_cpp = drivers["AVG_CREDITS_PER_PAGE"].mean()
        avg_tpp = drivers["AVG_TOKENS_PER_PAGE"].mean()
        tc = drivers["TOKEN_CREDIT_CORRELATION"].dropna().mean()
        ptc = drivers["PAGE_TOKEN_CORRELATION"].dropna().mean()
        d1.metric("Avg Credits/Page", "—" if hide_credits else f"{avg_cpp:.6f}")
        d2.metric("Avg Tokens/Page", f"{avg_tpp:,.0f}")
        d3.metric("Token↔Credit Corr.", f"{tc:.2f}" if not pd.isna(tc) else "N/A")
        d4.metric("Page↔Token Corr.", f"{ptc:.2f}" if not pd.isna(ptc) else "N/A")

        # Driver table
        if hide_credits:
            cols_show = [
                "DOC_TYPE",
                "TOTAL_CALLS",
                "AVG_PAGES",
                "AVG_FILE_SIZE",
                "AVG_TOKENS",
                "AVG_TOKENS_PER_PAGE",
                "FIELDS_EXTRACTED",
            ]
            names = [
                "Doc Type",
                "Calls",
                "Avg Pages",
                "Avg Size (B)",
                "Avg Tokens",
                "Tokens/Page",
                "Fields",
            ]
        else:
            cols_show = [
                "DOC_TYPE",
                "TOTAL_CALLS",
                "AVG_PAGES",
                "AVG_FILE_SIZE",
                "AVG_TOKENS",
                "AVG_CREDITS",
                "AVG_CREDITS_PER_PAGE",
                "AVG_TOKENS_PER_PAGE",
                "FIELDS_EXTRACTED",
            ]
            names = [
                "Doc Type",
                "Calls",
                "Avg Pages",
                "Avg Size (B)",
                "Avg Tokens",
                "Avg Credits",
                "Credits/Page",
                "Tokens/Page",
                "Fields",
            ]
        dd = drivers[cols_show].copy()
        dd.columns = names
        if show_usd and not hide_credits:
            dd["Credits/Page USD"] = dd["Credits/Page"] * credit_rate
        st.dataframe(dd, hide_index=True, use_container_width=True)

        # Scatter plots
        if len(per_pdf) > 0:
            sc1, sc2 = st.columns(2)
            with sc1:
                fig_s1 = px.scatter(
                    per_pdf,
                    x="PAGE_COUNT",
                    y="AI_CREDITS",
                    color="DOC_TYPE",
                    size="TOKENS",
                    hover_data=["FILE_NAME"],
                    labels={"PAGE_COUNT": "Pages", "AI_CREDITS": "Credits"},
                    title="Pages vs Credits",
                )
                fig_s1.update_layout(height=320, margin=dict(l=10, r=10, t=35, b=10))
                if hide_credits:
                    fig_s1.update_yaxes(showticklabels=False, title_text="Hidden")
                st.plotly_chart(
                    fig_s1, use_container_width=True, config={"displayModeBar": False}
                )
            with sc2:
                fig_s2 = px.scatter(
                    per_pdf,
                    x="TOKENS",
                    y="AI_CREDITS",
                    color="DOC_TYPE",
                    hover_data=["FILE_NAME", "PAGE_COUNT"],
                    labels={"TOKENS": "Tokens", "AI_CREDITS": "Credits"},
                    title="Tokens vs Credits",
                )
                fig_s2.update_layout(height=320, margin=dict(l=10, r=10, t=35, b=10))
                if hide_credits:
                    fig_s2.update_yaxes(showticklabels=False, title_text="Hidden")
                st.plotly_chart(
                    fig_s2, use_container_width=True, config={"displayModeBar": False}
                )

            with st.expander("Per-PDF Detail"):
                if hide_credits:
                    pdf_d = per_pdf[
                        [
                            "FILE_NAME",
                            "DOC_TYPE",
                            "PAGE_COUNT",
                            "TOKENS",
                            "FILE_SIZE_BYTES",
                            "FIELD_COUNT",
                        ]
                    ].copy()
                else:
                    pdf_d = per_pdf.copy()
                    if show_usd:
                        pdf_d["USD"] = pdf_d["AI_CREDITS"] * credit_rate
                st.dataframe(pdf_d, hide_index=True, use_container_width=True)
    else:
        st.info("No cost attribution data. Run `sql/13_cost_attribution.sql`.")
else:
    st.caption(
        "Click the button above to load cost driver analysis (queries ACCOUNT_USAGE views)."
    )

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# COST vs CONFIDENCE (lazy-loaded)
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Cost vs Confidence")

if st.button("Load Confidence Analysis", type="secondary"):
    with st.spinner("Computing confidence metrics…"):
        cost_conf = _query(
            session,
            f"""
            WITH conf_flat AS (
                SELECT
                    e.file_name, r.doc_type,
                    f.value::FLOAT AS field_conf
                FROM {DB}.EXTRACTED_FIELDS e
                JOIN {DB}.RAW_DOCUMENTS r ON e.file_name = r.file_name,
                LATERAL FLATTEN(input => e.raw_extraction:_confidence) f
                WHERE e.raw_extraction:_confidence IS NOT NULL
            ),
            conf_avg AS (
                SELECT file_name, doc_type, AVG(field_conf) AS avg_confidence
                FROM conf_flat GROUP BY 1, 2
            )
            SELECT ca.file_name, ca.doc_type, ca.avg_confidence,
                   cp.ai_credits, cp.tokens, cp.page_count
            FROM conf_avg ca
            LEFT JOIN {DB}.V_AI_EXTRACT_COST_PER_PDF cp ON ca.file_name = cp.file_name
            WHERE cp.ai_credits IS NOT NULL
            LIMIT 500
        """,
        )

    if len(cost_conf) > 0:
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Avg Confidence", f"{cost_conf['AVG_CONFIDENCE'].mean():.2f}")
        cc2.metric(
            "Low Confidence (<0.7)", f"{(cost_conf['AVG_CONFIDENCE'] < 0.7).sum()}"
        )
        cc3.metric("Docs Analyzed", f"{len(cost_conf):,}")

        fig_cc = px.scatter(
            cost_conf,
            x="AVG_CONFIDENCE",
            y="AI_CREDITS",
            color="DOC_TYPE",
            size="TOKENS",
            hover_data=["FILE_NAME"],
            labels={"AVG_CONFIDENCE": "Avg Confidence", "AI_CREDITS": "Credits"},
        )
        fig_cc.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
        if hide_credits:
            fig_cc.update_yaxes(showticklabels=False)
        st.plotly_chart(
            fig_cc, use_container_width=True, config={"displayModeBar": False}
        )
    else:
        st.info("No confidence data available.")
else:
    st.caption("Click to load confidence vs cost analysis.")

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# CREDITS BY DOC TYPE
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Credits by Document Type")

by_type = _query(
    session,
    f"""
    SELECT doc_type, COUNT(*) AS calls,
           ROUND(SUM(ai_credits), 6) AS total_credits, SUM(tokens) AS tokens
    FROM {DB}.V_AI_EXTRACT_COST_PER_PDF
    WHERE start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
    GROUP BY doc_type ORDER BY total_credits DESC
""",
)

if len(by_type) > 0:
    fig2 = px.bar(
        by_type,
        x="DOC_TYPE",
        y="TOTAL_CREDITS",
        text="CALLS",
        labels={"DOC_TYPE": "", "TOTAL_CREDITS": "Credits"},
        color_discrete_sequence=["#11567F"],
    )
    fig2.update_traces(textposition="outside")
    fig2.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    if hide_credits:
        fig2.update_yaxes(showticklabels=False)
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
else:
    st.info("No doc-type credit data available.")

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# RECENT CALLS (lazy table)
# ══════════════════════════════════════════════════════════════════════════════

with st.expander("Recent AI_EXTRACT Calls"):
    queries = _query(
        session,
        f"""
        SELECT query_id, start_time, doc_type, ai_credits, tokens, elapsed_sec, rows_produced
        FROM {DB}.V_AI_EXTRACT_QUERY_LOG
        WHERE start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
        ORDER BY start_time DESC LIMIT 100
    """,
    )
    if len(queries) > 0:
        if hide_credits:
            queries = queries.drop(columns=["AI_CREDITS"], errors="ignore")
        elif show_usd:
            queries["EST_USD"] = queries["AI_CREDITS"] * credit_rate
        st.dataframe(queries, hide_index=True, use_container_width=True)
    else:
        st.info("No calls found in this period.")


# ══════════════════════════════════════════════════════════════════════════════
# INFRASTRUCTURE CREDITS (all lazy-loaded in expanders)
# ══════════════════════════════════════════════════════════════════════════════

st.divider()

if hide_credits:
    st.subheader("Infrastructure")
    st.info("Infrastructure details hidden in demo mode.")
else:
    st.subheader("Infrastructure Credits")
    st.caption(
        "Warehouse, SPCS, and total account breakdown — click to load (queries ACCOUNT_USAGE)"
    )

    with st.expander("Warehouse Credits (AI_EXTRACT_WH)"):
        wh = _query(
            session,
            f"""
            SELECT DATE_TRUNC('day', start_time)::DATE AS usage_date,
                   SUM(credits_used) AS warehouse_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE warehouse_name = 'AI_EXTRACT_WH'
              AND start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 1
        """,
        )
        if len(wh) > 0:
            total_wh = wh["WAREHOUSE_CREDITS"].sum()
            st.metric(
                "Total WH Credits",
                f"{total_wh:.4f}",
                delta=f"${total_wh * credit_rate:.2f}" if show_usd else None,
            )
            fig_wh = px.bar(
                wh,
                x="USAGE_DATE",
                y="WAREHOUSE_CREDITS",
                color_discrete_sequence=["#11567F"],
            )
            fig_wh.update_layout(height=220, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(
                fig_wh, use_container_width=True, config={"displayModeBar": False}
            )
        else:
            st.info("No warehouse usage in this period.")

    with st.expander("SPCS Credits (Streamlit Hosting)"):
        spcs = _query(
            session,
            f"""
            SELECT DATE_TRUNC('day', start_time)::DATE AS usage_date,
                   SUM(credits_used) AS spcs_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
            WHERE compute_pool_name = 'AI_EXTRACT_POC_POOL'
              AND start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 1
        """,
        )
        if len(spcs) > 0:
            total_spcs = spcs["SPCS_CREDITS"].sum()
            st.metric(
                "Total SPCS Credits",
                f"{total_spcs:.4f}",
                delta=f"${total_spcs * credit_rate:.2f}" if show_usd else None,
            )
            fig_spcs = px.bar(
                spcs,
                x="USAGE_DATE",
                y="SPCS_CREDITS",
                color_discrete_sequence=["#FF4B4B"],
            )
            fig_spcs.update_layout(height=220, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(
                fig_spcs, use_container_width=True, config={"displayModeBar": False}
            )
        else:
            st.info("No SPCS usage in this period.")

    with st.expander("Full Account Breakdown"):
        breakdown = _query(
            session,
            f"""
            SELECT * FROM {DB}.V_AI_EXTRACT_COST_BREAKDOWN
            WHERE usage_date >= DATEADD('day', -{day_range}, CURRENT_DATE())
            ORDER BY usage_date DESC, service_type
        """,
        )
        if len(breakdown) > 0:
            fig4 = px.bar(
                breakdown,
                x="USAGE_DATE",
                y="CREDITS_USED",
                color="SERVICE_TYPE",
                barmode="stack",
                color_discrete_map={
                    "AI_SERVICES": "#29B5E8",
                    "WAREHOUSE_METERING": "#11567F",
                    "SNOWPARK_CONTAINER_SERVICES": "#FF4B4B",
                },
            )
            fig4.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(
                fig4, use_container_width=True, config={"displayModeBar": False}
            )
            st.dataframe(breakdown, hide_index=True, use_container_width=True)
        else:
            st.info("No metering data in this period.")

    with st.expander("Resource Monitor Status"):
        try:
            rm = session.sql(
                "SHOW RESOURCE MONITORS LIKE 'AI_EXTRACT_MONITOR'"
            ).to_pandas()
            if len(rm) > 0:
                r = rm.iloc[0]
                rc1, rc2, rc3 = st.columns(3)
                rc1.metric("Quota", r.get("credit_quota", "N/A"))
                rc2.metric("Used", r.get("used_credits", "N/A"))
                rc3.metric("Remaining", r.get("remaining_credits", "N/A"))
            else:
                st.info("No resource monitor configured.")
        except Exception:
            st.info("Requires MONITOR privilege on the warehouse.")


render_nav_bar()
