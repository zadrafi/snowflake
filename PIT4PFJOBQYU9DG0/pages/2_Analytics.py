"""
Page 2: Analytics — Comprehensive document analytics for AP / finance teams.

Sections:
  1. KPI bar (top-line summary)
  2. Filters (doc type, date range, sender)
  3. Spend Analysis (vendor breakdown, monthly trend, aging)
  4. Pipeline Throughput (processing volume, timing, failures)
  5. Extraction Quality (accuracy rates, field-level stats)
  6. Line Item Analysis (top items, category spend, patterns)

All sections support CSV export via download buttons.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from config import (
    DB,
    get_session,
    get_doc_type_labels,
    get_doc_types,
    inject_custom_css,
    sidebar_branding,
    render_nav_bar,
)

st.set_page_config(page_title="Analytics", page_icon="📊", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

st.title("Document Analytics")
st.caption("Spend insights, extraction quality, and pipeline health")


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════


def _download_csv(df: pd.DataFrame, filename: str, label: str = "Download CSV"):
    """Render a CSV download button for a dataframe."""
    if len(df) > 0:
        csv = df.to_csv(index=False)
        st.download_button(label, csv, file_name=filename, mime="text/csv")


def _safe_query(sql: str, params: list = None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame, or empty on error."""
    try:
        return session.sql(sql, params=params or []).to_pandas()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


def _plotly_defaults(fig, height=350):
    """Apply consistent Plotly styling."""
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.06)")
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# FILTERS
# ══════════════════════════════════════════════════════════════════════════════

doc_types = get_doc_types(session)

with st.container():
    fc1, fc2, fc3, fc4 = st.columns([2, 2, 3, 3])

    with fc1:
        selected_type = st.selectbox("Document Type", ["ALL"] + doc_types, index=0)

    with fc2:
        date_preset = st.selectbox(
            "Date Range",
            [
                "All Time",
                "Last 30 Days",
                "Last 90 Days",
                "Last 6 Months",
                "Last 12 Months",
                "Custom",
            ],
        )

    # Compute date bounds
    date_start = None
    date_end = None
    if date_preset == "Last 30 Days":
        date_start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    elif date_preset == "Last 90 Days":
        date_start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    elif date_preset == "Last 6 Months":
        date_start = (datetime.now() - timedelta(days=183)).strftime("%Y-%m-%d")
    elif date_preset == "Last 12 Months":
        date_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    elif date_preset == "Custom":
        with fc3:
            date_start = st.date_input(
                "From", value=datetime.now() - timedelta(days=90)
            )
            date_start = date_start.strftime("%Y-%m-%d") if date_start else None
        with fc4:
            date_end = st.date_input("To", value=datetime.now())
            date_end = date_end.strftime("%Y-%m-%d") if date_end else None

labels = get_doc_type_labels(
    session, selected_type if selected_type != "ALL" else "INVOICE"
)
sender_label = labels.get("sender_label", "Sender")
amount_label = labels.get("amount_label", "Total Amount")

# Build reusable WHERE fragments
type_clauses = []
type_params = []
if selected_type != "ALL":
    type_clauses.append("rd.doc_type = ?")
    type_params.append(selected_type)
if date_start:
    type_clauses.append("ef.field_4 >= ?")
    type_params.append(date_start)
if date_end:
    type_clauses.append("ef.field_4 <= ?")
    type_params.append(date_end)

where_and = (" AND " + " AND ".join(type_clauses)) if type_clauses else ""
where_clause = ("WHERE " + " AND ".join(type_clauses)) if type_clauses else ""

# Simpler version for tables without ef alias
type_and_rd = ""
type_params_rd = []
if selected_type != "ALL":
    type_and_rd = "AND rd.doc_type = ?"
    type_params_rd = [selected_type]


# ══════════════════════════════════════════════════════════════════════════════
# KPI BAR
# ══════════════════════════════════════════════════════════════════════════════

kpi_df = _safe_query(
    f"""
    SELECT
        COUNT(*)                                    AS total_docs,
        COUNT(DISTINCT ef.field_1)                  AS unique_senders,
        COALESCE(SUM(ef.field_10), 0)               AS total_spend,
        COALESCE(AVG(ef.field_10), 0)               AS avg_doc_amount,
        COALESCE(MAX(ef.field_10), 0)               AS max_doc_amount,
        MIN(ef.field_4)                             AS earliest_date,
        MAX(ef.field_4)                             AS latest_date
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    {"WHERE " + " AND ".join(type_clauses) if type_clauses else ""}
""",
    type_params,
)

if len(kpi_df) > 0 and kpi_df.iloc[0]["TOTAL_DOCS"] > 0:
    k = kpi_df.iloc[0]
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Documents", f"{int(k['TOTAL_DOCS']):,}")
    k2.metric(f"Unique {sender_label}s", f"{int(k['UNIQUE_SENDERS']):,}")
    k3.metric("Total Spend", f"${k['TOTAL_SPEND']:,.0f}")
    k4.metric("Avg per Document", f"${k['AVG_DOC_AMOUNT']:,.0f}")
    k5.metric("Largest Document", f"${k['MAX_DOC_AMOUNT']:,.0f}")
else:
    st.info("No extracted data found. Run the extraction pipeline first.")
    render_nav_bar()
    st.stop()

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: SPEND ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Spend Analysis")

spend_tab1, spend_tab2, spend_tab3 = st.tabs(
    [
        f"By {sender_label}",
        "Monthly Trend",
        "Aging",
    ]
)

# ── By Sender ─────────────────────────────────────────────────────────────────
with spend_tab1:
    vendor_df = _safe_query(
        f"""
        SELECT
            ef.field_1          AS vendor_name,
            COUNT(*)            AS document_count,
            SUM(ef.field_10)    AS total_amount,
            AVG(ef.field_10)    AS avg_amount,
            MIN(ef.field_4)     AS first_doc,
            MAX(ef.field_4)     AS last_doc
        FROM {DB}.EXTRACTED_FIELDS ef
            JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
        WHERE ef.field_1 IS NOT NULL {where_and}
        GROUP BY ef.field_1
        ORDER BY total_amount DESC
        LIMIT 20
    """,
        type_params,
    )

    if len(vendor_df) > 0:
        fig = px.bar(
            vendor_df,
            x="TOTAL_AMOUNT",
            y="VENDOR_NAME",
            orientation="h",
            color="TOTAL_AMOUNT",
            color_continuous_scale="Blues",
            labels={"TOTAL_AMOUNT": f"{amount_label} ($)", "VENDOR_NAME": sender_label},
            text_auto="$.2s",
            custom_data=["DOCUMENT_COUNT", "AVG_AMOUNT"],
        )
        fig.update_traces(
            hovertemplate=(
                f"<b>%{{y}}</b><br>"
                f"Total: $%{{x:,.2f}}<br>"
                f"Documents: %{{customdata[0]}}<br>"
                f"Avg: $%{{customdata[1]:,.2f}}"
                f"<extra></extra>"
            )
        )
        fig.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            yaxis=dict(autorange="reversed"),
        )
        _plotly_defaults(fig, height=max(300, len(vendor_df) * 28))
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("View Data Table"):
            st.dataframe(
                vendor_df,
                column_config={
                    "VENDOR_NAME": sender_label,
                    "DOCUMENT_COUNT": "Docs",
                    "TOTAL_AMOUNT": st.column_config.NumberColumn(
                        "Total", format="$%.2f"
                    ),
                    "AVG_AMOUNT": st.column_config.NumberColumn("Avg", format="$%.2f"),
                    "FIRST_DOC": st.column_config.DateColumn("First"),
                    "LAST_DOC": st.column_config.DateColumn("Last"),
                },
                hide_index=True,
                use_container_width=True,
            )
            _download_csv(vendor_df, "spend_by_sender.csv")
    else:
        st.info("No sender data available.")


# ── Monthly Trend ─────────────────────────────────────────────────────────────
with spend_tab2:
    monthly_df = _safe_query(
        f"""
        SELECT
            DATE_TRUNC('month', ef.field_4)   AS month,
            COUNT(*)                           AS document_count,
            SUM(ef.field_10)                   AS total_amount,
            AVG(ef.field_10)                   AS avg_amount,
            COUNT(DISTINCT ef.field_1)         AS unique_senders
        FROM {DB}.EXTRACTED_FIELDS ef
            JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
        WHERE ef.field_4 IS NOT NULL {where_and}
        GROUP BY DATE_TRUNC('month', ef.field_4)
        ORDER BY month
    """,
        type_params,
    )

    if len(monthly_df) > 0:
        # Dual-axis: spend bars + doc count line
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=monthly_df["MONTH"],
                y=monthly_df["TOTAL_AMOUNT"],
                name="Total Spend",
                marker_color="#1e3a5f",
                text=monthly_df["TOTAL_AMOUNT"].apply(lambda v: f"${v:,.0f}"),
                textposition="outside",
                hovertemplate="<b>%{x|%b %Y}</b><br>Spend: $%{y:,.2f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=monthly_df["MONTH"],
                y=monthly_df["DOCUMENT_COUNT"],
                name="Doc Count",
                mode="lines+markers",
                marker=dict(color="#29B5E8", size=8),
                line=dict(color="#29B5E8", width=2),
                yaxis="y2",
                hovertemplate="<b>%{x|%b %Y}</b><br>Documents: %{y}<extra></extra>",
            )
        )
        fig.update_layout(
            yaxis=dict(title="Spend ($)", showgrid=True, gridcolor="rgba(0,0,0,0.06)"),
            yaxis2=dict(
                title="Documents", overlaying="y", side="right", showgrid=False
            ),
            legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
            barmode="group",
        )
        _plotly_defaults(fig, height=400)
        st.plotly_chart(fig, use_container_width=True)

        # Summary metrics
        mc1, mc2, mc3, mc4 = st.columns(4)
        total = monthly_df["TOTAL_AMOUNT"].sum()
        avg_m = monthly_df["TOTAL_AMOUNT"].mean()
        mc1.metric("Total Spend", f"${total:,.0f}")
        mc2.metric("Monthly Avg", f"${avg_m:,.0f}")
        mc3.metric("Peak Month", f"${monthly_df['TOTAL_AMOUNT'].max():,.0f}")
        # MoM change
        if len(monthly_df) >= 2:
            last = monthly_df.iloc[-1]["TOTAL_AMOUNT"]
            prev = monthly_df.iloc[-2]["TOTAL_AMOUNT"]
            pct = ((last - prev) / prev * 100) if prev else 0
            mc4.metric("Latest MoM", f"{pct:+.1f}%")
        else:
            mc4.metric("Months", f"{len(monthly_df)}")

        with st.expander("View Data Table"):
            display_monthly = monthly_df.copy()
            display_monthly["MONTH"] = pd.to_datetime(
                display_monthly["MONTH"]
            ).dt.strftime("%Y-%m")
            st.dataframe(
                display_monthly,
                column_config={
                    "MONTH": "Month",
                    "DOCUMENT_COUNT": "Docs",
                    "TOTAL_AMOUNT": st.column_config.NumberColumn(
                        "Total", format="$%.2f"
                    ),
                    "AVG_AMOUNT": st.column_config.NumberColumn("Avg", format="$%.2f"),
                    "UNIQUE_SENDERS": f"{sender_label}s",
                },
                hide_index=True,
                use_container_width=True,
            )
            _download_csv(display_monthly, "monthly_trend.csv")
    else:
        st.info("No monthly data available.")


# ── Aging ─────────────────────────────────────────────────────────────────────
with spend_tab3:
    aging_df = _safe_query(
        f"""
        SELECT
            aging_bucket,
            COUNT(*)              AS document_count,
            SUM(total_amount)     AS total_amount,
            AVG(total_amount)     AS avg_amount,
            sort_order
        FROM (
            SELECT
                total_amount, doc_type,
                CASE
                    WHEN due_date IS NULL              THEN 'N/A'
                    WHEN due_date >= CURRENT_DATE()    THEN 'Current'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 30  THEN '1-30 Days'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 60  THEN '31-60 Days'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 90  THEN '61-90 Days'
                    ELSE '90+ Days'
                END AS aging_bucket,
                CASE
                    WHEN due_date IS NULL              THEN 99
                    WHEN due_date >= CURRENT_DATE()    THEN 0
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 30  THEN 1
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 60  THEN 2
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 90  THEN 3
                    ELSE 4
                END AS sort_order
            FROM {DB}.V_DOCUMENT_LEDGER
        ) sub
        WHERE aging_bucket != 'N/A'
            {"AND doc_type = ?" if type_params_rd else ""}
        GROUP BY aging_bucket, sort_order
        ORDER BY sort_order
    """,
        type_params_rd,
    )

    if len(aging_df) > 0:
        color_map = {
            "Current": "#22c55e",
            "1-30 Days": "#f59e0b",
            "31-60 Days": "#ef4444",
            "61-90 Days": "#b91c1c",
            "90+ Days": "#450a0a",
        }

        ac1, ac2 = st.columns([2, 1])
        with ac1:
            fig = px.bar(
                aging_df,
                x="AGING_BUCKET",
                y="TOTAL_AMOUNT",
                color="AGING_BUCKET",
                color_discrete_map=color_map,
                labels={"TOTAL_AMOUNT": "Amount ($)", "AGING_BUCKET": ""},
                text=aging_df["TOTAL_AMOUNT"].apply(lambda v: f"${v:,.0f}"),
                custom_data=["DOCUMENT_COUNT"],
            )
            fig.update_traces(
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>$%{y:,.2f}<br>%{customdata[0]} docs<extra></extra>",
            )
            fig.update_layout(showlegend=False)
            _plotly_defaults(fig, height=350)
            st.plotly_chart(fig, use_container_width=True)

        with ac2:
            # Donut chart for proportion
            fig_donut = px.pie(
                aging_df,
                values="TOTAL_AMOUNT",
                names="AGING_BUCKET",
                color="AGING_BUCKET",
                color_discrete_map=color_map,
                hole=0.5,
            )
            fig_donut.update_traces(
                textinfo="percent",
                hovertemplate="<b>%{label}</b><br>$%{value:,.2f}<br>%{percent}<extra></extra>",
            )
            _plotly_defaults(fig_donut, height=350)
            st.plotly_chart(fig_donut, use_container_width=True)

        # Overdue total callout
        overdue = aging_df[aging_df["SORT_ORDER"].between(1, 4)]
        if len(overdue) > 0:
            overdue_total = overdue["TOTAL_AMOUNT"].sum()
            overdue_count = overdue["DOCUMENT_COUNT"].sum()
            st.warning(
                f"**${overdue_total:,.0f}** past due across **{int(overdue_count)}** documents"
            )

        with st.expander("View Data Table"):
            st.dataframe(
                aging_df.drop(columns=["SORT_ORDER"], errors="ignore"),
                hide_index=True,
                use_container_width=True,
            )
            _download_csv(aging_df, "aging_analysis.csv")
    else:
        st.info("No aging data available (due dates may not be extracted).")


st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: PIPELINE THROUGHPUT
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Pipeline Throughput")

pipe_col1, pipe_col2 = st.columns([2, 1])

with pipe_col1:
    pipeline_df = _safe_query(
        f"""
        SELECT
            DATE_TRUNC('day', rd.staged_at)   AS process_date,
            COUNT(*)                            AS files_processed,
            SUM(CASE WHEN ef.record_id IS NOT NULL THEN 1 ELSE 0 END)  AS successfully_extracted,
            SUM(CASE WHEN ef.record_id IS NULL THEN 1 ELSE 0 END)      AS failed,
            AVG(DATEDIFF('second', rd.staged_at,
                COALESCE(ef.extracted_at, rd.staged_at)))              AS avg_processing_seconds
        FROM {DB}.RAW_DOCUMENTS rd
            LEFT JOIN {DB}.EXTRACTED_FIELDS ef ON rd.file_name = ef.file_name
        {"WHERE rd.doc_type = ?" if type_params_rd else ""}
        GROUP BY DATE_TRUNC('day', rd.staged_at)
        ORDER BY process_date DESC
        LIMIT 60
    """,
        type_params_rd,
    )

    if len(pipeline_df) > 0:
        pipeline_df = pipeline_df.sort_values("PROCESS_DATE")
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=pipeline_df["PROCESS_DATE"],
                y=pipeline_df["SUCCESSFULLY_EXTRACTED"],
                name="Extracted",
                marker_color="#22c55e",
            )
        )
        fig.add_trace(
            go.Bar(
                x=pipeline_df["PROCESS_DATE"],
                y=pipeline_df["FAILED"],
                name="Failed",
                marker_color="#ef4444",
            )
        )
        fig.update_layout(
            barmode="stack",
            legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
            yaxis_title="Documents",
        )
        _plotly_defaults(fig, height=300)
        st.plotly_chart(fig, use_container_width=True)

with pipe_col2:
    # Pipeline KPIs
    status_df = _safe_query(f"SELECT * FROM {DB}.V_EXTRACTION_STATUS")
    if len(status_df) > 0:
        s = status_df.iloc[0]
        st.metric("Total Staged", f"{int(s['TOTAL_FILES']):,}")
        st.metric("Extracted", f"{int(s['EXTRACTED_FILES']):,}")
        st.metric("Pending", f"{int(s['PENDING_FILES']):,}")
        st.metric("Failed", f"{int(s['FAILED_FILES']):,}")
        if s.get("LAST_EXTRACTION"):
            st.caption(f"Last run: {s['LAST_EXTRACTION']}")

    # Avg processing time
    if len(pipeline_df) > 0:
        avg_sec = pipeline_df["AVG_PROCESSING_SECONDS"].mean()
        if pd.notna(avg_sec):
            if avg_sec < 60:
                st.metric("Avg Processing", f"{avg_sec:.0f}s")
            else:
                st.metric("Avg Processing", f"{avg_sec/60:.1f}m")

if len(pipeline_df) > 0:
    with st.expander("View Pipeline Data"):
        st.dataframe(pipeline_df, hide_index=True, use_container_width=True)
        _download_csv(pipeline_df, "pipeline_throughput.csv")

st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3: EXTRACTION QUALITY
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Extraction Quality")

quality_df = _safe_query(
    f"""
    SELECT
        ef.file_name,
        rd.doc_type,
        ef.raw_extraction,
        ef.field_1 AS sender,
        ef.field_10 AS total_amount
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    WHERE ef.raw_extraction IS NOT NULL {where_and}
    ORDER BY ef.extracted_at DESC
    LIMIT 200
""",
    type_params,
)

if len(quality_df) > 0:
    import json as _json
    from config import _parse_variant

    # Compute per-document quality stats
    quality_rows = []
    field_stats = {}  # field_name → {present: int, missing: int}

    for _, row in quality_df.iterrows():
        raw = _parse_variant(row.get("RAW_EXTRACTION")) or {}
        skip = {"_confidence", "_validation_warnings"}
        fields = {k: v for k, v in raw.items() if k not in skip}
        total_f = len(fields)
        filled = sum(
            1
            for v in fields.values()
            if v is not None and str(v).strip().lower() not in ("", "null", "none")
        )
        pct = round(filled / total_f * 100, 1) if total_f > 0 else 0

        quality_rows.append(
            {
                "file_name": row["FILE_NAME"],
                "doc_type": row["DOC_TYPE"],
                "sender": row["SENDER"],
                "total_fields": total_f,
                "filled_fields": filled,
                "fill_rate": pct,
            }
        )

        for fname, val in fields.items():
            if fname not in field_stats:
                field_stats[fname] = {"present": 0, "missing": 0, "total": 0}
            field_stats[fname]["total"] += 1
            if val is not None and str(val).strip().lower() not in ("", "null", "none"):
                field_stats[fname]["present"] += 1
            else:
                field_stats[fname]["missing"] += 1

    qdf = pd.DataFrame(quality_rows)

    qc1, qc2, qc3, qc4 = st.columns(4)
    avg_fill = qdf["fill_rate"].mean()
    perfect = len(qdf[qdf["fill_rate"] == 100])
    below_80 = len(qdf[qdf["fill_rate"] < 80])
    qc1.metric("Avg Field Fill Rate", f"{avg_fill:.1f}%")
    qc2.metric("Perfect Extractions", f"{perfect}/{len(qdf)}")
    qc3.metric("Below 80% Fill", f"{below_80}")
    qc4.metric("Documents Analyzed", f"{len(qdf)}")

    eq1, eq2 = st.columns(2)

    with eq1:
        # Field fill rate distribution
        fig = px.histogram(
            qdf,
            x="fill_rate",
            nbins=20,
            labels={"fill_rate": "Fill Rate (%)", "count": "Documents"},
            color_discrete_sequence=["#1e3a5f"],
        )
        fig.update_layout(title="Fill Rate Distribution")
        _plotly_defaults(fig, height=300)
        st.plotly_chart(fig, use_container_width=True)

    with eq2:
        # Per-field presence rate
        if field_stats:
            fs_df = pd.DataFrame(
                [
                    {
                        "field": fname,
                        "fill_rate": (
                            round(s["present"] / s["total"] * 100, 1)
                            if s["total"] > 0
                            else 0
                        ),
                        "present": s["present"],
                        "missing": s["missing"],
                    }
                    for fname, s in field_stats.items()
                ]
            ).sort_values("fill_rate", ascending=True)

            fig = px.bar(
                fs_df,
                x="fill_rate",
                y="field",
                orientation="h",
                labels={"fill_rate": "Fill Rate (%)", "field": ""},
                color="fill_rate",
                color_continuous_scale=["#ef4444", "#f59e0b", "#22c55e"],
                range_color=[0, 100],
            )
            fig.update_layout(title="Per-Field Fill Rate", coloraxis_showscale=False)
            _plotly_defaults(fig, height=max(250, len(fs_df) * 22))
            st.plotly_chart(fig, use_container_width=True)

    # Worst extractions
    with st.expander("Lowest Quality Documents"):
        worst = qdf.nsmallest(10, "fill_rate")
        st.dataframe(
            worst,
            column_config={
                "file_name": "File",
                "doc_type": "Type",
                "sender": sender_label,
                "total_fields": "Total Fields",
                "filled_fields": "Filled",
                "fill_rate": st.column_config.ProgressColumn(
                    "Fill Rate", min_value=0, max_value=100, format="%.1f%%"
                ),
            },
            hide_index=True,
            use_container_width=True,
        )
        _download_csv(qdf, "extraction_quality.csv")

else:
    st.info("No raw extraction data available for quality analysis.")


st.divider()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4: LINE ITEM ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Line Item Analysis")

li_tab1, li_tab2, li_tab3 = st.tabs(
    ["Top Items", "Category Spend", "Spend Distribution"]
)

# ── Top Items ─────────────────────────────────────────────────────────────────
with li_tab1:
    top_items = _safe_query(
        f"""
        SELECT
            etd.col_1               AS item_description,
            etd.col_2               AS category,
            COUNT(*)                AS appearance_count,
            SUM(etd.col_3)          AS total_quantity,
            AVG(etd.col_4)          AS avg_unit_price,
            SUM(etd.col_5)          AS total_spend,
            COUNT(DISTINCT etd.file_name) AS doc_count
        FROM {DB}.EXTRACTED_TABLE_DATA etd
            JOIN {DB}.RAW_DOCUMENTS rd ON etd.file_name = rd.file_name
        WHERE etd.col_1 IS NOT NULL {type_and_rd}
        GROUP BY etd.col_1, etd.col_2
        ORDER BY total_spend DESC
        LIMIT 25
    """,
        type_params_rd,
    )

    if len(top_items) > 0:
        fig = px.bar(
            top_items.head(15),
            x="TOTAL_SPEND",
            y="ITEM_DESCRIPTION",
            orientation="h",
            color="CATEGORY",
            labels={"TOTAL_SPEND": "Total Spend ($)", "ITEM_DESCRIPTION": "Item"},
            text_auto="$.2s",
        )
        fig.update_layout(yaxis=dict(autorange="reversed"))
        _plotly_defaults(fig, height=max(300, min(len(top_items), 15) * 30))
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("View Full Table"):
            st.dataframe(
                top_items,
                column_config={
                    "ITEM_DESCRIPTION": "Item",
                    "CATEGORY": "Category",
                    "APPEARANCE_COUNT": "Count",
                    "TOTAL_QUANTITY": st.column_config.NumberColumn(
                        "Total Qty", format="%.0f"
                    ),
                    "AVG_UNIT_PRICE": st.column_config.NumberColumn(
                        "Avg Price", format="$%.2f"
                    ),
                    "TOTAL_SPEND": st.column_config.NumberColumn(
                        "Total Spend", format="$%.2f"
                    ),
                    "DOC_COUNT": "Documents",
                },
                hide_index=True,
                use_container_width=True,
            )
            _download_csv(top_items, "top_line_items.csv")
    else:
        st.info("No line item data available.")


# ── Category Spend ────────────────────────────────────────────────────────────
with li_tab2:
    cat_df = _safe_query(
        f"""
        SELECT
            COALESCE(etd.col_2, 'Uncategorized')  AS category,
            COUNT(*)                                AS line_count,
            SUM(etd.col_5)                          AS total_spend,
            AVG(etd.col_5)                          AS avg_line_amount,
            COUNT(DISTINCT etd.file_name)            AS doc_count
        FROM {DB}.EXTRACTED_TABLE_DATA etd
            JOIN {DB}.RAW_DOCUMENTS rd ON etd.file_name = rd.file_name
        WHERE etd.col_5 IS NOT NULL {type_and_rd}
        GROUP BY COALESCE(etd.col_2, 'Uncategorized')
        ORDER BY total_spend DESC
    """,
        type_params_rd,
    )

    if len(cat_df) > 0:
        cc1, cc2 = st.columns([1, 1])
        with cc1:
            fig = px.pie(
                cat_df,
                values="TOTAL_SPEND",
                names="CATEGORY",
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.4,
            )
            fig.update_traces(
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>$%{value:,.2f}<br>%{percent}<extra></extra>",
            )
            fig.update_layout(title="Spend by Category")
            _plotly_defaults(fig, height=350)
            st.plotly_chart(fig, use_container_width=True)

        with cc2:
            fig = px.bar(
                cat_df,
                x="CATEGORY",
                y="TOTAL_SPEND",
                color="TOTAL_SPEND",
                color_continuous_scale="Teal",
                labels={"TOTAL_SPEND": "Total ($)", "CATEGORY": ""},
                text_auto="$.2s",
            )
            fig.update_layout(coloraxis_showscale=False, title="Category Breakdown")
            _plotly_defaults(fig, height=350)
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("View Data"):
            st.dataframe(cat_df, hide_index=True, use_container_width=True)
            _download_csv(cat_df, "category_spend.csv")
    else:
        st.info("No categorized line items found.")


# ── Spend Distribution ────────────────────────────────────────────────────────
with li_tab3:
    dist_df = _safe_query(
        f"""
        SELECT
            etd.col_5 AS line_amount,
            etd.col_1 AS description,
            etd.col_2 AS category
        FROM {DB}.EXTRACTED_TABLE_DATA etd
            JOIN {DB}.RAW_DOCUMENTS rd ON etd.file_name = rd.file_name
        WHERE etd.col_5 IS NOT NULL AND etd.col_5 > 0 {type_and_rd}
        ORDER BY etd.col_5 DESC
        LIMIT 1000
    """,
        type_params_rd,
    )

    if len(dist_df) > 0:
        fig = px.histogram(
            dist_df,
            x="LINE_AMOUNT",
            nbins=50,
            labels={"LINE_AMOUNT": "Line Item Amount ($)", "count": "Count"},
            color_discrete_sequence=["#1e3a5f"],
            marginal="box",
        )
        fig.update_layout(title="Line Item Amount Distribution")
        _plotly_defaults(fig, height=350)
        st.plotly_chart(fig, use_container_width=True)

        dc1, dc2, dc3, dc4 = st.columns(4)
        dc1.metric("Median", f"${dist_df['LINE_AMOUNT'].median():,.2f}")
        dc2.metric("Mean", f"${dist_df['LINE_AMOUNT'].mean():,.2f}")
        dc3.metric("Max", f"${dist_df['LINE_AMOUNT'].max():,.2f}")
        dc4.metric("Std Dev", f"${dist_df['LINE_AMOUNT'].std():,.2f}")
    else:
        st.info("No line item amounts available.")


render_nav_bar()
