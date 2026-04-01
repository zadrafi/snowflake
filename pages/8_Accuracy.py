"""
Page 8: AI_EXTRACT Accuracy & Pipeline Observability

Sections:
  1. Extraction Accuracy (cross-domain, field-level, heatmap, failures)
  2. Pipeline Timing (stage-to-extract latency, task runs, per-doc duration)
  3. Task & Procedure Execution History
"""

import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import DB, STAGE, get_session, inject_custom_css, render_nav_bar

st.set_page_config(page_title="Accuracy & Pipeline", page_icon="🎯", layout="wide")
inject_custom_css()
session = get_session()

st.title("Extraction Accuracy & Pipeline Observability")
st.caption("Extraction quality metrics + end-to-end pipeline timing")


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=300, show_spinner=False)
def _query(_session, sql: str) -> pd.DataFrame:
    try:
        return _session.sql(sql).to_pandas()
    except Exception:
        return pd.DataFrame()


def _plotly_defaults(fig, height=320):
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(0,0,0,0.05)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.05)")
    return fig


def _fmt_duration(seconds):
    """Format seconds into human-readable duration."""
    if seconds is None or pd.isna(seconds):
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds/60:.1f}m"
    return f"{seconds/3600:.1f}h"


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

FIELD_MAP = {
    "Company/Vendor": {
        "Utility Bill": "Utility Company",
        "Invoice": "Vendor Name",
        "Contract": "Party A Name",
        "Receipt": "Merchant Name",
    },
    "Account/Invoice #": {
        "Utility Bill": "Account Number",
        "Invoice": "Invoice Number",
        "Contract": "Party B Name",
        "Receipt": "Receipt Number",
    },
    "Address": {
        "Utility Bill": "Service Address",
        "Invoice": "Vendor Address",
        "Contract": "Party A Address",
        "Receipt": "N/A",
    },
    "Date (Primary)": {
        "Utility Bill": "Billing Period End",
        "Invoice": "Invoice Date",
        "Contract": "Effective Date",
        "Receipt": "Transaction Date",
    },
    "Date (Secondary)": {
        "Utility Bill": "Due Date",
        "Invoice": "Due Date",
        "Contract": "Expiration Date",
        "Receipt": "Transaction Time",
    },
    "Total Amount": {
        "Utility Bill": "Total Due",
        "Invoice": "Total Due",
        "Contract": "Contract Value",
        "Receipt": "Total",
    },
    "Subtotal/Current": {
        "Utility Bill": "Current Charges",
        "Invoice": "Subtotal",
        "Contract": "Confidentiality Years",
        "Receipt": "Subtotal",
    },
    "Tax/Surcharge": {
        "Utility Bill": "Demand kW",
        "Invoice": "Tax Amount",
        "Contract": "Termination Notice Days",
        "Receipt": "Tax Amount",
    },
    "Secondary Amount": {
        "Utility Bill": "Previous Balance",
        "Invoice": "Shipping",
        "Contract": "Auto-Renewal",
        "Receipt": "Tip Amount",
    },
    "Classification": {
        "Utility Bill": "Rate Schedule",
        "Invoice": "Payment Terms",
        "Contract": "Contract Type",
        "Receipt": "Receipt Type",
    },
    "ID/Reference": {
        "Utility Bill": "Meter Number",
        "Invoice": "PO Number",
        "Contract": "Governing Law",
        "Receipt": "Payment Method",
    },
    "Quantity/Count": {
        "Utility Bill": "kWh Usage",
        "Invoice": "Line Item Count",
        "Contract": "Dispute Resolution",
        "Receipt": "Line Item Count",
    },
}


@st.cache_data(ttl=300, show_spinner="Loading accuracy data…")
def load_accuracy_data(_session, db):
    try:
        df = _session.sql(
            """
            SELECT FILE_NAME, DOC_TYPE, DOC_SUBTYPE, FIELD_RESULTS
            FROM UTILITY_BILL_POC.EXTRACTION.V_UNIFIED_ACCURACY
        """
        ).to_pandas()
    except Exception:
        df = _session.sql(
            f"""
            SELECT e.file_name AS FILE_NAME, r.doc_type AS DOC_TYPE, r.doc_type AS DOC_SUBTYPE,
                   e.raw_extraction:_confidence::VARCHAR AS FIELD_RESULTS
            FROM {db}.EXTRACTED_FIELDS e
            JOIN {db}.RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE e.raw_extraction:_confidence IS NOT NULL
        """
        ).to_pandas()

    records = []
    for _, row in df.iterrows():
        fr = row["FIELD_RESULTS"]
        if fr is None:
            continue
        if isinstance(fr, str):
            fr = json.loads(fr)
        if not isinstance(fr, dict):
            continue
        for field, val in fr.items():
            if val is not None:
                records.append(
                    {
                        "file": row["FILE_NAME"],
                        "doc_type": row["DOC_TYPE"],
                        "sub_type": row.get("DOC_SUBTYPE", row["DOC_TYPE"]),
                        "field": field,
                        "passed": (
                            int(float(val) >= 0.9)
                            if isinstance(val, (int, float))
                            else int(val)
                        ),
                        "confidence": (
                            float(val) if isinstance(val, (int, float)) else None
                        ),
                    }
                )
    return (
        pd.DataFrame(records)
        if records
        else pd.DataFrame(
            columns=["file", "doc_type", "sub_type", "field", "passed", "confidence"]
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB LAYOUT
# ══════════════════════════════════════════════════════════════════════════════

tab_accuracy, tab_pipeline, tab_tasks = st.tabs(
    [
        "🎯 Extraction Accuracy",
        "⏱️ Pipeline Timing",
        "📋 Task History",
    ]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: EXTRACTION ACCURACY
# ══════════════════════════════════════════════════════════════════════════════

with tab_accuracy:
    data = load_accuracy_data(session, DB)

    # KPI bar
    total_fields = len(data)
    total_passed = int(data["passed"].sum()) if total_fields > 0 else 0
    overall_pct = round(total_passed / total_fields * 100, 1) if total_fields > 0 else 0
    total_docs = data["file"].nunique() if total_fields > 0 else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Documents", total_docs)
    k2.metric("Fields Tested", f"{total_fields:,}")
    k3.metric("Fields Passed", f"{total_passed:,}")
    k4.metric("Overall Accuracy", f"{overall_pct}%")

    if total_fields == 0:
        st.info(
            "No accuracy data available. Extract documents with confidence scoring to populate this."
        )
    else:
        st.divider()

        # ── Accuracy by Doc Type ──────────────────────────────────────────
        st.markdown("##### Accuracy by Document Type")

        type_stats = (
            data.groupby("doc_type")
            .agg(
                docs=("file", "nunique"),
                total=("passed", "count"),
                passed=("passed", "sum"),
            )
            .reset_index()
        )
        type_stats["accuracy"] = (
            type_stats["passed"] / type_stats["total"] * 100
        ).round(1)
        type_stats["failed"] = type_stats["total"] - type_stats["passed"]
        type_stats = type_stats.sort_values("accuracy", ascending=False)

        tc1, tc2 = st.columns([2, 1])
        with tc1:
            fig = px.bar(
                type_stats,
                x="doc_type",
                y="accuracy",
                color="accuracy",
                color_continuous_scale=["#ef4444", "#f59e0b", "#22c55e"],
                range_color=[0, 100],
                text="accuracy",
                labels={"doc_type": "", "accuracy": "Accuracy %"},
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(coloraxis_showscale=False)
            _plotly_defaults(fig, 300)
            st.plotly_chart(
                fig, use_container_width=True, config={"displayModeBar": False}
            )

        with tc2:
            display_df = type_stats[
                ["doc_type", "docs", "total", "passed", "failed", "accuracy"]
            ].copy()
            display_df.columns = ["Type", "Docs", "Fields", "Passed", "Failed", "Acc %"]
            st.dataframe(display_df, hide_index=True, use_container_width=True)

        st.divider()

        # ── Field Accuracy Heatmap ────────────────────────────────────────
        st.markdown("##### Field Accuracy Heatmap")

        field_stats = (
            data.groupby("field")
            .agg(
                total=("passed", "count"),
                passed=("passed", "sum"),
            )
            .reset_index()
        )
        field_stats["accuracy"] = (
            field_stats["passed"] / field_stats["total"] * 100
        ).round(1)
        field_stats = field_stats.sort_values("accuracy")

        pivot = (
            data.groupby(["field", "doc_type"])["passed"]
            .mean()
            .unstack(fill_value=None)
        )
        pivot = (pivot * 100).round(1)
        pivot = pivot.reindex(field_stats["field"].tolist())

        def color_accuracy(val):
            if pd.isna(val):
                return "background-color: #f8fafc; color: #94a3b8"
            if val == 100:
                return "background-color: #dcfce7; color: #166534"
            if val >= 90:
                return "background-color: #fef9c3; color: #854d0e"
            return "background-color: #fee2e2; color: #991b1b"

        styled = pivot.style.map(color_accuracy).format("{:.1f}%", na_rep="—")
        st.dataframe(
            styled, use_container_width=True, height=min(400, len(pivot) * 35 + 40)
        )

        # ── Per-field bar chart ───────────────────────────────────────────
        with st.expander("Field Accuracy Detail"):
            fig_f = px.bar(
                field_stats,
                x="accuracy",
                y="field",
                orientation="h",
                color="accuracy",
                color_continuous_scale=["#ef4444", "#f59e0b", "#22c55e"],
                range_color=[0, 100],
                labels={"accuracy": "Accuracy %", "field": ""},
                text="accuracy",
            )
            fig_f.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_f.update_layout(coloraxis_showscale=False)
            _plotly_defaults(fig_f, max(250, len(field_stats) * 24))
            st.plotly_chart(
                fig_f, use_container_width=True, config={"displayModeBar": False}
            )

        st.divider()

        # ── Failure Drill-Down ────────────────────────────────────────────
        st.markdown("##### Failure Drill-Down")

        fc1, fc2 = st.columns(2)
        with fc1:
            sel_type = st.selectbox(
                "Document Type", ["All"] + sorted(data["doc_type"].unique().tolist())
            )
        with fc2:
            sel_field = st.selectbox(
                "Field", ["All"] + sorted(data["field"].unique().tolist())
            )

        failures = data[data["passed"] == 0].copy()
        if sel_type != "All":
            failures = failures[failures["doc_type"] == sel_type].copy()
        if sel_field != "All":
            failures = failures[failures["field"] == sel_field].copy()

        failures["actual_field"] = failures.apply(
            lambda r: FIELD_MAP.get(r["field"], {}).get(r["doc_type"], r["field"]),
            axis=1,
        )

        if len(failures) == 0:
            st.success("No failures for the selected filters.")
        else:
            st.error(f"{len(failures)} field extraction failure(s)")

            fail_display = failures[
                ["file", "doc_type", "sub_type", "field", "actual_field"]
            ].copy()
            fail_display.columns = [
                "File",
                "Type",
                "Sub-Type",
                "Normalized Field",
                "Actual Field",
            ]
            st.dataframe(fail_display, hide_index=True, use_container_width=True)

            with st.expander("Failure Summary by Category"):
                fail_summary = (
                    failures.groupby(["doc_type", "field"])
                    .size()
                    .reset_index(name="count")
                )
                fail_summary = fail_summary.sort_values("count", ascending=False)
                fail_summary.columns = ["Type", "Field", "Failures"]
                st.dataframe(fail_summary, hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: PIPELINE TIMING
# ══════════════════════════════════════════════════════════════════════════════

with tab_pipeline:
    st.markdown("##### End-to-End Processing Latency")
    st.caption(
        "Time from file staging (`created_at`) to extraction completion (`extracted_at`)"
    )

    # Per-document timing
    timing_df = _query(
        session,
        f"""
        SELECT
            rd.file_name,
            rd.doc_type,
            rd.staged_at                                          AS staged_at,
            ef.extracted_at,
            DATEDIFF('second', rd.staged_at, ef.extracted_at)     AS latency_seconds,
            rd.page_count,
            rd.file_size_bytes
        FROM {DB}.RAW_DOCUMENTS rd
            JOIN {DB}.EXTRACTED_FIELDS ef ON rd.file_name = ef.file_name
        WHERE ef.extracted_at IS NOT NULL
          AND rd.staged_at IS NOT NULL
        ORDER BY ef.extracted_at DESC
        LIMIT 500
    """,
    )

    if len(timing_df) > 0 and "LATENCY_SECONDS" in timing_df.columns:
        valid_timing = timing_df[
            timing_df["LATENCY_SECONDS"].notna() & (timing_df["LATENCY_SECONDS"] >= 0)
        ].copy()

        if len(valid_timing) > 0:
            # KPI cards
            tc1, tc2, tc3, tc4, tc5 = st.columns(5)
            med = valid_timing["LATENCY_SECONDS"].median()
            avg = valid_timing["LATENCY_SECONDS"].mean()
            p95 = valid_timing["LATENCY_SECONDS"].quantile(0.95)
            mn = valid_timing["LATENCY_SECONDS"].min()
            mx = valid_timing["LATENCY_SECONDS"].max()

            tc1.metric("Median Latency", _fmt_duration(med))
            tc2.metric("Avg Latency", _fmt_duration(avg))
            tc3.metric("P95 Latency", _fmt_duration(p95))
            tc4.metric("Fastest", _fmt_duration(mn))
            tc5.metric("Slowest", _fmt_duration(mx))

            st.divider()

            lc1, lc2 = st.columns(2)

            with lc1:
                # Latency distribution
                fig_hist = px.histogram(
                    valid_timing,
                    x="LATENCY_SECONDS",
                    nbins=30,
                    labels={"LATENCY_SECONDS": "Seconds", "count": "Documents"},
                    color_discrete_sequence=["#1e3a5f"],
                    marginal="box",
                )
                fig_hist.update_layout(title="Latency Distribution (stage → extract)")
                _plotly_defaults(fig_hist, 320)
                st.plotly_chart(
                    fig_hist, use_container_width=True, config={"displayModeBar": False}
                )

            with lc2:
                # Latency by doc type
                type_timing = (
                    valid_timing.groupby("DOC_TYPE")
                    .agg(
                        docs=("FILE_NAME", "count"),
                        median_sec=("LATENCY_SECONDS", "median"),
                        avg_sec=("LATENCY_SECONDS", "mean"),
                        p95_sec=("LATENCY_SECONDS", lambda x: x.quantile(0.95)),
                    )
                    .reset_index()
                    .sort_values("median_sec")
                )

                fig_box = px.box(
                    valid_timing,
                    x="DOC_TYPE",
                    y="LATENCY_SECONDS",
                    color="DOC_TYPE",
                    labels={"DOC_TYPE": "", "LATENCY_SECONDS": "Seconds"},
                )
                fig_box.update_layout(
                    title="Latency by Document Type", showlegend=False
                )
                _plotly_defaults(fig_box, 320)
                st.plotly_chart(
                    fig_box, use_container_width=True, config={"displayModeBar": False}
                )

            st.divider()

            # Latency over time (daily median + P95)
            st.markdown("##### Latency Trend")

            valid_timing["extract_date"] = pd.to_datetime(
                valid_timing["EXTRACTED_AT"]
            ).dt.date
            daily_latency = (
                valid_timing.groupby("extract_date")
                .agg(
                    docs=("FILE_NAME", "count"),
                    median_sec=("LATENCY_SECONDS", "median"),
                    avg_sec=("LATENCY_SECONDS", "mean"),
                    p95_sec=("LATENCY_SECONDS", lambda x: x.quantile(0.95)),
                )
                .reset_index()
            )

            fig_trend = go.Figure()
            fig_trend.add_trace(
                go.Scatter(
                    x=daily_latency["extract_date"],
                    y=daily_latency["median_sec"],
                    name="Median",
                    mode="lines+markers",
                    line=dict(color="#1e3a5f", width=2),
                    marker=dict(size=6),
                )
            )
            fig_trend.add_trace(
                go.Scatter(
                    x=daily_latency["extract_date"],
                    y=daily_latency["p95_sec"],
                    name="P95",
                    mode="lines+markers",
                    line=dict(color="#ef4444", width=2, dash="dash"),
                    marker=dict(size=6),
                )
            )
            fig_trend.add_trace(
                go.Bar(
                    x=daily_latency["extract_date"],
                    y=daily_latency["docs"],
                    name="Docs",
                    yaxis="y2",
                    marker_color="rgba(41,181,232,0.3)",
                )
            )
            fig_trend.update_layout(
                yaxis=dict(title="Seconds"),
                yaxis2=dict(
                    title="Documents", overlaying="y", side="right", showgrid=False
                ),
                legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
            )
            _plotly_defaults(fig_trend, 300)
            st.plotly_chart(
                fig_trend, use_container_width=True, config={"displayModeBar": False}
            )

            # Page count vs latency scatter
            if "PAGE_COUNT" in valid_timing.columns:
                has_pages = valid_timing[
                    valid_timing["PAGE_COUNT"].notna()
                    & (valid_timing["PAGE_COUNT"] > 0)
                ]
                if len(has_pages) > 5:
                    st.divider()
                    st.markdown("##### Page Count vs Processing Time")
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        fig_pages = px.scatter(
                            has_pages,
                            x="PAGE_COUNT",
                            y="LATENCY_SECONDS",
                            color="DOC_TYPE",
                            hover_data=["FILE_NAME"],
                            labels={
                                "PAGE_COUNT": "Pages",
                                "LATENCY_SECONDS": "Seconds",
                            },
                            trendline="ols",
                        )
                        fig_pages.update_layout(title="Pages vs Latency")
                        _plotly_defaults(fig_pages, 300)
                        st.plotly_chart(
                            fig_pages,
                            use_container_width=True,
                            config={"displayModeBar": False},
                        )

                    with sc2:
                        page_stats = (
                            has_pages.groupby("PAGE_COUNT")
                            .agg(
                                docs=("FILE_NAME", "count"),
                                avg_sec=("LATENCY_SECONDS", "mean"),
                                median_sec=("LATENCY_SECONDS", "median"),
                            )
                            .reset_index()
                            .sort_values("PAGE_COUNT")
                        )
                        page_stats.columns = [
                            "Pages",
                            "Docs",
                            "Avg (sec)",
                            "Median (sec)",
                        ]
                        st.dataframe(
                            page_stats, hide_index=True, use_container_width=True
                        )

            # Recent documents detail
            with st.expander("Recent Document Processing Detail"):
                detail = (
                    valid_timing[
                        [
                            "FILE_NAME",
                            "DOC_TYPE",
                            "STAGED_AT",
                            "EXTRACTED_AT",
                            "LATENCY_SECONDS",
                            "PAGE_COUNT",
                            "FILE_SIZE_BYTES",
                        ]
                    ]
                    .head(50)
                    .copy()
                )
                detail["LATENCY"] = detail["LATENCY_SECONDS"].apply(_fmt_duration)
                st.dataframe(
                    detail,
                    column_config={
                        "FILE_NAME": "File",
                        "DOC_TYPE": "Type",
                        "STAGED_AT": st.column_config.DatetimeColumn(
                            "Staged", format="MMM D h:mm a"
                        ),
                        "EXTRACTED_AT": st.column_config.DatetimeColumn(
                            "Extracted", format="MMM D h:mm a"
                        ),
                        "LATENCY_SECONDS": None,
                        "LATENCY": "Duration",
                        "PAGE_COUNT": "Pages",
                        "FILE_SIZE_BYTES": st.column_config.NumberColumn(
                            "Size (B)", format="%d"
                        ),
                    },
                    hide_index=True,
                    use_container_width=True,
                )
                csv = detail.to_csv(index=False)
                st.download_button(
                    "Download CSV", csv, "processing_detail.csv", "text/csv"
                )
        else:
            st.info("No valid timing data (latency could not be computed).")
    else:
        st.info(
            "No timing data available. Ensure `RAW_DOCUMENTS.created_at` and "
            "`EXTRACTED_FIELDS.extracted_at` are populated."
        )

    st.divider()

    # ── Pending / Stale documents ─────────────────────────────────────────
    st.markdown("##### Pending & Stale Documents")
    st.caption(
        "Documents staged but not yet extracted, or stuck for an unusually long time"
    )

    pending = _query(
        session,
        f"""
        SELECT
            rd.file_name,
            rd.doc_type,
            rd.staged_at                                        AS staged_at,
            DATEDIFF('minute', rd.staged_at, CURRENT_TIMESTAMP()) AS waiting_minutes,
            rd.page_count,
            rd.file_size_bytes,
            rd.status
        FROM {DB}.RAW_DOCUMENTS rd
            LEFT JOIN {DB}.EXTRACTED_FIELDS ef ON rd.file_name = ef.file_name
        WHERE ef.record_id IS NULL
        ORDER BY rd.staged_at ASC
        LIMIT 50
    """,
    )

    if len(pending) > 0:
        st.warning(f"**{len(pending)}** document(s) pending extraction")

        stale = (
            pending[pending["WAITING_MINUTES"] > 30]
            if "WAITING_MINUTES" in pending.columns
            else pd.DataFrame()
        )
        if len(stale) > 0:
            st.error(
                f"**{len(stale)}** document(s) waiting more than 30 minutes — may indicate a task failure"
            )

        st.dataframe(
            pending,
            column_config={
                "FILE_NAME": "File",
                "DOC_TYPE": "Type",
                "STAGED_AT": st.column_config.DatetimeColumn(
                    "Staged", format="MMM D h:mm a"
                ),
                "WAITING_MINUTES": st.column_config.NumberColumn(
                    "Waiting (min)", format="%d"
                ),
                "PAGE_COUNT": "Pages",
                "FILE_SIZE_BYTES": st.column_config.NumberColumn("Size (B)", format="%d"),
                "STATUS": "Status",
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.success("No pending documents — all staged files have been extracted.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: TASK & PROCEDURE HISTORY
# ══════════════════════════════════════════════════════════════════════════════

with tab_tasks:
    st.markdown("##### Scheduled Task Execution History")
    st.caption("Recent runs of `EXTRACT_NEW_DOCUMENTS_TASK` and related procedures")

    day_range = st.selectbox(
        "Time Range",
        [7, 14, 30, 90],
        index=1,
        format_func=lambda d: f"Last {d} days",
        key="task_days",
    )

    # Task history from INFORMATION_SCHEMA
    task_history = _query(
        session,
        f"""
        SELECT
            name                                           AS task_name,
            state,
            scheduled_time,
            completed_time,
            DATEDIFF('second', scheduled_time, completed_time) AS duration_seconds,
            error_code,
            error_message,
            query_id
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD('day', -{day_range}, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 200
        ))
        ORDER BY scheduled_time DESC
    """,
    )

    if len(task_history) > 0:
        # Filter to extraction-related tasks
        extract_tasks = (
            task_history[
                task_history["TASK_NAME"].str.contains("EXTRACT", case=False, na=False)
            ]
            if "TASK_NAME" in task_history.columns
            else task_history
        )

        display_tasks = extract_tasks if len(extract_tasks) > 0 else task_history

        # KPIs
        th1, th2, th3, th4 = st.columns(4)
        total_runs = len(display_tasks)
        succeeded = (
            len(display_tasks[display_tasks["STATE"] == "SUCCEEDED"])
            if "STATE" in display_tasks.columns
            else 0
        )
        failed_runs = (
            len(display_tasks[display_tasks["STATE"] == "FAILED"])
            if "STATE" in display_tasks.columns
            else 0
        )
        avg_dur = (
            display_tasks["DURATION_SECONDS"].median()
            if "DURATION_SECONDS" in display_tasks.columns
            else None
        )

        th1.metric("Total Runs", f"{total_runs}")
        th2.metric("Succeeded", f"{succeeded}")
        th3.metric("Failed", f"{failed_runs}")
        th4.metric("Median Duration", _fmt_duration(avg_dur))

        st.divider()

        tc1, tc2 = st.columns([2, 1])

        with tc1:
            # Duration over time
            if "DURATION_SECONDS" in display_tasks.columns:
                run_data = display_tasks[
                    display_tasks["DURATION_SECONDS"].notna()
                ].copy()
                if len(run_data) > 0:
                    color_col = "STATE" if "STATE" in run_data.columns else None
                    color_map = {
                        "SUCCEEDED": "#22c55e",
                        "FAILED": "#ef4444",
                        "SKIPPED": "#94a3b8",
                    }
                    fig_runs = px.scatter(
                        run_data,
                        x="SCHEDULED_TIME",
                        y="DURATION_SECONDS",
                        color=color_col,
                        color_discrete_map=color_map,
                        labels={
                            "SCHEDULED_TIME": "",
                            "DURATION_SECONDS": "Duration (sec)",
                        },
                        hover_data=["TASK_NAME", "QUERY_ID"],
                    )
                    fig_runs.update_layout(title="Task Run Duration")
                    _plotly_defaults(fig_runs, 300)
                    st.plotly_chart(
                        fig_runs,
                        use_container_width=True,
                        config={"displayModeBar": False},
                    )

        with tc2:
            # State breakdown
            if "STATE" in display_tasks.columns:
                state_counts = display_tasks["STATE"].value_counts().reset_index()
                state_counts.columns = ["State", "Count"]
                color_map_pie = {
                    "SUCCEEDED": "#22c55e",
                    "FAILED": "#ef4444",
                    "SKIPPED": "#94a3b8",
                    "CANCELLED": "#f59e0b",
                }
                fig_pie = px.pie(
                    state_counts,
                    values="Count",
                    names="State",
                    color="State",
                    color_discrete_map=color_map_pie,
                    hole=0.5,
                )
                fig_pie.update_traces(textinfo="value+percent")
                _plotly_defaults(fig_pie, 300)
                st.plotly_chart(
                    fig_pie, use_container_width=True, config={"displayModeBar": False}
                )

        # Failed runs detail
        if failed_runs > 0:
            st.divider()
            st.markdown("##### Failed Task Runs")
            failed_df = display_tasks[display_tasks["STATE"] == "FAILED"].copy()
            st.dataframe(
                failed_df[
                    [
                        "TASK_NAME",
                        "SCHEDULED_TIME",
                        "ERROR_CODE",
                        "ERROR_MESSAGE",
                        "QUERY_ID",
                    ]
                ],
                column_config={
                    "TASK_NAME": "Task",
                    "SCHEDULED_TIME": st.column_config.DatetimeColumn(
                        "Scheduled", format="MMM D h:mm a"
                    ),
                    "ERROR_CODE": "Error Code",
                    "ERROR_MESSAGE": "Error",
                    "QUERY_ID": "Query ID",
                },
                hide_index=True,
                use_container_width=True,
            )

        # Full history table
        with st.expander("Full Task History"):
            display_hist = display_tasks.copy()
            if "DURATION_SECONDS" in display_hist.columns:
                display_hist["DURATION"] = display_hist["DURATION_SECONDS"].apply(
                    _fmt_duration
                )
            st.dataframe(
                display_hist,
                column_config={
                    "TASK_NAME": "Task",
                    "STATE": "State",
                    "SCHEDULED_TIME": st.column_config.DatetimeColumn(
                        "Scheduled", format="MMM D h:mm:ss a"
                    ),
                    "COMPLETED_TIME": st.column_config.DatetimeColumn(
                        "Completed", format="MMM D h:mm:ss a"
                    ),
                    "DURATION_SECONDS": None,
                    "DURATION": "Duration",
                    "ERROR_CODE": "Error",
                    "ERROR_MESSAGE": "Message",
                    "QUERY_ID": "Query ID",
                },
                hide_index=True,
                use_container_width=True,
            )
            csv = display_hist.to_csv(index=False)
            st.download_button(
                "Download Task History", csv, "task_history.csv", "text/csv"
            )
    else:
        st.info(
            "No task history available. This requires access to "
            "`INFORMATION_SCHEMA.TASK_HISTORY()` — check that tasks are scheduled "
            "and the role has MONITOR privilege."
        )

    st.divider()

    # ── Stored Procedure Execution ────────────────────────────────────────
    st.markdown("##### Stored Procedure Calls")
    st.caption("Recent calls to extraction and registration procedures")

    sp_history = _query(
        session,
        f"""
        SELECT
            query_text,
            start_time,
            end_time,
            DATEDIFF('second', start_time, end_time) AS duration_seconds,
            execution_status,
            rows_produced,
            query_id
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_type = 'CALL'
          AND query_text ILIKE '%SP_EXTRACT%' OR query_text ILIKE '%SP_REGISTER%' OR query_text ILIKE '%SP_REEXTRACT%'
          AND start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
        ORDER BY start_time DESC
        LIMIT 100
    """,
    )

    if len(sp_history) > 0:
        sp1, sp2, sp3 = st.columns(3)
        sp_total = len(sp_history)
        sp_ok = (
            len(sp_history[sp_history["EXECUTION_STATUS"] == "SUCCESS"])
            if "EXECUTION_STATUS" in sp_history.columns
            else 0
        )
        sp_avg = (
            sp_history["DURATION_SECONDS"].median()
            if "DURATION_SECONDS" in sp_history.columns
            else None
        )
        sp1.metric("SP Calls", f"{sp_total}")
        sp2.metric("Succeeded", f"{sp_ok}")
        sp3.metric("Median Duration", _fmt_duration(sp_avg))

        sp_display = sp_history.copy()
        if "DURATION_SECONDS" in sp_display.columns:
            sp_display["DURATION"] = sp_display["DURATION_SECONDS"].apply(_fmt_duration)
        # Truncate query text for display
        if "QUERY_TEXT" in sp_display.columns:
            sp_display["PROCEDURE"] = sp_display["QUERY_TEXT"].str[:80]

        st.dataframe(
            sp_display,
            column_config={
                "QUERY_TEXT": None,
                "PROCEDURE": "Call",
                "START_TIME": st.column_config.DatetimeColumn(
                    "Started", format="MMM D h:mm a"
                ),
                "END_TIME": st.column_config.DatetimeColumn(
                    "Completed", format="MMM D h:mm a"
                ),
                "DURATION_SECONDS": None,
                "DURATION": "Duration",
                "EXECUTION_STATUS": "Status",
                "ROWS_PRODUCED": "Rows",
                "QUERY_ID": "Query ID",
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No stored procedure calls found. Requires ACCOUNT_USAGE access.")


render_nav_bar()
