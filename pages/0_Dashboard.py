"""
Page 0: Dashboard — Clean executive front page with KPI cards,
pipeline health, recent activity, and quick-action links.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from config import (
    DB,
    get_session,
    get_doc_type_labels,
    get_doc_types,
    inject_custom_css,
    sidebar_branding,
    render_nav_bar,
)

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════


def _safe_query(sql, params=None):
    try:
        return session.sql(sql, params=params or []).to_pandas()
    except Exception:
        return pd.DataFrame()


def _sparkline(values, color="#29B5E8", height=45, width=120):
    """Return a tiny Plotly sparkline figure."""
    fig = go.Figure(
        go.Scatter(
            y=values,
            mode="lines",
            line=dict(color=color, width=2),
            fill="tozeroy",
            fillcolor=f"rgba{_hex_rgb(color)+(0.1,)}",
        )
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
        width=width,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def _hex_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i : i + 2], 16) for i in (0, 2, 4))


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════

st.markdown(
    '<div style="margin-bottom:0.5rem;">'
    '<h1 style="margin-bottom:0;line-height:1.1;">Document Processing Dashboard</h1>'
    '<p style="font-size:1.05rem;color:#64748b;margin-top:0.25rem;">'
    'Powered by <strong style="color:#29B5E8;">Snowflake Cortex AI_EXTRACT</strong>'
    "</p></div>",
    unsafe_allow_html=True,
)

# ── Filter ────────────────────────────────────────────────────────────────────
doc_types = get_doc_types(session)
selected_type = st.selectbox("Document Type", ["ALL"] + doc_types, index=0)

labels = get_doc_type_labels(
    session, selected_type if selected_type != "ALL" else "INVOICE"
)
sender_label = labels.get("sender_label", "Sender")
amount_label = labels.get("amount_label", "Total Amount")

type_clause = ""
type_and = ""
type_params = []
if selected_type != "ALL":
    type_clause = "WHERE rd.doc_type = ?"
    type_and = "AND rd.doc_type = ?"
    type_params = [selected_type]

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# KPI CARDS
# ══════════════════════════════════════════════════════════════════════════════

kpis = _safe_query(
    f"""
    SELECT
        COUNT(*)                                    AS total_documents,
        COALESCE(SUM(ef.field_10), 0)               AS total_amount,
        COUNT(DISTINCT ef.field_1)                   AS unique_senders,
        COALESCE(AVG(ef.field_10), 0)               AS avg_amount,
        COUNT(CASE WHEN ef.field_5 IS NOT NULL
                    AND ef.field_5 < CURRENT_DATE() THEN 1 END)  AS overdue_count,
        COALESCE(SUM(CASE WHEN ef.field_5 IS NOT NULL
                          AND ef.field_5 < CURRENT_DATE()
                     THEN ef.field_10 ELSE 0 END), 0)           AS overdue_amount
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    {type_clause}
""",
    type_params,
)

# Monthly trend for sparklines
monthly = _safe_query(
    f"""
    SELECT
        DATE_TRUNC('month', ef.field_4) AS month,
        COUNT(*)                         AS doc_count,
        SUM(ef.field_10)                 AS total_amount
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    WHERE ef.field_4 IS NOT NULL {type_and}
    GROUP BY DATE_TRUNC('month', ef.field_4)
    ORDER BY month
""",
    type_params,
)

if len(kpis) > 0 and kpis.iloc[0]["TOTAL_DOCUMENTS"] > 0:
    k = kpis.iloc[0]

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("Documents Processed", f"{int(k['TOTAL_DOCUMENTS']):,}")
        if len(monthly) >= 3:
            st.plotly_chart(
                _sparkline(monthly["DOC_COUNT"].tolist(), "#29B5E8"),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    with c2:
        st.metric("Total Spend", f"${k['TOTAL_AMOUNT']:,.0f}")
        if len(monthly) >= 3:
            st.plotly_chart(
                _sparkline(monthly["TOTAL_AMOUNT"].tolist(), "#1e3a5f"),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    with c3:
        st.metric(f"Unique {sender_label}s", f"{int(k['UNIQUE_SENDERS']):,}")
        st.metric("Avg per Document", f"${k['AVG_AMOUNT']:,.0f}")

    with c4:
        overdue_amt = k["OVERDUE_AMOUNT"]
        overdue_cnt = int(k["OVERDUE_COUNT"])
        if overdue_cnt > 0:
            st.metric(
                "Overdue",
                f"${overdue_amt:,.0f}",
                delta=f"{overdue_cnt} docs past due",
                delta_color="inverse",
            )
        else:
            st.metric("Overdue", "$0", delta="All current", delta_color="off")

else:
    st.info(
        "No documents extracted yet. Upload documents and run the extraction pipeline to get started."
    )
    render_nav_bar()
    st.stop()


st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE HEALTH + TOP SENDERS (side by side)
# ══════════════════════════════════════════════════════════════════════════════

left_col, right_col = st.columns([1, 1])

# ── Pipeline Health ───────────────────────────────────────────────────────────
with left_col:
    st.markdown("##### Pipeline Status")

    status = _safe_query(f"SELECT * FROM {DB}.V_EXTRACTION_STATUS")
    if len(status) > 0:
        s = status.iloc[0]
        total = int(s["TOTAL_FILES"])
        extracted = int(s["EXTRACTED_FILES"])
        pending = int(s["PENDING_FILES"])
        failed = int(s["FAILED_FILES"])
        pct = round(extracted / total * 100) if total > 0 else 0

        # Progress bar
        st.progress(pct / 100, text=f"{extracted}/{total} processed ({pct}%)")

        pc1, pc2, pc3 = st.columns(3)
        pc1.metric("Extracted", f"{extracted:,}")
        pc2.metric("Pending", f"{pending:,}")
        pc3.metric("Failed", f"{failed:,}", delta_color="inverse")

        if s.get("LAST_EXTRACTION"):
            st.caption(f"Last extraction: {s['LAST_EXTRACTION']}")
    else:
        st.warning("Pipeline status unavailable — check that views are deployed.")

# ── Top Senders ───────────────────────────────────────────────────────────────
with right_col:
    st.markdown(f"##### Top {sender_label}s by Spend")

    top_senders = _safe_query(
        f"""
        SELECT
            ef.field_1       AS sender,
            COUNT(*)         AS doc_count,
            SUM(ef.field_10) AS total_spend
        FROM {DB}.EXTRACTED_FIELDS ef
            JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
        WHERE ef.field_1 IS NOT NULL {type_and}
        GROUP BY ef.field_1
        ORDER BY total_spend DESC
        LIMIT 5
    """,
        type_params,
    )

    if len(top_senders) > 0:
        for _, row in top_senders.iterrows():
            sender = row["SENDER"] or "Unknown"
            spend = row["TOTAL_SPEND"] or 0
            docs = int(row["DOC_COUNT"])
            # Proportion bar
            max_spend = top_senders["TOTAL_SPEND"].max()
            pct = spend / max_spend if max_spend > 0 else 0
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">'
                f'<div style="flex:1;min-width:0;">'
                f'<div style="font-size:0.85rem;font-weight:600;color:#1e293b;'
                f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{sender}</div>'
                f'<div style="background:#e2e8f0;border-radius:4px;height:6px;margin-top:3px;">'
                f'<div style="background:#1e3a5f;height:100%;border-radius:4px;width:{pct*100:.0f}%;"></div>'
                f"</div></div>"
                f'<div style="text-align:right;min-width:90px;">'
                f'<div style="font-size:0.85rem;font-weight:600;color:#1e293b;">${spend:,.0f}</div>'
                f'<div style="font-size:0.7rem;color:#64748b;">{docs} docs</div>'
                f"</div></div>",
                unsafe_allow_html=True,
            )
    else:
        st.info("No sender data available.")


st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# RECENT DOCUMENTS + AGING (side by side)
# ══════════════════════════════════════════════════════════════════════════════

left2, right2 = st.columns([2, 1])

# ── Recent Documents ──────────────────────────────────────────────────────────
with left2:
    st.markdown("##### Recent Documents")

    recent = _safe_query(
        f"""
        SELECT
            ef.field_1       AS sender,
            ef.field_2       AS document_number,
            ef.field_4       AS document_date,
            ef.field_10      AS total_amount,
            ef.extracted_at
        FROM {DB}.EXTRACTED_FIELDS ef
            JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
        {type_clause}
        ORDER BY ef.extracted_at DESC NULLS LAST
        LIMIT 10
    """,
        type_params,
    )

    if len(recent) > 0:
        st.dataframe(
            recent,
            column_config={
                "SENDER": sender_label,
                "DOCUMENT_NUMBER": labels.get("reference_label", "Document #"),
                "DOCUMENT_DATE": st.column_config.DateColumn(
                    labels.get("date_label", "Date")
                ),
                "TOTAL_AMOUNT": st.column_config.NumberColumn(
                    amount_label, format="$%.2f"
                ),
                "EXTRACTED_AT": st.column_config.DatetimeColumn(
                    "Extracted", format="MMM D, h:mm a"
                ),
            },
            hide_index=True,
            use_container_width=True,
            height=390,
        )
    else:
        st.info("No documents extracted yet.")


# ── Aging Snapshot ────────────────────────────────────────────────────────────
with right2:
    st.markdown("##### Aging Snapshot")

    aging = _safe_query(
        f"""
        SELECT
            aging_bucket,
            COUNT(*)           AS doc_count,
            SUM(total_amount)  AS total_amount,
            sort_order
        FROM (
            SELECT total_amount, doc_type,
                CASE
                    WHEN due_date IS NULL              THEN 'N/A'
                    WHEN due_date >= CURRENT_DATE()    THEN 'Current'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 30  THEN '1-30 Days'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 60  THEN '31-60 Days'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 90  THEN '61-90 Days'
                    ELSE '90+ Days'
                END AS aging_bucket,
                CASE
                    WHEN due_date IS NULL THEN 99
                    WHEN due_date >= CURRENT_DATE()    THEN 0
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 30  THEN 1
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 60  THEN 2
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 90  THEN 3
                    ELSE 4
                END AS sort_order
            FROM {DB}.V_DOCUMENT_LEDGER
        ) sub
        WHERE aging_bucket != 'N/A'
            {"AND doc_type = ?" if type_params else ""}
        GROUP BY aging_bucket, sort_order
        ORDER BY sort_order
    """,
        type_params,
    )

    if len(aging) > 0:
        color_map = {
            "Current": "#22c55e",
            "1-30 Days": "#f59e0b",
            "31-60 Days": "#ef4444",
            "61-90 Days": "#b91c1c",
            "90+ Days": "#450a0a",
        }
        for _, row in aging.iterrows():
            bucket = row["AGING_BUCKET"]
            amt = row["TOTAL_AMOUNT"] or 0
            cnt = int(row["DOC_COUNT"])
            dot_color = color_map.get(bucket, "#94a3b8")
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;align-items:center;'
                f"padding:8px 12px;margin-bottom:4px;border-radius:6px;"
                f'background:#f8fafc;border-left:4px solid {dot_color};">'
                f"<div>"
                f'<div style="font-size:0.85rem;font-weight:600;color:#1e293b;">{bucket}</div>'
                f'<div style="font-size:0.72rem;color:#64748b;">{cnt} document{"s" if cnt != 1 else ""}</div>'
                f"</div>"
                f'<div style="font-size:0.95rem;font-weight:700;color:#1e293b;">${amt:,.0f}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )

        # Overdue total
        overdue_rows = aging[aging["SORT_ORDER"].between(1, 4)]
        if len(overdue_rows) > 0:
            ov_total = overdue_rows["TOTAL_AMOUNT"].sum()
            ov_count = int(overdue_rows["DOC_COUNT"].sum())
            if ov_total > 0:
                st.markdown(
                    f'<div style="margin-top:8px;padding:10px 12px;border-radius:6px;'
                    f'background:#fef2f2;border:1px solid #fecaca;text-align:center;">'
                    f'<div style="font-size:0.78rem;color:#991b1b;font-weight:600;">'
                    f"${ov_total:,.0f} past due ({ov_count} docs)</div></div>",
                    unsafe_allow_html=True,
                )
    else:
        st.caption("No aging data available.")


st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# MONTHLY TREND (full width)
# ══════════════════════════════════════════════════════════════════════════════

if len(monthly) >= 2:
    st.markdown("##### Monthly Trend")

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=monthly["MONTH"],
            y=monthly["TOTAL_AMOUNT"],
            name="Spend",
            marker_color="#1e3a5f",
            text=monthly["TOTAL_AMOUNT"].apply(lambda v: f"${v:,.0f}" if v else ""),
            textposition="outside",
            textfont_size=10,
            hovertemplate="<b>%{x|%b %Y}</b><br>$%{y:,.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=monthly["MONTH"],
            y=monthly["DOC_COUNT"],
            name="Documents",
            mode="lines+markers",
            marker=dict(color="#29B5E8", size=7),
            line=dict(color="#29B5E8", width=2),
            yaxis="y2",
            hovertemplate="<b>%{x|%b %Y}</b><br>%{y} docs<extra></extra>",
        )
    )
    fig.update_layout(
        height=300,
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(title="", showgrid=True, gridcolor="rgba(0,0,0,0.05)"),
        yaxis2=dict(title="", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.15, x=0.5, xanchor="center"),
        font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


render_nav_bar()
