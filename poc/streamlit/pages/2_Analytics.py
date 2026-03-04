"""
Page 2: Analytics — Spend/volume by sender, monthly trends, top line items.
"""

import streamlit as st
import plotly.express as px
from config import DB

st.set_page_config(page_title="Analytics", page_icon="📊", layout="wide")

conn = st.connection("snowflake")

st.title("Document Analytics")
st.caption("Insights from your extracted document data")


# --- Summary by Sender (horizontal bar) ---
st.subheader("Amount by Sender")

vendor_df = conn.query(
    f"""
    SELECT
        vendor_name,
        document_count,
        total_amount,
        avg_amount
    FROM {DB}.V_SUMMARY_BY_VENDOR
    ORDER BY total_amount DESC
    LIMIT 15
    """,
    ttl=60,
)

if len(vendor_df) > 0:
    fig_vendor = px.bar(
        vendor_df,
        x="TOTAL_AMOUNT",
        y="VENDOR_NAME",
        orientation="h",
        color="TOTAL_AMOUNT",
        color_continuous_scale="Blues",
        labels={"TOTAL_AMOUNT": "Total Amount ($)", "VENDOR_NAME": "Sender"},
        text_auto="$.2s",
    )
    fig_vendor.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        yaxis=dict(autorange="reversed"),
        height=400,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_vendor, use_container_width=True)
else:
    st.info("No data available. Run extraction first.")

st.divider()

# --- Monthly Trend + Aging side by side ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Monthly Trend")
    monthly_df = conn.query(
        f"""
        SELECT month, document_count, total_amount, total_tax
        FROM {DB}.V_MONTHLY_TREND
        ORDER BY month
        """,
        ttl=60,
    )

    if len(monthly_df) > 0:
        fig_monthly = px.area(
            monthly_df,
            x="MONTH",
            y="TOTAL_AMOUNT",
            labels={"TOTAL_AMOUNT": "Total Amount ($)", "MONTH": "Month"},
            color_discrete_sequence=["#1a237e"],
        )
        fig_monthly.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

        total = monthly_df["TOTAL_AMOUNT"].sum()
        avg_monthly = monthly_df["TOTAL_AMOUNT"].mean()
        st.caption(f"Total: ${total:,.0f} | Monthly Avg: ${avg_monthly:,.0f}")

with col2:
    st.subheader("Aging Distribution")
    aging_df = conn.query(
        f"""
        SELECT aging_bucket, document_count, total_amount, sort_order
        FROM {DB}.V_AGING_SUMMARY
        WHERE aging_bucket != 'N/A'
        ORDER BY sort_order
        """,
        ttl=30,
    )

    if len(aging_df) > 0:
        color_map = {
            "Current": "#4caf50",
            "1-30 Days": "#ff9800",
            "31-60 Days": "#f44336",
            "61-90 Days": "#b71c1c",
            "90+ Days": "#4a0000",
        }
        fig_aging = px.bar(
            aging_df,
            x="AGING_BUCKET",
            y="TOTAL_AMOUNT",
            color="AGING_BUCKET",
            color_discrete_map=color_map,
            labels={"TOTAL_AMOUNT": "Amount ($)", "AGING_BUCKET": "Aging Bucket"},
            text_auto="$.2s",
        )
        fig_aging.update_layout(
            showlegend=False,
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_aging, use_container_width=True)

st.divider()

# --- Top Line Items ---
st.subheader("Top 20 Items by Amount")

top_items_df = conn.query(
    f"""
    SELECT
        item_description,
        category,
        appearance_count,
        total_quantity,
        avg_unit_price,
        total_spend
    FROM {DB}.V_TOP_LINE_ITEMS
    LIMIT 20
    """,
    ttl=60,
)

if len(top_items_df) > 0:
    st.dataframe(
        top_items_df,
        column_config={
            "ITEM_DESCRIPTION": "Item",
            "CATEGORY": "Category",
            "APPEARANCE_COUNT": "Occurrences",
            "TOTAL_QUANTITY": st.column_config.NumberColumn("Total Qty", format="%.0f"),
            "AVG_UNIT_PRICE": st.column_config.NumberColumn("Avg Price", format="$%.2f"),
            "TOTAL_SPEND": st.column_config.NumberColumn("Total Amount", format="$%.2f"),
        },
        hide_index=True,
        use_container_width=True,
    )
