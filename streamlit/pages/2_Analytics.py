"""
Page 2: Analytics — Spend by vendor, category, monthly trends, aging distribution.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from config import DB

st.set_page_config(page_title="AP Analytics", page_icon="📊", layout="wide")

conn = st.connection("snowflake")

st.title("Accounts Payable Analytics")
st.caption("Spend analysis across vendors, categories, and time periods")

# --- Spend by Vendor (horizontal bar) ---
st.subheader("Spend by Vendor")

vendor_df = conn.query(
    f"""
    SELECT
        vendor_name,
        invoice_count,
        total_spend,
        avg_invoice_amount
    FROM {DB}.V_SPEND_BY_VENDOR
    ORDER BY total_spend DESC
    LIMIT 15
    """,
    ttl=60,
)

if len(vendor_df) > 0:
    fig_vendor = px.bar(
        vendor_df,
        x="TOTAL_SPEND",
        y="VENDOR_NAME",
        orientation="h",
        color="TOTAL_SPEND",
        color_continuous_scale="Blues",
        labels={"TOTAL_SPEND": "Total Spend ($)", "VENDOR_NAME": "Vendor"},
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

st.divider()

# --- Monthly Trend + Category Breakdown side by side ---
col1, col2 = st.columns(2)

# Monthly Trend
with col1:
    st.subheader("Monthly Spend Trend")
    monthly_df = conn.query(
        f"""
        SELECT
            month,
            invoice_count,
            total_spend,
            total_tax
        FROM {DB}.V_MONTHLY_TREND
        ORDER BY month
        """,
        ttl=60,
    )

    if len(monthly_df) > 0:
        fig_monthly = px.area(
            monthly_df,
            x="MONTH",
            y="TOTAL_SPEND",
            labels={"TOTAL_SPEND": "Total Spend ($)", "MONTH": "Month"},
            color_discrete_sequence=["#1a237e"],
        )
        fig_monthly.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

        # Summary stats
        total = monthly_df["TOTAL_SPEND"].sum()
        avg_monthly = monthly_df["TOTAL_SPEND"].mean()
        st.caption(f"Total: ${total:,.0f} | Monthly Avg: ${avg_monthly:,.0f}")

# Category Breakdown
with col2:
    st.subheader("Spend by Category")
    category_df = conn.query(
        f"""
        SELECT
            category,
            total_spend,
            total_units,
            invoice_count
        FROM {DB}.V_SPEND_BY_CATEGORY
        ORDER BY total_spend DESC
        """,
        ttl=60,
    )

    if len(category_df) > 0:
        fig_category = px.treemap(
            category_df,
            path=["CATEGORY"],
            values="TOTAL_SPEND",
            color="TOTAL_SPEND",
            color_continuous_scale="Blues",
            labels={"TOTAL_SPEND": "Spend ($)"},
        )
        fig_category.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_category, use_container_width=True)

st.divider()

# --- Aging Distribution + Top Line Items side by side ---
col3, col4 = st.columns(2)

# Aging Distribution
with col3:
    st.subheader("Aging Distribution")
    aging_df = conn.query(
        f"""
        SELECT
            aging_bucket,
            invoice_count,
            total_outstanding,
            sort_order
        FROM {DB}.V_AGING_SUMMARY
        WHERE aging_bucket != 'Paid'
        ORDER BY sort_order
        """,
        ttl=30,
    )

    if len(aging_df) > 0:
        # Color mapping for aging severity
        color_map = {
            "Current": "#4caf50",
            "1-30 Days": "#ff9800",
            "31-60 Days": "#f44336",
            "61-90 Days": "#b71c1c",
            "90+ Days": "#4a0000",
        }
        aging_df["COLOR"] = aging_df["AGING_BUCKET"].map(color_map)

        fig_aging = px.bar(
            aging_df,
            x="AGING_BUCKET",
            y="TOTAL_OUTSTANDING",
            color="AGING_BUCKET",
            color_discrete_map=color_map,
            labels={
                "TOTAL_OUTSTANDING": "Outstanding ($)",
                "AGING_BUCKET": "Aging Bucket",
            },
            text_auto="$.2s",
        )
        fig_aging.update_layout(
            showlegend=False,
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_aging, use_container_width=True)

# Top Line Items
with col4:
    st.subheader("Top 20 Products by Spend")
    top_items_df = conn.query(
        f"""
        SELECT
            product_name,
            category,
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
                "PRODUCT_NAME": "Product",
                "CATEGORY": "Category",
                "TOTAL_QUANTITY": st.column_config.NumberColumn("Total Qty", format="%.0f"),
                "AVG_UNIT_PRICE": st.column_config.NumberColumn("Avg Price", format="$%.2f"),
                "TOTAL_SPEND": st.column_config.NumberColumn("Total Spend", format="$%.2f"),
            },
            hide_index=True,
            use_container_width=True,
            height=350,
        )

st.divider()

# --- Vendor Payment Terms Summary ---
st.subheader("Vendor Payment Terms Summary")
terms_df = conn.query(
    f"""
    SELECT
        vendor_name,
        payment_terms,
        invoice_count,
        total_spend,
        paid_amount,
        outstanding_amount
    FROM {DB}.V_VENDOR_PAYMENT_TERMS
    ORDER BY total_spend DESC
    """,
    ttl=60,
)

if len(terms_df) > 0:
    st.dataframe(
        terms_df,
        column_config={
            "VENDOR_NAME": "Vendor",
            "PAYMENT_TERMS": "Terms",
            "INVOICE_COUNT": "Invoices",
            "TOTAL_SPEND": st.column_config.NumberColumn("Total Spend", format="$%.2f"),
            "PAID_AMOUNT": st.column_config.NumberColumn("Paid", format="$%.2f"),
            "OUTSTANDING_AMOUNT": st.column_config.NumberColumn("Outstanding", format="$%.2f"),
        },
        hide_index=True,
        use_container_width=True,
    )
