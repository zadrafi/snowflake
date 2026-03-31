"""
KPI Overview Dashboard
"""

import streamlit as st
from config import DB

st.title("Accounts Payable — Invoice Processing Dashboard")
st.caption("Powered by Snowflake AI_EXTRACT | Convenience Store Invoice Pipeline")

# --- Snowflake connection ---
conn = st.connection("snowflake")


def get_kpis():
    """Fetch aggregate AP metrics."""
    df = conn.query(
        f"""
        SELECT
            COUNT(*)                                                    AS total_invoices,
            SUM(total_amount)                                           AS total_spend,
            SUM(CASE WHEN status != 'PAID' THEN total_amount ELSE 0 END) AS total_outstanding,
            SUM(CASE WHEN status != 'PAID' AND due_date < CURRENT_DATE()
                     THEN total_amount ELSE 0 END)                      AS total_overdue,
            COUNT(CASE WHEN status != 'PAID' AND due_date < CURRENT_DATE()
                       THEN 1 END)                                      AS overdue_count,
            AVG(CASE WHEN status = 'PAID'
                     THEN DATEDIFF(day, invoice_date, payment_date) END) AS avg_days_to_pay,
            COUNT(DISTINCT vendor_name)                                 AS vendor_count
        FROM {DB}.EXTRACTED_INVOICES
        """,
        ttl=30,
    )
    return df.iloc[0]


def get_extraction_status():
    """Fetch pipeline status."""
    df = conn.query(
        f"""
        SELECT
            total_files,
            extracted_files,
            pending_files,
            failed_files,
            last_extraction
        FROM {DB}.V_EXTRACTION_STATUS
        """,
        ttl=10,
    )
    return df.iloc[0] if len(df) > 0 else None


def get_recent_invoices():
    """Fetch the 10 most recent invoices."""
    return conn.query(
        f"""
        SELECT
            invoice_number,
            vendor_name,
            invoice_date,
            due_date,
            total_amount,
            status,
            aging_bucket
        FROM {DB}.V_AP_LEDGER
        ORDER BY extracted_at DESC NULLS LAST
        LIMIT 10
        """,
        ttl=30,
    )


# --- KPI Cards ---
kpis = get_kpis()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Invoices",
        value=f"{int(kpis['TOTAL_INVOICES']):,}",
    )

with col2:
    st.metric(
        label="Total Spend",
        value=f"${kpis['TOTAL_SPEND']:,.0f}",
    )

with col3:
    st.metric(
        label="Outstanding",
        value=f"${kpis['TOTAL_OUTSTANDING']:,.0f}",
    )

with col4:
    overdue_val = kpis["TOTAL_OVERDUE"] if kpis["TOTAL_OVERDUE"] else 0
    st.metric(
        label="Overdue",
        value=f"${overdue_val:,.0f}",
        delta=f"{int(kpis['OVERDUE_COUNT'])} invoices" if kpis["OVERDUE_COUNT"] else "0 invoices",
        delta_color="inverse",
    )

st.divider()

# --- Secondary metrics ---
col5, col6, col7 = st.columns(3)

with col5:
    avg_days = kpis["AVG_DAYS_TO_PAY"]
    st.metric(
        label="Avg Days to Pay",
        value=f"{avg_days:.0f} days" if avg_days else "N/A",
    )

with col6:
    st.metric(
        label="Active Vendors",
        value=f"{int(kpis['VENDOR_COUNT'])}",
    )

with col7:
    status = get_extraction_status()
    if status is not None:
        st.metric(
            label="Extraction Pipeline",
            value=f"{int(status['EXTRACTED_FILES'])}/{int(status['TOTAL_FILES'])} processed",
        )
    else:
        st.metric(label="Extraction Pipeline", value="No data")

st.divider()

# --- Recent invoices ---
st.subheader("Recently Processed Invoices")
recent = get_recent_invoices()

if len(recent) > 0:
    st.dataframe(
        recent,
        column_config={
            "INVOICE_NUMBER": "Invoice #",
            "VENDOR_NAME": "Vendor",
            "INVOICE_DATE": st.column_config.DateColumn("Invoice Date"),
            "DUE_DATE": st.column_config.DateColumn("Due Date"),
            "TOTAL_AMOUNT": st.column_config.NumberColumn("Total", format="$%.2f"),
            "STATUS": "Status",
            "AGING_BUCKET": "Aging",
        },
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("No invoices processed yet. Run the extraction pipeline first.")
