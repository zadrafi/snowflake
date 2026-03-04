"""
Page 0: Dashboard — KPI cards, recent documents, extraction metrics.
"""

import streamlit as st
from config import DB

st.title("Document Processing Dashboard")
st.caption("Powered by Snowflake Cortex AI_EXTRACT")

conn = st.connection("snowflake")


# --- KPI Cards ---
kpis = conn.query(
    f"""
    SELECT
        COUNT(*)                                                     AS total_documents,
        SUM(field_10)                                                AS total_amount,
        COUNT(DISTINCT field_1)                                      AS unique_senders,
        COUNT(CASE WHEN field_5 IS NOT NULL
                    AND field_5 < CURRENT_DATE() THEN 1 END)        AS overdue_count,
        SUM(CASE WHEN field_5 IS NOT NULL
                  AND field_5 < CURRENT_DATE() THEN field_10 ELSE 0 END) AS overdue_amount
    FROM {DB}.EXTRACTED_FIELDS
    """,
    ttl=30,
)

if len(kpis) > 0:
    k = kpis.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Documents", f"{int(k['TOTAL_DOCUMENTS']):,}")

    with col2:
        total = k["TOTAL_AMOUNT"] or 0
        st.metric("Total Amount", f"${total:,.0f}")

    with col3:
        st.metric("Unique Senders", f"{int(k['UNIQUE_SENDERS']):,}")

    with col4:
        overdue = k["OVERDUE_AMOUNT"] or 0
        st.metric(
            "Overdue",
            f"${overdue:,.0f}",
            delta=f"{int(k['OVERDUE_COUNT'])} documents" if k["OVERDUE_COUNT"] else "0",
            delta_color="inverse",
        )

st.divider()

# --- Extraction Pipeline Status ---
status = conn.query(f"SELECT * FROM {DB}.V_EXTRACTION_STATUS", ttl=10)

if len(status) > 0:
    s = status.iloc[0]
    col5, col6, col7 = st.columns(3)
    with col5:
        st.metric(
            "Pipeline Progress",
            f"{int(s['EXTRACTED_FILES'])}/{int(s['TOTAL_FILES'])} processed",
        )
    with col6:
        st.metric("Pending", f"{int(s['PENDING_FILES'])}")
    with col7:
        st.metric("Failed", f"{int(s['FAILED_FILES'])}")

st.divider()

# --- Recent Documents ---
st.subheader("Recently Extracted Documents")

recent = conn.query(
    f"""
    SELECT
        field_2       AS document_number,
        field_1       AS sender,
        field_4       AS document_date,
        field_5       AS due_date,
        field_10      AS total_amount,
        status,
        extracted_at
    FROM {DB}.EXTRACTED_FIELDS
    ORDER BY extracted_at DESC NULLS LAST
    LIMIT 15
    """,
    ttl=30,
)

if len(recent) > 0:
    st.dataframe(
        recent,
        column_config={
            "DOCUMENT_NUMBER": "Document #",
            "SENDER": "Sender",
            "DOCUMENT_DATE": st.column_config.DateColumn("Date"),
            "DUE_DATE": st.column_config.DateColumn("Due Date"),
            "TOTAL_AMOUNT": st.column_config.NumberColumn("Amount", format="$%.2f"),
            "STATUS": "Status",
            "EXTRACTED_AT": "Extracted At",
        },
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("No documents extracted yet. Run the extraction pipeline first (sql/04_batch_extract.sql).")
