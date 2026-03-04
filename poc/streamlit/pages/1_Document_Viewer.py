"""
Page 1: Document Viewer — Browse documents, view extracted fields + source PDF.
"""

import streamlit as st
import pandas as pd
import tempfile
import os
import pypdfium2 as pdfium
from config import DB, STAGE

st.set_page_config(page_title="Document Viewer", page_icon="📋", layout="wide")

conn = st.connection("snowflake")

st.title("Document Viewer")
st.caption("Browse extracted documents — select any row to view source PDF and extracted fields")

# --- Filters ---
col_f1, col_f2 = st.columns(2)

senders = conn.query(
    f"SELECT DISTINCT field_1 AS sender FROM {DB}.EXTRACTED_FIELDS WHERE field_1 IS NOT NULL ORDER BY sender",
    ttl=60,
)
sender_list = ["All"] + senders["SENDER"].tolist()

with col_f1:
    selected_sender = st.selectbox("Sender / Vendor", sender_list)

with col_f2:
    selected_status = st.selectbox("Status", ["All", "EXTRACTED"])

# --- Build filtered query ---
session = conn.session()

where_clauses = []
params = []
if selected_sender != "All":
    where_clauses.append("field_1 = ?")
    params.append(selected_sender)
if selected_status != "All":
    where_clauses.append("status = ?")
    params.append(selected_status)

where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

ledger_query = f"""
    SELECT
        record_id,
        file_name,
        field_1       AS sender,
        field_2       AS document_number,
        field_4       AS document_date,
        field_5       AS due_date,
        field_6       AS terms,
        field_8       AS subtotal,
        field_9       AS tax,
        field_10      AS total_amount,
        status
    FROM {DB}.EXTRACTED_FIELDS
    {where_sql}
    ORDER BY field_4 DESC NULLS LAST
"""

ledger_df = session.sql(ledger_query, params=params).to_pandas()

# --- Document list ---
st.subheader(f"Documents ({len(ledger_df)} results)")

if len(ledger_df) > 0:
    # --- Aging cards (only if due_date fields exist) ---
    aging_data = conn.query(
        f"SELECT * FROM {DB}.V_AGING_SUMMARY ORDER BY sort_order",
        ttl=30,
    )
    if len(aging_data) > 0:
        aging_cols = st.columns(len(aging_data))
        for i, row in aging_data.iterrows():
            with aging_cols[i]:
                label = row["AGING_BUCKET"]
                count = int(row["DOCUMENT_COUNT"])
                amount = row["TOTAL_AMOUNT"]
                if amount and amount > 0:
                    st.metric(label=label, value=f"${amount:,.0f}", delta=f"{count} docs")
                else:
                    st.metric(label=label, value=f"{count} docs")
        st.divider()

    st.dataframe(
        ledger_df,
        column_config={
            "RECORD_ID": None,
            "FILE_NAME": None,
            "SENDER": "Sender",
            "DOCUMENT_NUMBER": "Document #",
            "DOCUMENT_DATE": st.column_config.DateColumn("Date"),
            "DUE_DATE": st.column_config.DateColumn("Due Date"),
            "TERMS": "Terms",
            "SUBTOTAL": st.column_config.NumberColumn("Subtotal", format="$%.2f"),
            "TAX": st.column_config.NumberColumn("Tax", format="$%.2f"),
            "TOTAL_AMOUNT": st.column_config.NumberColumn("Total", format="$%.2f"),
            "STATUS": "Status",
        },
        hide_index=True,
        use_container_width=True,
    )

    # --- Drill-down: select a document ---
    st.divider()
    st.subheader("Document Detail")

    doc_options = ledger_df["DOCUMENT_NUMBER"].dropna().tolist()
    if not doc_options:
        doc_options = ledger_df["FILE_NAME"].tolist()

    selected_doc = st.selectbox("Select a document to view details", doc_options)

    if selected_doc:
        # Get the full record
        file_row = session.sql(
            f"""
            SELECT file_name, field_1, field_2, field_3, field_4, field_5,
                   field_6, field_7, field_8, field_9, field_10
            FROM {DB}.EXTRACTED_FIELDS
            WHERE field_2 = ? OR file_name = ?
            LIMIT 1
            """,
            params=[selected_doc, selected_doc],
        ).to_pandas()

        col_pdf, col_fields = st.columns([1, 1])

        # Left: Source PDF
        with col_pdf:
            st.markdown("**Source Document**")
            if len(file_row) > 0 and file_row.iloc[0]["FILE_NAME"]:
                file_name = file_row.iloc[0]["FILE_NAME"]
                try:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        stage_path = f"@{STAGE}/{file_name}"
                        session.file.get(stage_path, tmpdir)
                        local_path = os.path.join(tmpdir, file_name)
                        pdf = pdfium.PdfDocument(local_path)
                        for page_idx in range(len(pdf)):
                            page = pdf[page_idx]
                            bitmap = page.render(scale=2)
                            pil_image = bitmap.to_pil()
                            st.image(pil_image, use_container_width=True)
                        pdf.close()
                except Exception as e:
                    st.warning(f"Could not render document: {e}")
            else:
                st.info("No source file available.")

        # Right: Extracted fields + line items
        with col_fields:
            st.markdown("**Extracted Fields**")
            if len(file_row) > 0:
                inv = file_row.iloc[0]
                h1, h2 = st.columns(2)
                with h1:
                    st.markdown(f"**Sender:** {inv['FIELD_1'] or 'N/A'}")
                    st.markdown(f"**Document #:** {inv['FIELD_2'] or 'N/A'}")
                    st.markdown(f"**Reference:** {inv['FIELD_3'] or 'N/A'}")
                    st.markdown(f"**Recipient:** {inv['FIELD_7'] or 'N/A'}")
                with h2:
                    st.markdown(f"**Date:** {inv['FIELD_4'] or 'N/A'}")
                    st.markdown(f"**Due Date:** {inv['FIELD_5'] or 'N/A'}")
                    st.markdown(f"**Terms:** {inv['FIELD_6'] or 'N/A'}")

                t1, t2, t3 = st.columns(3)
                subtotal = inv["FIELD_8"] or 0
                tax = inv["FIELD_9"] or 0
                total = inv["FIELD_10"] or 0
                t1.metric("Subtotal", f"${subtotal:,.2f}")
                t2.metric("Tax", f"${tax:,.2f}")
                t3.metric("Total", f"${total:,.2f}")

            st.divider()
            st.markdown("**Extracted Line Items**")

            file_name_for_lines = file_row.iloc[0]["FILE_NAME"] if len(file_row) > 0 else ""
            line_items = session.sql(
                f"""
                SELECT
                    line_number,
                    col_1       AS description,
                    col_2       AS category,
                    col_3       AS quantity,
                    col_4       AS unit_price,
                    col_5       AS line_total
                FROM {DB}.EXTRACTED_TABLE_DATA
                WHERE file_name = ?
                ORDER BY line_number
                """,
                params=[file_name_for_lines],
            ).to_pandas()

            if len(line_items) > 0:
                st.dataframe(
                    line_items,
                    column_config={
                        "LINE_NUMBER": "#",
                        "DESCRIPTION": "Description",
                        "CATEGORY": "Category",
                        "QUANTITY": st.column_config.NumberColumn("Qty", format="%.0f"),
                        "UNIT_PRICE": st.column_config.NumberColumn("Unit Price", format="$%.2f"),
                        "LINE_TOTAL": st.column_config.NumberColumn("Total", format="$%.2f"),
                    },
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.info("No line items found for this document.")
else:
    st.info("No documents match the selected filters.")
