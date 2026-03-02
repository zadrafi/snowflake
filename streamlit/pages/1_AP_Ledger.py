"""
Page 1: AP Ledger — Invoice list with aging buckets, filtering, and line-item drill-down.
"""

import streamlit as st
import pandas as pd
import tempfile
import os
import pypdfium2 as pdfium
from io import BytesIO
from config import DB, STAGE

st.set_page_config(page_title="AP Ledger", page_icon="📋", layout="wide")

conn = st.connection("snowflake")

st.title("Accounts Payable Ledger")
st.caption("All invoices extracted from PDF documents via AI_EXTRACT")

# --- Filters ---
col_f1, col_f2, col_f3 = st.columns(3)

vendors = conn.query(
    f"SELECT DISTINCT vendor_name FROM {DB}.EXTRACTED_INVOICES ORDER BY vendor_name",
    ttl=60,
)
vendor_list = ["All Vendors"] + vendors["VENDOR_NAME"].tolist()

with col_f1:
    selected_vendor = st.selectbox("Vendor", vendor_list)

with col_f2:
    selected_status = st.selectbox("Status", ["All", "PENDING", "APPROVED", "PAID"])

with col_f3:
    selected_aging = st.selectbox(
        "Aging Bucket",
        ["All", "Current", "1-30 Days", "31-60 Days", "61-90 Days", "90+ Days", "Paid"],
    )

# --- Build query with filters (parameterized) ---
session = conn.session()

where_clauses = []
params = {}
if selected_vendor != "All Vendors":
    where_clauses.append("vendor_name = ?")
    params["vendor"] = selected_vendor
if selected_status != "All":
    where_clauses.append("status = ?")
    params["status"] = selected_status
if selected_aging != "All":
    where_clauses.append("aging_bucket = ?")
    params["aging"] = selected_aging

where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

ledger_query = f"""
    SELECT
        invoice_id,
        invoice_number,
        vendor_name,
        invoice_date,
        due_date,
        payment_terms,
        subtotal,
        tax_amount,
        total_amount,
        status,
        aging_bucket,
        days_past_due,
        outstanding_amount,
        payment_date
    FROM {DB}.V_AP_LEDGER
    {where_sql}
    ORDER BY due_date DESC
"""

ledger_df = session.sql(ledger_query, params=list(params.values())).to_pandas()

# --- Aging summary cards ---
st.subheader("Aging Summary")
aging_df = conn.query(
    f"SELECT * FROM {DB}.V_AGING_SUMMARY ORDER BY sort_order", ttl=30
)

if len(aging_df) > 0:
    aging_cols = st.columns(len(aging_df))
    for i, row in aging_df.iterrows():
        with aging_cols[i]:
            label = row["AGING_BUCKET"]
            count = int(row["INVOICE_COUNT"])
            amount = row["TOTAL_OUTSTANDING"]
            if amount and amount > 0:
                st.metric(label=label, value=f"${amount:,.0f}", delta=f"{count} invoices")
            else:
                st.metric(label=label, value=f"{count} invoices")

st.divider()

# --- Invoice table ---
st.subheader(f"Invoices ({len(ledger_df)} results)")

if len(ledger_df) > 0:
    st.dataframe(
        ledger_df,
        column_config={
            "INVOICE_ID": None,  # Hide ID column
            "INVOICE_NUMBER": "Invoice #",
            "VENDOR_NAME": "Vendor",
            "INVOICE_DATE": st.column_config.DateColumn("Invoice Date"),
            "DUE_DATE": st.column_config.DateColumn("Due Date"),
            "PAYMENT_TERMS": "Terms",
            "SUBTOTAL": st.column_config.NumberColumn("Subtotal", format="$%.2f"),
            "TAX_AMOUNT": st.column_config.NumberColumn("Tax", format="$%.2f"),
            "TOTAL_AMOUNT": st.column_config.NumberColumn("Total", format="$%.2f"),
            "STATUS": "Status",
            "AGING_BUCKET": "Aging",
            "DAYS_PAST_DUE": "Days Overdue",
            "OUTSTANDING_AMOUNT": st.column_config.NumberColumn("Outstanding", format="$%.2f"),
            "PAYMENT_DATE": st.column_config.DateColumn("Paid On"),
        },
        hide_index=True,
        use_container_width=True,
    )

    # --- Drill-down: select an invoice to see line items + source PDF ---
    st.divider()
    st.subheader("Invoice Detail")

    invoice_options = ledger_df["INVOICE_NUMBER"].dropna().tolist()
    if invoice_options:
        selected_invoice = st.selectbox(
            "Select an invoice to view details", invoice_options
        )

        if selected_invoice:
            # Get all extracted fields for this invoice
            file_row = session.sql(
                f"""
                SELECT file_name, vendor_name, invoice_number, po_number,
                       invoice_date, due_date, payment_terms, bill_to,
                       subtotal, tax_amount, total_amount, status
                FROM {DB}.EXTRACTED_INVOICES
                WHERE invoice_number = ?
                LIMIT 1
                """,
                params=[selected_invoice],
            ).to_pandas()

            col_pdf, col_lines = st.columns([1, 1])

            # Left column: source PDF
            with col_pdf:
                st.markdown("**Source PDF**")
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
                        st.warning(f"Could not render PDF: {e}")
                else:
                    st.info("No source PDF available for this invoice.")

            # Right column: extracted header fields + line items
            with col_lines:
                st.markdown("**Extracted Header Fields**")
                if len(file_row) > 0:
                    inv = file_row.iloc[0]
                    h1, h2 = st.columns(2)
                    with h1:
                        st.markdown(f"**Vendor:** {inv['VENDOR_NAME']}")
                        st.markdown(f"**Invoice #:** {inv['INVOICE_NUMBER']}")
                        st.markdown(f"**PO #:** {inv['PO_NUMBER'] or 'N/A'}")
                        st.markdown(f"**Bill To:** {inv['BILL_TO'] or 'N/A'}")
                    with h2:
                        st.markdown(f"**Invoice Date:** {inv['INVOICE_DATE']}")
                        st.markdown(f"**Due Date:** {inv['DUE_DATE']}")
                        st.markdown(f"**Terms:** {inv['PAYMENT_TERMS']}")
                        st.markdown(f"**Status:** {inv['STATUS']}")
                    t1, t2, t3 = st.columns(3)
                    t1.metric("Subtotal", f"${inv['SUBTOTAL']:,.2f}")
                    t2.metric("Tax", f"${inv['TAX_AMOUNT']:,.2f}")
                    t3.metric("Total", f"${inv['TOTAL_AMOUNT']:,.2f}")

                st.divider()
                st.markdown("**Extracted Line Items**")
                line_items = session.sql(
                    f"""
                    SELECT
                        line_number,
                        product_name,
                        category,
                        quantity,
                        unit_price,
                        line_total
                    FROM {DB}.EXTRACTED_LINE_ITEMS
                    WHERE invoice_number = ?
                    ORDER BY line_number
                    """,
                    params=[selected_invoice],
                ).to_pandas()

                if len(line_items) > 0:
                    st.dataframe(
                        line_items,
                        column_config={
                            "LINE_NUMBER": "#",
                            "PRODUCT_NAME": "Product",
                            "CATEGORY": "Category",
                            "QUANTITY": st.column_config.NumberColumn("Qty", format="%.0f"),
                            "UNIT_PRICE": st.column_config.NumberColumn("Unit Price", format="$%.2f"),
                            "LINE_TOTAL": st.column_config.NumberColumn("Total", format="$%.2f"),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.info("No line items found for this invoice.")
else:
    st.info("No invoices match the selected filters.")
