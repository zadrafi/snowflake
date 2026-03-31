"""
Page 3: Process New Invoices — Live demo page.

During the live demo, the presenter:
  1. Configures and generates new PDF invoices inside Snowflake
  2. Clicks "Run Extraction" to execute AI_EXTRACT on the new PDFs
  3. Watches the progress panel update as files are extracted
  4. Sees the newly extracted invoices appear inline
"""

import streamlit as st
import time
import tempfile
import os
import pypdfium2 as pdfium
from config import DB, STAGE

st.set_page_config(page_title="Process New Invoices", page_icon="🔄", layout="wide")

session = st.connection("snowflake").session()

st.title("Process New Invoices")
st.caption("Live demo: Generate new PDF invoices in Snowflake, run AI_EXTRACT, and watch the results appear")

# --- Reset Demo (so it's repeatable) ---
with st.expander("Reset Demo Data", expanded=False):
    st.markdown("Remove all generated/demo invoices so the demo can be run again cleanly.")
    if st.button("Reset Demo Invoices", key="reset_btn"):
        with st.status("Resetting demo invoices...", expanded=True) as status:
            st.write("Removing generated/demo line items...")
            session.sql(f"""
                DELETE FROM {DB}.EXTRACTED_LINE_ITEMS
                WHERE invoice_number IN (
                    SELECT invoice_number FROM {DB}.EXTRACTED_INVOICES
                    WHERE file_name LIKE 'demo_%' OR file_name LIKE 'gen_%'
                )
            """).collect()

            st.write("Removing generated/demo extracted invoices...")
            session.sql(f"""
                DELETE FROM {DB}.EXTRACTED_INVOICES
                WHERE file_name LIKE 'demo_%' OR file_name LIKE 'gen_%'
            """).collect()

            st.write("Removing generated/demo raw invoice records...")
            session.sql(f"""
                DELETE FROM {DB}.RAW_INVOICES
                WHERE file_name LIKE 'demo_%' OR file_name LIKE 'gen_%'
            """).collect()

            status.update(label="Demo data reset — ready to generate new invoices", state="complete")
        st.rerun()

st.divider()

# --- Step 1: Generate New Invoices ---
st.subheader("Step 1: Generate New Invoices")
st.markdown(
    "Configure and generate **brand new PDF invoices** directly inside Snowflake "
    "using a Python UDTF, then stage them for AI extraction."
)

# Load vendors for the dropdown
vendors_df = session.sql(
    f"SELECT vendor_name FROM {DB}.VENDORS ORDER BY vendor_name"
).to_pandas()
vendor_list = vendors_df["VENDOR_NAME"].tolist()

CATEGORIES = [
    "Beverages", "Snacks", "Candy & Gum", "Tobacco",
    "Dairy & Refrigerated", "Frozen", "General Merchandise",
]

with st.form("generate_form"):
    col_left, col_right = st.columns(2)

    with col_left:
        vendor = st.selectbox("Vendor", vendor_list, index=0)
        num_invoices = st.slider("Number of invoices", 1, 10, 3)
        approx_total = st.slider("Approximate total per invoice ($)", 50, 5000, 500, step=50)

    with col_right:
        categories = st.multiselect(
            "Product categories",
            CATEGORIES,
            default=["Beverages", "Snacks"],
        )
        num_items = st.slider("Line items per invoice", 3, 15, 8)

    submitted = st.form_submit_button("Generate & Stage Invoices", type="primary")

if submitted:
    if not categories:
        st.error("Select at least one product category.")
    else:
        cat_str = ",".join(categories)
        with st.status(
            f"Generating {num_invoices} invoice(s) from {vendor}...", expanded=True
        ) as status:
            st.write(f"Vendor: **{vendor}**")
            st.write(f"Categories: {cat_str}")
            st.write(f"~{num_items} line items, ~${approx_total:,} per invoice")
            st.write("Generating PDFs and staging to @INVOICE_STAGE...")

            result_df = session.sql(f"""
                CALL {DB}.SP_GENERATE_DEMO_INVOICES(
                    ?, ?, ?, ?::FLOAT, ?
                )
            """, params=[vendor, num_items, cat_str, approx_total, num_invoices]).collect()

            result_msg = result_df[0][0] if len(result_df) > 0 else "Done"
            st.write(f"Result: **{result_msg}**")
            status.update(label=result_msg, state="complete")

st.divider()

# --- Step 2: Run Extraction ---
st.subheader("Step 2: Run Extraction")
st.markdown(
    """
    Click below to trigger the `EXTRACT_NEW_INVOICES_TASK` on-demand.
    This calls the `SP_EXTRACT_NEW_INVOICES` stored procedure, which runs
    `AI_EXTRACT` on all unprocessed PDF files.
    """
)

if st.button("Run Extraction", type="primary", key="extract_btn"):
    with st.status("Running AI_EXTRACT on new invoices...", expanded=True) as status:
        st.write("Executing extraction task...")

        result_df = session.sql(
            f"CALL {DB}.SP_EXTRACT_NEW_INVOICES()"
        ).collect()

        result_msg = result_df[0][0] if len(result_df) > 0 else "Done"
        st.write(f"Result: **{result_msg}**")

        status.update(label=f"Extraction complete: {result_msg}", state="complete")

st.divider()

# --- Step 3: Live Progress Monitor ---
st.subheader("Step 3: Extraction Status")

# Current pipeline status
pipeline_df = session.sql(f"""
    SELECT
        total_files,
        extracted_files,
        pending_files,
        failed_files,
        last_extraction
    FROM {DB}.V_EXTRACTION_STATUS
""").to_pandas()

if len(pipeline_df) > 0:
    ps = pipeline_df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Files", int(ps["TOTAL_FILES"]))
    with col2:
        st.metric("Extracted", int(ps["EXTRACTED_FILES"]))
    with col3:
        st.metric("Pending", int(ps["PENDING_FILES"]))
    with col4:
        st.metric("Failed", int(ps["FAILED_FILES"]))

    if ps["LAST_EXTRACTION"]:
        st.caption(f"Last extraction: {ps['LAST_EXTRACTION']}")

    # Progress bar
    total = int(ps["TOTAL_FILES"])
    extracted = int(ps["EXTRACTED_FILES"])
    if total > 0:
        progress = extracted / total
        st.progress(progress, text=f"{extracted}/{total} files processed ({progress:.0%})")
else:
    st.info("No files registered yet.")

st.divider()

# --- Step 4: Preview newly extracted invoices ---
st.subheader("Step 4: Recently Extracted Invoices")

recent_df = session.sql(f"""
    SELECT
        ei.invoice_number,
        ei.vendor_name,
        ei.invoice_date,
        ei.due_date,
        ei.total_amount,
        ei.payment_terms,
        ei.status,
        ri.staged_at,
        ri.extracted_at
    FROM {DB}.EXTRACTED_INVOICES ei
        JOIN {DB}.RAW_INVOICES ri ON ei.file_name = ri.file_name
    ORDER BY ri.extracted_at DESC NULLS LAST
    LIMIT 10
""").to_pandas()

if len(recent_df) > 0:
    st.dataframe(
        recent_df,
        column_config={
            "INVOICE_NUMBER": "Invoice #",
            "VENDOR_NAME": "Vendor",
            "INVOICE_DATE": st.column_config.DateColumn("Invoice Date"),
            "DUE_DATE": st.column_config.DateColumn("Due Date"),
            "TOTAL_AMOUNT": st.column_config.NumberColumn("Total", format="$%.2f"),
            "PAYMENT_TERMS": "Terms",
            "STATUS": "Status",
            "STAGED_AT": "Staged",
            "EXTRACTED_AT": "Extracted",
        },
        hide_index=True,
        use_container_width=True,
    )

    # Line items + PDF for selected invoice
    invoice_options = recent_df["INVOICE_NUMBER"].dropna().tolist()
    if invoice_options:
        st.divider()
        selected_invoice = st.selectbox(
            "Select an invoice to view details", invoice_options, key="process_invoice_select"
        )
        st.markdown(f"#### Invoice Detail: {selected_invoice}")

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
            lines_df = session.sql(f"""
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
            """, params=[selected_invoice]).to_pandas()
            if len(lines_df) > 0:
                st.dataframe(
                    lines_df,
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
                st.info("No line items extracted for this invoice.")
else:
    st.info("No extracted invoices yet. Run Steps 1 and 2 above.")

# --- Auto-refresh button ---
st.divider()
if st.button("Refresh Status"):
    st.rerun()
