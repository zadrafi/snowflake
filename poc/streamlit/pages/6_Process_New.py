"""
Page 6: Process New Documents — Upload, register, and extract documents.
"""

import streamlit as st
from config import (
    DB,
    STAGE,
    get_session,
    get_doc_types,
    inject_custom_css,
    sidebar_branding,
    render_nav_bar,
)

st.set_page_config(page_title="Process New Documents", page_icon="📤", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

st.title("Process New Documents")
st.caption("Upload documents, register them, and trigger AI_EXTRACT extraction")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: UPLOAD
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Step 1: Upload Documents")

uploaded_files = st.file_uploader(
    "Upload PDF documents",
    type=["pdf"],
    accept_multiple_files=True,
)

doc_types = get_doc_types(session)
selected_type = st.selectbox(
    "Document Type",
    doc_types,
    index=doc_types.index("INVOICE") if "INVOICE" in doc_types else 0,
)

# Show file summary before staging
if uploaded_files:
    total_size = sum(f.size for f in uploaded_files)
    size_label = (
        f"{total_size / 1024:.0f} KB"
        if total_size < 1_048_576
        else f"{total_size / 1_048_576:.1f} MB"
    )
    st.caption(f"{len(uploaded_files)} file(s) selected — {size_label} total")

if uploaded_files and st.button("Stage & Register", type="primary"):
    with st.status(
        f"Processing {len(uploaded_files)} file(s)...", expanded=True
    ) as status:
        staged = 0
        for uf in uploaded_files:
            st.write(f"Staging **{uf.name}**...")
            try:
                session.file.put_stream(
                    uf,
                    f"@{STAGE}/{uf.name}",
                    auto_compress=False,
                    overwrite=True,
                )
                # Parameterized MERGE — avoids SQL injection via file names
                session.sql(
                    f"""
                    MERGE INTO {DB}.RAW_DOCUMENTS AS tgt
                    USING (SELECT ? AS file_name) AS src
                    ON tgt.file_name = src.file_name
                    WHEN NOT MATCHED THEN INSERT (file_name, file_path, doc_type)
                    VALUES (?, ?, ?)
                    """,
                    params=[uf.name, uf.name, f"@{STAGE}/{uf.name}", selected_type],
                ).collect()
                st.write(f"✓ **{uf.name}** registered as {selected_type}")
                staged += 1
            except Exception as e:
                st.warning(f"⚠ **{uf.name}** failed: {e}")
        status.update(
            label=f"Staged {staged}/{len(uploaded_files)} file(s)", state="complete"
        )

    if staged == len(uploaded_files):
        st.success(f"Uploaded and registered {staged} document(s).")
    else:
        st.warning(f"Staged {staged} of {len(uploaded_files)} — check warnings above.")


st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: EXTRACT
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Step 2: Run Extraction")

col1, col2 = st.columns(2)
with col1:
    extract_type = st.selectbox(
        "Extract doc type", ["ALL"] + doc_types, key="extract_type"
    )
with col2:
    extract_method = st.radio(
        "Method",
        ["SP (with confidence)", "Batch SQL (fast)"],
        horizontal=True,
        help="SP method runs the stored procedure with per-field confidence scoring. "
        "Batch SQL is faster but skips confidence.",
    )

if st.button("Run Extraction", type="primary"):
    with st.status("Extracting...", expanded=True) as status:
        if extract_method.startswith("SP"):
            types_to_run = doc_types if extract_type == "ALL" else [extract_type]
            for dt in types_to_run:
                st.write(f"Extracting **{dt}**...")
                try:
                    result = session.sql(
                        f"CALL {DB}.SP_EXTRACT_BY_DOC_TYPE(?)",
                        params=[dt],
                    ).collect()
                    st.write(f"✓ {result[0][0]}")
                except Exception as e:
                    st.warning(f"⚠ {dt}: {e}")
        else:
            st.write("Running batch SQL extraction...")
            try:
                session.sql(
                    f"""
                    INSERT INTO {DB}.EXTRACTED_FIELDS (
                        file_name, field_1, field_2, field_3, field_4, field_5,
                        field_6, field_7, field_8, field_9, field_10, raw_extraction
                    )
                    SELECT
                        sub.RELATIVE_PATH,
                        ext:response:vendor_name::VARCHAR,
                        ext:response:document_number::VARCHAR,
                        ext:response:reference::VARCHAR,
                        TRY_TO_DATE(ext:response:document_date::VARCHAR),
                        TRY_TO_DATE(ext:response:due_date::VARCHAR),
                        ext:response:terms::VARCHAR,
                        ext:response:recipient::VARCHAR,
                        TRY_TO_NUMBER(REGEXP_REPLACE(ext:response:subtotal::VARCHAR, '[^0-9.]', ''), 12, 2),
                        TRY_TO_NUMBER(REGEXP_REPLACE(ext:response:tax::VARCHAR, '[^0-9.]', ''), 12, 2),
                        TRY_TO_NUMBER(REGEXP_REPLACE(ext:response:total::VARCHAR, '[^0-9.]', ''), 12, 2),
                        ext
                    FROM (
                        SELECT RELATIVE_PATH, AI_EXTRACT(
                            TO_FILE('@{STAGE}', RELATIVE_PATH),
                            {{'vendor_name':'vendor?','document_number':'invoice number?',
                              'reference':'PO number?','document_date':'date? YYYY-MM-DD',
                              'due_date':'due date? YYYY-MM-DD','terms':'payment terms?',
                              'recipient':'addressed to?','subtotal':'subtotal? number only',
                              'tax':'tax? number only','total':'total? number only'}}
                        ) AS ext
                        FROM DIRECTORY(@{STAGE})
                        WHERE RELATIVE_PATH LIKE '%.pdf'
                          AND RELATIVE_PATH NOT IN (SELECT file_name FROM {DB}.EXTRACTED_FIELDS)
                    ) sub
                """
                ).collect()
                st.write("✓ Batch extraction complete")
            except Exception as e:
                st.error(f"Batch extraction failed: {e}")
        status.update(label="Extraction complete", state="complete")
    st.balloons()


st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: STATUS
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Step 3: Pipeline Status")

try:
    status_df = session.sql(f"SELECT * FROM {DB}.V_EXTRACTION_STATUS").to_pandas()
    if len(status_df) > 0:
        s = status_df.iloc[0]
        total = int(s["TOTAL_FILES"])
        extracted = int(s["EXTRACTED_FILES"])
        pct = round(extracted / total * 100) if total > 0 else 0

        st.progress(pct / 100, text=f"{extracted}/{total} processed ({pct}%)")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Files", f"{total:,}")
        c2.metric("Extracted", f"{extracted:,}")
        c3.metric("Pending", f"{int(s['PENDING_FILES']):,}")
        c4.metric("Failed", f"{int(s['FAILED_FILES']):,}")
except Exception as e:
    st.error(f"Could not load status: {e}")


st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: RECENT
# ══════════════════════════════════════════════════════════════════════════════

st.subheader("Step 4: Recently Extracted")

try:
    recent = session.sql(
        f"""
        SELECT
            e.file_name,
            r.doc_type,
            e.field_1       AS sender,
            e.field_10      AS total,
            e.status,
            e.extracted_at
        FROM {DB}.EXTRACTED_FIELDS e
            JOIN {DB}.RAW_DOCUMENTS r ON r.file_name = e.file_name
        ORDER BY e.extracted_at DESC NULLS LAST
        LIMIT 10
    """
    ).to_pandas()

    if len(recent) > 0:
        st.dataframe(
            recent,
            column_config={
                "FILE_NAME": "File",
                "DOC_TYPE": "Type",
                "SENDER": "Sender",
                "TOTAL": st.column_config.NumberColumn("Total", format="$%.2f"),
                "STATUS": "Status",
                "EXTRACTED_AT": st.column_config.DatetimeColumn(
                    "Extracted", format="MMM D, h:mm a"
                ),
            },
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No extractions yet.")
except Exception as e:
    st.error(f"Could not load recent extractions: {e}")


render_nav_bar()
