"""
Page 4: AI_EXTRACT Lab — Interactive playground for experimenting with
Snowflake Cortex AI_EXTRACT on staged or uploaded documents.
"""

import json
import os
import re
import tempfile

import pandas as pd
import pypdfium2 as pdfium
import streamlit as st
from config import STAGE

st.set_page_config(page_title="AI_EXTRACT Lab", page_icon="🧪", layout="wide")

session = st.connection("snowflake").session()

st.title("AI_EXTRACT Lab")
st.caption(
    "Experiment with Snowflake Cortex AI_EXTRACT — pick a document, craft a prompt, and see results in real time"
)

# ── AI_EXTRACT Reference Card ────────────────────────────────────────────────

with st.expander("AI_EXTRACT Quick Reference", expanded=False):
    ref1, ref2 = st.columns(2)
    with ref1:
        st.markdown(
            """
**Supported file formats**
PDF, PNG, JPEG, DOCX, DOC, PPTX, PPT, HTML, TXT, TIF, TIFF,
BMP, GIF, WEBP, EML, MD

**Extraction modes**

| Mode | responseFormat | Output |
|---|---|---|
| Entity | `{'field': 'question'}` | One value per field |
| List / Array | JSON schema with `"type": "array"` | Array of values |
| Table | JSON schema with `"type": "object"` + `column_ordering` | Columnar arrays |

**Response format options**
- Simple dict: `{'name': 'What is the name?'}`
- Array of strings: `['What is the name?', 'What is the date?']`
- Array of pairs: `[['name', 'What is the name?'], ['date', 'What is the date?']]`
- JSON schema: `{'schema': {'type': 'object', 'properties': {...}}}`
"""
        )
    with ref2:
        st.markdown(
            """
**Limits**
- Max **125 pages** per document, **100 MB** file size
- Up to **100 entity** questions per call
- Up to **10 table** questions per call (1 table = 10 entity questions)
- Entity output: 512 tokens max per question
- Table output: 4,096 tokens max

**Cost**
- Each page = 970 tokens (PDF, DOCX, TIF)
- Each image file = 970 tokens
- Plus input prompt tokens + output tokens
- Recommended warehouse: **MEDIUM or smaller**

**Best practices**
- Use plain English questions
- Be specific (e.g., "invoice date" not just "date")
- Ask for one value per question
- For tables, define columns in document order (left→right)
- Use `description` field to help locate the right table
- Use `column_ordering` to match document layout
"""
        )

st.divider()

# ── Prompt Builder ────────────────────────────────────────────────────────────

STARTER_TEMPLATES = {
    "Invoice Header Fields": json.dumps(
        {
            "vendor_name": "What is the vendor or company name on this invoice?",
            "invoice_number": "What is the invoice number?",
            "po_number": "What is the PO number or purchase order number?",
            "invoice_date": "What is the invoice date? Return in YYYY-MM-DD format.",
            "due_date": "What is the due date or payment due date? Return in YYYY-MM-DD format.",
            "payment_terms": "What are the payment terms (e.g., Net 15, Net 30)?",
            "bill_to": "Who is the invoice billed to? Return the store name and address.",
            "subtotal": "What is the subtotal amount before tax? Return as a number only.",
            "tax_amount": "What is the tax amount? Return as a number only.",
            "total_amount": "What is the total amount due? Return as a number only.",
        },
        indent=2,
    ),
    "Line Items Table": json.dumps(
        {
            "schema": {
                "type": "object",
                "properties": {
                    "line_items": {
                        "description": "The table of line items on the invoice",
                        "type": "object",
                        "column_ordering": [
                            "Line",
                            "Product",
                            "Category",
                            "Qty",
                            "Unit Price",
                            "Total",
                        ],
                        "properties": {
                            "Line": {
                                "description": "Line item number",
                                "type": "array",
                            },
                            "Product": {
                                "description": "Product name or description",
                                "type": "array",
                            },
                            "Category": {
                                "description": "Product category",
                                "type": "array",
                            },
                            "Qty": {
                                "description": "Quantity ordered",
                                "type": "array",
                            },
                            "Unit Price": {
                                "description": "Price per unit in dollars",
                                "type": "array",
                            },
                            "Total": {
                                "description": "Line total in dollars",
                                "type": "array",
                            },
                        },
                    }
                },
            }
        },
        indent=2,
    ),
    "General Document Q&A": json.dumps(
        {
            "title": "What is the title of this document?",
            "author": "Who is the author or sender?",
            "date": "What is the date of this document? Return in YYYY-MM-DD format.",
            "summary": "Provide a one-sentence summary of this document.",
        },
        indent=2,
    ),
}

st.subheader("Prompt Builder")

prompt_mode = st.radio(
    "Build your prompt",
    ["Starter Template", "Visual Builder", "Raw JSON Editor"],
    horizontal=True,
    key="prompt_mode",
)

prompt_json_str = ""

if prompt_mode == "Starter Template":
    template_name = st.selectbox(
        "Choose a template",
        list(STARTER_TEMPLATES.keys()),
        key="template_select",
    )
    # Use template name in the key so switching templates resets the editor
    prompt_json_str = st.text_area(
        "Edit the prompt (valid JSON)",
        value=STARTER_TEMPLATES[template_name],
        height=300,
        key=f"template_editor_{template_name}",
    )

elif prompt_mode == "Visual Builder":
    builder_type = st.radio(
        "Extraction type",
        ["Entity (key → question)", "Table (JSON schema)"],
        horizontal=True,
        key="builder_type",
    )

    if builder_type == "Entity (key → question)":
        if "vb_fields" not in st.session_state:
            st.session_state.vb_fields = [
                {"key": "vendor_name", "question": "What is the vendor name?"},
                {"key": "total", "question": "What is the total amount?"},
            ]

        for i, field in enumerate(st.session_state.vb_fields):
            fc1, fc2, fc3 = st.columns([2, 5, 1])
            with fc1:
                st.session_state.vb_fields[i]["key"] = st.text_input(
                    "Field name", value=field["key"], key=f"vb_key_{i}"
                )
            with fc2:
                st.session_state.vb_fields[i]["question"] = st.text_input(
                    "Question", value=field["question"], key=f"vb_q_{i}"
                )
            with fc3:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✕", key=f"vb_del_{i}") and len(st.session_state.vb_fields) > 1:
                    st.session_state.vb_fields.pop(i)
                    st.rerun()

        if st.button("Add field", key="vb_add"):
            st.session_state.vb_fields.append({"key": "", "question": ""})
            st.rerun()

        prompt_dict = {
            f["key"]: f["question"]
            for f in st.session_state.vb_fields
            if f["key"].strip() and f["question"].strip()
        }
        prompt_json_str = json.dumps(prompt_dict, indent=2)

    else:  # Table schema builder
        st.markdown("Define the table to extract:")
        table_desc = st.text_input(
            "Table description",
            value="The table of line items on the invoice",
            key="vb_table_desc",
        )
        table_name = st.text_input(
            "Table name (key in response)",
            value="line_items",
            key="vb_table_name",
        )

        if "vb_columns" not in st.session_state:
            st.session_state.vb_columns = [
                {"name": "Product", "description": "Product name"},
                {"name": "Quantity", "description": "Quantity ordered"},
                {"name": "Price", "description": "Unit price"},
            ]

        for i, col in enumerate(st.session_state.vb_columns):
            cc1, cc2, cc3 = st.columns([3, 5, 1])
            with cc1:
                st.session_state.vb_columns[i]["name"] = st.text_input(
                    "Column name", value=col["name"], key=f"vb_col_{i}"
                )
            with cc2:
                st.session_state.vb_columns[i]["description"] = st.text_input(
                    "Description", value=col["description"], key=f"vb_coldesc_{i}"
                )
            with cc3:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✕", key=f"vb_coldel_{i}") and len(st.session_state.vb_columns) > 1:
                    st.session_state.vb_columns.pop(i)
                    st.rerun()

        if st.button("Add column", key="vb_coladd"):
            st.session_state.vb_columns.append({"name": "", "description": ""})
            st.rerun()

        valid_cols = [c for c in st.session_state.vb_columns if c["name"].strip()]
        col_props = {}
        col_order = []
        for c in valid_cols:
            col_props[c["name"]] = {
                "description": c["description"] or c["name"],
                "type": "array",
            }
            col_order.append(c["name"])

        schema_dict = {
            "schema": {
                "type": "object",
                "properties": {
                    table_name: {
                        "description": table_desc,
                        "type": "object",
                        "column_ordering": col_order,
                        "properties": col_props,
                    }
                },
            }
        }
        prompt_json_str = json.dumps(schema_dict, indent=2)

else:  # Raw JSON Editor
    prompt_json_str = st.text_area(
        "Write your responseFormat JSON (dict, array, or schema)",
        value='{\n  "title": "What is the title?",\n  "date": "What is the date?"\n}',
        height=300,
        key="raw_editor",
    )

# Validate the prompt JSON
prompt_valid = False
prompt_parsed = None
try:
    prompt_parsed = json.loads(prompt_json_str)
    prompt_valid = True
except json.JSONDecodeError as e:
    st.error(f"Invalid JSON: {e}")

if prompt_valid:
    with st.expander("Preview responseFormat", expanded=False):
        st.json(prompt_parsed)

st.divider()

# ── Helper: render PDF from stage ────────────────────────────────────────────


def render_pdf_from_stage(file_name: str, stage: str = f"@{STAGE}"):
    """Download a file from stage and render PDF pages as images."""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            stage_path = f"{stage}/{file_name}"
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


def render_pdf_from_bytes(file_bytes: bytes):
    """Render PDF pages from raw bytes."""
    try:
        pdf = pdfium.PdfDocument(file_bytes)
        for page_idx in range(len(pdf)):
            page = pdf[page_idx]
            bitmap = page.render(scale=2)
            pil_image = bitmap.to_pil()
            st.image(pil_image, use_container_width=True)
        pdf.close()
    except Exception as e:
        st.warning(f"Could not render file: {e}")


# ── Helper: run AI_EXTRACT and display results ───────────────────────────────


def _safe_file_ref(stage: str, path: str) -> str:
    """Build a TO_FILE() SQL expression with basic safety check."""
    # Reject any path containing single quotes (shouldn't happen with sanitization)
    if "'" in path:
        raise ValueError(f"Unsafe character in file path: {path}")
    return f"TO_FILE('{stage}', '{path}')"


def run_extraction(file_ref_sql: str, response_format: dict):
    """Execute AI_EXTRACT and display the results.

    ``file_ref_sql`` must be a SQL expression that evaluates to a FILE value,
    e.g. ``TO_FILE('@STAGE', 'file.pdf')``.
    """
    # Build the SQL — we embed responseFormat as a PARSE_JSON literal so
    # arbitrary user JSON is safely injected as a string parameter.
    sql = f"""
        SELECT AI_EXTRACT(
            file => {file_ref_sql},
            responseFormat => PARSE_JSON(?)
        ) AS extraction
    """
    response_json_str = json.dumps(response_format)

    with st.status("Running AI_EXTRACT ...", expanded=True) as status:
        st.code(
            f"AI_EXTRACT(\n  file => {file_ref_sql},\n  responseFormat => {json.dumps(response_format, indent=2)}\n)",
            language="sql",
        )
        try:
            result = session.sql(sql, params=[response_json_str]).to_pandas()
            status.update(label="Extraction complete", state="complete")
        except Exception as e:
            status.update(label="Extraction failed", state="error")
            st.error(f"AI_EXTRACT error: {e}")
            return

    if len(result) == 0:
        st.warning("No result returned.")
        return

    raw = result.iloc[0]["EXTRACTION"]
    # raw may be a string or variant — parse if needed
    if isinstance(raw, str):
        parsed = json.loads(raw)
    else:
        parsed = raw

    # Show raw JSON
    st.subheader("Raw Response")
    st.json(parsed)

    # Check for extraction errors
    if parsed.get("error"):
        st.error(f"AI_EXTRACT returned an error: {parsed['error']}")
        return

    # Try to display a friendlier view
    response = parsed.get("response", parsed)
    if not isinstance(response, dict):
        return

    st.subheader("Parsed Results")

    for key, value in response.items():
        if isinstance(value, dict):
            # Table extraction — dict of column arrays
            st.markdown(f"**{key}**")
            try:
                df = pd.DataFrame(value)
                st.dataframe(df, hide_index=True, use_container_width=True)
            except Exception:
                st.json(value)
        elif isinstance(value, list):
            st.markdown(f"**{key}:** {', '.join(str(v) for v in value)}")
        else:
            st.metric(label=key, value=str(value) if value is not None else "None")


# ── Section 1: Browse & Extract from Staged Invoices ─────────────────────────

st.header("Browse Staged Invoices")

files_df = session.sql(f"""
    SELECT RELATIVE_PATH AS file_name
    FROM DIRECTORY(@{STAGE})
    WHERE RELATIVE_PATH LIKE '%.pdf'
      AND RELATIVE_PATH NOT LIKE 'playground/%'
    ORDER BY RELATIVE_PATH
""").to_pandas()

if len(files_df) > 0:
    selected_file = st.selectbox(
        "Select a staged PDF",
        files_df["FILE_NAME"].tolist(),
        key="staged_file_select",
    )

    col_preview, col_extract = st.columns([1, 1])

    with col_preview:
        st.markdown("**Document Preview**")
        if selected_file:
            render_pdf_from_stage(selected_file)

    with col_extract:
        st.markdown("**Extraction Results**")
        if selected_file and prompt_valid:
            file_ref = _safe_file_ref(f"@{STAGE}", selected_file)
            if st.button("Run AI_EXTRACT", type="primary", key="run_staged"):
                run_extraction(file_ref, prompt_parsed)
        elif not prompt_valid:
            st.warning("Fix the prompt JSON above before running extraction.")
else:
    st.info("No PDF files found on @INVOICE_STAGE.")

st.divider()

# ── Section 2: Upload & Extract ──────────────────────────────────────────────

st.header("Upload & Extract")
st.markdown(
    "Upload your own document and run AI_EXTRACT with the prompt you built above."
)

uploaded_file = st.file_uploader(
    "Upload a document",
    type=["pdf", "png", "jpg", "jpeg", "docx", "pptx", "html", "txt", "tif", "tiff"],
    key="upload_file",
)

if uploaded_file is not None:
    file_bytes = uploaded_file.read()
    # Sanitize filename — keep only safe characters
    upload_name = re.sub(r"[^a-zA-Z0-9._-]", "_", uploaded_file.name)

    col_up_preview, col_up_extract = st.columns([1, 1])

    with col_up_preview:
        st.markdown("**Document Preview**")
        ext = os.path.splitext(upload_name)[1].lower()
        if ext == ".pdf":
            render_pdf_from_bytes(file_bytes)
        elif ext in (".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".gif", ".webp"):
            st.image(file_bytes, use_container_width=True)
        else:
            st.info(f"Preview not available for {ext} files. Extraction will still work.")

    with col_up_extract:
        st.markdown("**Extraction Results**")
        if prompt_valid:
            if st.button("Upload & Run AI_EXTRACT", type="primary", key="run_upload"):
                # Stage the file to a playground subfolder
                playground_path = f"playground/{upload_name}"
                stage_target = f"@{STAGE}/playground"

                with st.spinner("Uploading to stage..."):
                    with tempfile.TemporaryDirectory() as tmpdir:
                        local_path = os.path.join(tmpdir, upload_name)
                        with open(local_path, "wb") as f:
                            f.write(file_bytes)
                        session.file.put(
                            local_path,
                            stage_target,
                            auto_compress=False,
                            overwrite=True,
                        )

                file_ref = _safe_file_ref(f"@{STAGE}", playground_path)
                run_extraction(file_ref, prompt_parsed)

                # Store the path so we can offer cleanup after rerun
                st.session_state["last_playground_file"] = playground_path
        else:
            st.warning("Fix the prompt JSON above before running extraction.")

# Cleanup previously uploaded playground files
if st.session_state.get("last_playground_file"):
    _path = st.session_state["last_playground_file"]
    if st.button(f"Remove uploaded file from stage ({_path})", key="cleanup_upload"):
        try:
            session.sql(
                f"REMOVE @{STAGE}/{_path}"
            ).collect()
            st.success(f"Removed {_path} from stage.")
            del st.session_state["last_playground_file"]
        except Exception as e:
            st.warning(f"Could not remove file: {e}")
