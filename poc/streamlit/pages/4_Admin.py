"""
Page 4: Admin — Manage document type configurations.

Redesigned for mixed technical / non-technical users:
  • Starter templates for common doc types (Invoice, Utility Bill, Receipt, PO, …)
  • Step-by-step wizard: Basic Info → Fields → Table Columns → Review & Save
  • Inline editing of existing doc types (no delete-and-recreate)
  • Live preview of generated prompt + config as you build
  • Test extraction with a sample PDF before committing
"""

import json
import re
import streamlit as st
import pandas as pd
from config import (
    DB, STAGE, get_session, get_all_doc_type_configs, get_doc_type_config,
    get_field_names_from_labels, _parse_variant,
    inject_custom_css, sidebar_branding, render_nav_bar,
)

st.set_page_config(page_title="Admin: Document Types", page_icon="⚙️", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

st.title("Document Type Configuration")
st.caption("Set up and manage the document types your pipeline can extract")


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS & TEMPLATES
# ══════════════════════════════════════════════════════════════════════════════

FIELD_TYPE_OPTIONS = ["Text", "Number", "Date"]
TYPE_MAP = {"Text": "VARCHAR", "Number": "NUMBER", "Date": "DATE"}
REVERSE_TYPE_MAP = {"VARCHAR": "Text", "NUMBER": "Number", "DATE": "Date"}

# ── Starter templates ─────────────────────────────────────────────────────────
TEMPLATES = {
    "Blank": {
        "display_name": "",
        "fields": [],
        "table_columns": [],
        "description": "Start from scratch — define your own fields.",
    },
    "INVOICE": {
        "display_name": "Invoice",
        "description": "Standard AP invoice with line items.",
        "fields": [
            {"name": "vendor_name",     "label": "Vendor Name",     "type": "Text",   "correctable": True},
            {"name": "invoice_number",  "label": "Invoice Number",  "type": "Text",   "correctable": True},
            {"name": "po_number",       "label": "PO Number",       "type": "Text",   "correctable": True},
            {"name": "invoice_date",    "label": "Invoice Date",    "type": "Date",   "correctable": True},
            {"name": "due_date",        "label": "Due Date",        "type": "Date",   "correctable": True},
            {"name": "terms",           "label": "Payment Terms",   "type": "Text",   "correctable": True},
            {"name": "recipient",       "label": "Bill To",         "type": "Text",   "correctable": True},
            {"name": "subtotal",        "label": "Subtotal",        "type": "Number", "correctable": True},
            {"name": "tax",             "label": "Tax Amount",      "type": "Number", "correctable": True},
            {"name": "total",           "label": "Total Amount",    "type": "Number", "correctable": True},
        ],
        "table_columns": [
            {"name": "description",  "description": "Product or service description"},
            {"name": "category",     "description": "Item category or GL code"},
            {"name": "quantity",     "description": "Quantity ordered"},
            {"name": "unit_price",   "description": "Price per unit"},
            {"name": "line_total",   "description": "Line item total"},
        ],
    },
    "UTILITY_BILL": {
        "display_name": "Utility Bill",
        "description": "Electric, gas, water, or telecom bills.",
        "fields": [
            {"name": "utility_company",     "label": "Utility Company",       "type": "Text",   "correctable": True},
            {"name": "account_number",      "label": "Account Number",        "type": "Text",   "correctable": True},
            {"name": "meter_number",        "label": "Meter Number",          "type": "Text",   "correctable": True},
            {"name": "service_address",     "label": "Service Address",       "type": "Text",   "correctable": True},
            {"name": "billing_period_start","label": "Billing Period Start",  "type": "Date",   "correctable": True},
            {"name": "billing_period_end",  "label": "Billing Period End",    "type": "Date",   "correctable": True},
            {"name": "due_date",            "label": "Due Date",              "type": "Date",   "correctable": True},
            {"name": "kwh_usage",           "label": "kWh Usage",             "type": "Number", "correctable": True},
            {"name": "current_charges",     "label": "Current Charges",       "type": "Number", "correctable": True},
            {"name": "previous_balance",    "label": "Previous Balance",      "type": "Number", "correctable": True},
            {"name": "total_due",           "label": "Total Amount Due",      "type": "Number", "correctable": True},
        ],
        "table_columns": [
            {"name": "charge_description", "description": "Description of the charge"},
            {"name": "amount",             "description": "Charge amount"},
        ],
    },
    "RECEIPT": {
        "display_name": "Receipt",
        "description": "Point-of-sale receipts and purchase confirmations.",
        "fields": [
            {"name": "store_name",       "label": "Store / Merchant",   "type": "Text",   "correctable": True},
            {"name": "store_address",    "label": "Store Address",      "type": "Text",   "correctable": True},
            {"name": "receipt_number",   "label": "Receipt Number",     "type": "Text",   "correctable": True},
            {"name": "transaction_date", "label": "Transaction Date",   "type": "Date",   "correctable": True},
            {"name": "payment_method",   "label": "Payment Method",     "type": "Text",   "correctable": True},
            {"name": "subtotal",         "label": "Subtotal",           "type": "Number", "correctable": True},
            {"name": "tax",              "label": "Tax",                "type": "Number", "correctable": True},
            {"name": "total",            "label": "Total",              "type": "Number", "correctable": True},
        ],
        "table_columns": [
            {"name": "item_name",   "description": "Item purchased"},
            {"name": "quantity",    "description": "Quantity"},
            {"name": "price",       "description": "Item price"},
        ],
    },
    "PURCHASE_ORDER": {
        "display_name": "Purchase Order",
        "description": "POs with vendor, shipping, and line items.",
        "fields": [
            {"name": "vendor_name",   "label": "Vendor Name",     "type": "Text",   "correctable": True},
            {"name": "po_number",     "label": "PO Number",       "type": "Text",   "correctable": True},
            {"name": "po_date",       "label": "PO Date",         "type": "Date",   "correctable": True},
            {"name": "delivery_date", "label": "Delivery Date",   "type": "Date",   "correctable": True},
            {"name": "ship_to",       "label": "Ship To",         "type": "Text",   "correctable": True},
            {"name": "buyer_name",    "label": "Buyer Name",      "type": "Text",   "correctable": True},
            {"name": "terms",         "label": "Payment Terms",   "type": "Text",   "correctable": True},
            {"name": "subtotal",      "label": "Subtotal",        "type": "Number", "correctable": True},
            {"name": "tax",           "label": "Tax",             "type": "Number", "correctable": True},
            {"name": "total",         "label": "Total",           "type": "Number", "correctable": True},
        ],
        "table_columns": [
            {"name": "item_number",  "description": "Item or SKU number"},
            {"name": "description",  "description": "Item description"},
            {"name": "quantity",     "description": "Quantity ordered"},
            {"name": "unit_price",   "description": "Unit price"},
            {"name": "line_total",   "description": "Line total"},
        ],
    },
    "STATEMENT": {
        "display_name": "Account Statement",
        "description": "Bank or vendor account statements.",
        "fields": [
            {"name": "company_name",    "label": "Company / Bank",     "type": "Text",   "correctable": True},
            {"name": "account_number",  "label": "Account Number",     "type": "Text",   "correctable": True},
            {"name": "statement_date",  "label": "Statement Date",     "type": "Date",   "correctable": True},
            {"name": "period_start",    "label": "Period Start",       "type": "Date",   "correctable": True},
            {"name": "period_end",      "label": "Period End",         "type": "Date",   "correctable": True},
            {"name": "opening_balance", "label": "Opening Balance",    "type": "Number", "correctable": True},
            {"name": "closing_balance", "label": "Closing Balance",    "type": "Number", "correctable": True},
            {"name": "total_debits",    "label": "Total Debits",       "type": "Number", "correctable": True},
            {"name": "total_credits",   "label": "Total Credits",      "type": "Number", "correctable": True},
        ],
        "table_columns": [
            {"name": "date",         "description": "Transaction date"},
            {"name": "description",  "description": "Transaction description"},
            {"name": "debit",        "description": "Debit amount"},
            {"name": "credit",       "description": "Credit amount"},
            {"name": "balance",      "description": "Running balance"},
        ],
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _build_config_from_fields(doc_type_code, display_name, fields, table_columns=None):
    """Build all config JSON from a list of field dicts."""
    field_names = [f["name"] for f in fields]
    prompt = (
        f"Extract the following fields from this {display_name.lower()}: "
        + ", ".join(field_names)
        + ". FORMATTING RULES: Return all dates in YYYY-MM-DD format. "
        "Return all monetary values as plain numbers without currency symbols or commas "
        "(e.g. 1234.56 not $1,234.56). Return numeric values without units. "
        "Return 0 for zero or missing amounts, not null. "
        "Return the full legal company or person name, not abbreviations."
    )

    field_labels = {}
    for i, f in enumerate(fields):
        field_labels[f"field_{i+1}"] = f["label"]

    # Meta-labels
    if fields:
        field_labels["sender_label"] = fields[0]["label"]
    for f in fields:
        if f["type"] == "Number":
            field_labels["amount_label"] = f["label"]
    for f in fields:
        if f["type"] == "Date":
            field_labels["date_label"] = f["label"]
    text_fields = [f for f in fields if f["type"] == "Text"]
    if len(text_fields) >= 2:
        field_labels["reference_label"] = text_fields[1]["label"]
    if len(text_fields) >= 3:
        field_labels["secondary_ref_label"] = text_fields[2]["label"]

    correctable = [f["name"] for f in fields if f.get("correctable", True)]
    types = {f["name"]: TYPE_MAP[f["type"]] for f in fields}
    review_fields = {"correctable": correctable, "types": types}

    table_schema = None
    if table_columns:
        table_schema = {
            "columns": [tc["name"] for tc in table_columns],
            "descriptions": [tc.get("description", tc["name"]) for tc in table_columns],
        }

    return prompt, field_labels, review_fields, table_schema


def _config_to_fields(cfg: dict) -> list[dict]:
    """Reverse-engineer field dicts from an existing config."""
    labels = cfg.get("field_labels") or {}
    review_fields = cfg.get("review_fields") or {}
    types = review_fields.get("types", {})
    correctable = review_fields.get("correctable", [])
    field_keys = get_field_names_from_labels(labels)

    fields = []
    for fk in field_keys:
        idx = int(fk.split("_")[1]) - 1
        label = labels.get(fk, "")
        if correctable and idx < len(correctable):
            ext_name = correctable[idx]
        else:
            ext_name = label.lower().replace(" ", "_")
        ftype = REVERSE_TYPE_MAP.get(types.get(ext_name, "VARCHAR"), "Text")
        fields.append({
            "name": ext_name,
            "label": label,
            "type": ftype,
            "correctable": ext_name in correctable,
        })
    return fields


def _config_to_table_columns(cfg: dict) -> list[dict]:
    """Reverse-engineer table column dicts from an existing config."""
    table_schema = cfg.get("table_extraction_schema")
    if not table_schema:
        return []
    cols = table_schema.get("columns", [])
    descs = table_schema.get("descriptions", [])
    return [
        {"name": cols[i], "description": descs[i] if i < len(descs) else cols[i]}
        for i in range(len(cols))
    ]


def _render_field_builder(fields_key: str, initial_fields: list[dict], max_fields: int = 15):
    """Render the interactive field builder. Returns list of field dicts."""
    if fields_key not in st.session_state:
        st.session_state[fields_key] = initial_fields.copy() if initial_fields else []

    fields = st.session_state[fields_key]

    # Header row
    hc1, hc2, hc3, hc4, hc5 = st.columns([3, 3, 2, 1, 1])
    hc1.markdown("**Field Name**")
    hc2.markdown("**Display Label**")
    hc3.markdown("**Type**")
    hc4.markdown("**Editable**")
    hc5.markdown("**Remove**")

    to_remove = None
    for i, f in enumerate(fields):
        c1, c2, c3, c4, c5 = st.columns([3, 3, 2, 1, 1])
        with c1:
            new_name = st.text_input(
                "name", value=f["name"], key=f"{fields_key}_n_{i}",
                label_visibility="collapsed",
            )
            fields[i]["name"] = new_name.strip().lower().replace(" ", "_") if new_name else ""
        with c2:
            new_label = st.text_input(
                "label", value=f["label"], key=f"{fields_key}_l_{i}",
                label_visibility="collapsed",
            )
            fields[i]["label"] = new_label.strip() if new_label else ""
        with c3:
            type_idx = FIELD_TYPE_OPTIONS.index(f["type"]) if f["type"] in FIELD_TYPE_OPTIONS else 0
            new_type = st.selectbox(
                "type", FIELD_TYPE_OPTIONS, index=type_idx,
                key=f"{fields_key}_t_{i}", label_visibility="collapsed",
            )
            fields[i]["type"] = new_type
        with c4:
            new_corr = st.checkbox(
                "corr", value=f.get("correctable", True),
                key=f"{fields_key}_c_{i}", label_visibility="collapsed",
            )
            fields[i]["correctable"] = new_corr
        with c5:
            if st.button("✕", key=f"{fields_key}_rm_{i}", type="secondary"):
                to_remove = i

    if to_remove is not None:
        fields.pop(to_remove)
        st.session_state[fields_key] = fields
        st.rerun()

    # Add field button
    if len(fields) < max_fields:
        if st.button("＋ Add Field", key=f"{fields_key}_add"):
            fields.append({"name": "", "label": "", "type": "Text", "correctable": True})
            st.session_state[fields_key] = fields
            st.rerun()

    # Filter out empty rows
    return [f for f in fields if f.get("name")]


def _render_table_column_builder(tc_key: str, initial_cols: list[dict], max_cols: int = 8):
    """Render the table column builder. Returns list of column dicts."""
    if tc_key not in st.session_state:
        st.session_state[tc_key] = initial_cols.copy() if initial_cols else []

    cols = st.session_state[tc_key]

    hc1, hc2, hc3 = st.columns([3, 5, 1])
    hc1.markdown("**Column Name**")
    hc2.markdown("**Description**")
    hc3.markdown("**Remove**")

    to_remove = None
    for i, tc in enumerate(cols):
        c1, c2, c3 = st.columns([3, 5, 1])
        with c1:
            new_name = st.text_input(
                "col", value=tc["name"], key=f"{tc_key}_n_{i}",
                label_visibility="collapsed",
            )
            cols[i]["name"] = new_name.strip() if new_name else ""
        with c2:
            new_desc = st.text_input(
                "desc", value=tc.get("description", ""), key=f"{tc_key}_d_{i}",
                label_visibility="collapsed",
            )
            cols[i]["description"] = new_desc.strip() if new_desc else ""
        with c3:
            if st.button("✕", key=f"{tc_key}_rm_{i}", type="secondary"):
                to_remove = i

    if to_remove is not None:
        cols.pop(to_remove)
        st.session_state[tc_key] = cols
        st.rerun()

    if len(cols) < max_cols:
        if st.button("＋ Add Column", key=f"{tc_key}_add"):
            cols.append({"name": "", "description": ""})
            st.session_state[tc_key] = cols
            st.rerun()

    return [c for c in cols if c.get("name")]


def _render_live_preview(display_name, fields, table_columns):
    """Show what the generated config will look like."""
    if not fields:
        st.info("Add at least one field to see a preview.")
        return

    prompt, field_labels, review_fields, table_schema = _build_config_from_fields(
        "PREVIEW", display_name or "Document", fields, table_columns or None,
    )

    st.markdown("**Generated Extraction Prompt:**")
    st.code(prompt, language="text")

    col_p1, col_p2 = st.columns(2)
    with col_p1:
        st.markdown("**Fields → AI_EXTRACT will return:**")
        preview_data = []
        for i, f in enumerate(fields):
            preview_data.append({
                "#": i + 1,
                "Field": f["name"],
                "Label": f["label"],
                "Type": f["type"],
                "Editable": "✓" if f.get("correctable") else "",
            })
        st.dataframe(pd.DataFrame(preview_data), hide_index=True, use_container_width=True)

    with col_p2:
        st.markdown("**Example extraction output:**")
        example = {}
        for f in fields:
            if f["type"] == "Date":
                example[f["name"]] = "2025-01-15"
            elif f["type"] == "Number":
                example[f["name"]] = 1234.56
            else:
                example[f["name"]] = f"Sample {f['label']}"
        st.json(example)

        if table_columns:
            st.markdown("**Table columns:**")
            st.dataframe(
                pd.DataFrame(table_columns),
                hide_index=True, use_container_width=True,
            )


def _save_config(doc_type_code, display_name, fields, table_columns, is_update=False):
    """Save or update a document type configuration."""
    prompt, field_labels, review_fields, table_schema = _build_config_from_fields(
        doc_type_code, display_name, fields, table_columns or None,
    )

    if is_update:
        session.sql(
            f"""
            UPDATE {DB}.DOCUMENT_TYPE_CONFIG SET
                display_name = ?,
                extraction_prompt = ?,
                field_labels = PARSE_JSON(?),
                table_extraction_schema = PARSE_JSON(?),
                review_fields = PARSE_JSON(?),
                updated_at = CURRENT_TIMESTAMP()
            WHERE doc_type = ?
            """,
            params=[
                display_name,
                prompt,
                json.dumps(field_labels),
                json.dumps(table_schema) if table_schema else None,
                json.dumps(review_fields),
                doc_type_code,
            ],
        ).collect()
        st.success(f"Updated **{doc_type_code}** configuration.")
    else:
        session.sql(
            f"""
            INSERT INTO {DB}.DOCUMENT_TYPE_CONFIG (
                doc_type, display_name, extraction_prompt,
                field_labels, table_extraction_schema, review_fields
            ) SELECT ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?), PARSE_JSON(?)
            """,
            params=[
                doc_type_code,
                display_name,
                prompt,
                json.dumps(field_labels),
                json.dumps(table_schema) if table_schema else None,
                json.dumps(review_fields),
            ],
        ).collect()
        st.success(f"Created **{doc_type_code}** document type.")

    return prompt, field_labels, review_fields, table_schema


def _run_test_extraction(doc_type_code, prompt, test_file):
    """Upload a test PDF and run AI_EXTRACT against it."""
    test_fname = f"_test_{doc_type_code}_{test_file.name}"
    try:
        session.file.put_stream(
            test_file, f"@{STAGE}/{test_fname}",
            auto_compress=False, overwrite=True,
        )

        prompt_for_parsing = re.split(r'\.\s*FORMATTING\s+RULES', prompt, maxsplit=1)[0]
        match = re.search(r':\s*(.+)$', prompt_for_parsing)
        if match:
            fnames = [f.strip().rstrip('.') for f in match.group(1).split(',')]
            prompt_parts = []
            for fn in fnames:
                lbl = fn.replace('_', ' ').title()
                prompt_parts.append(f"'{fn}': 'What is the {lbl.lower()}?'")
            prompt_obj = '{' + ', '.join(prompt_parts) + '}'

            result = session.sql(f"""
                SELECT AI_EXTRACT(
                    TO_FILE('@{STAGE}', '{test_fname}'),
                    {prompt_obj}
                ) AS extraction
            """).collect()

            if result:
                ext = result[0]['EXTRACTION']
                ext_json = json.loads(ext) if isinstance(ext, str) else ext
                return ext_json.get('response', ext_json), None
            else:
                return None, "No extraction result returned."
        else:
            return None, "Could not parse field names from prompt."
    except Exception as e:
        return None, str(e)
    finally:
        try:
            session.sql(f"REMOVE @{STAGE}/{test_fname}").collect()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: EXISTING CONFIGURATIONS
# ══════════════════════════════════════════════════════════════════════════════

configs = get_all_doc_type_configs(session)

if configs:
    st.subheader("Your Document Types")

    # Summary cards
    card_cols = st.columns(min(len(configs), 4))
    for i, cfg in enumerate(configs):
        col = card_cols[i % len(card_cols)]
        labels = cfg.get("field_labels") or {}
        field_count = len([k for k in labels if k.startswith("field_")])
        table_schema = cfg.get("table_extraction_schema")
        table_count = len(table_schema.get("columns", [])) if table_schema else 0
        status = "🟢" if cfg.get("active", True) else "🔴"

        with col:
            st.metric(
                label=f"{status} {cfg['display_name']}",
                value=f"{field_count} fields",
                delta=f"{table_count} table cols" if table_count else "No table",
            )

    st.divider()

    # ── Edit existing doc type ────────────────────────────────────────────────
    st.subheader("Edit Configuration")

    selected_type = st.selectbox(
        "Select document type",
        [c["doc_type"] for c in configs],
        format_func=lambda x: next((c["display_name"] for c in configs if c["doc_type"] == x), x),
    )

    if selected_type:
        cfg = get_doc_type_config(session, selected_type)
        if cfg:
            edit_tab, actions_tab, prompt_tab = st.tabs(["Fields & Settings", "Actions", "Raw Prompt"])

            with edit_tab:
                # Load existing config into editable form
                edit_display = st.text_input(
                    "Display Name",
                    value=cfg["display_name"],
                    key=f"edit_dn_{selected_type}",
                )

                st.markdown("**Entity Fields**")
                existing_fields = _config_to_fields(cfg)
                edit_fields = _render_field_builder(
                    f"edit_fields_{selected_type}",
                    existing_fields,
                )

                st.markdown("---")
                st.markdown("**Table / Line-Item Columns** (optional)")
                existing_tc = _config_to_table_columns(cfg)
                edit_tc = _render_table_column_builder(
                    f"edit_tc_{selected_type}",
                    existing_tc,
                )

                # Live preview
                st.markdown("---")
                with st.expander("Preview Generated Config", expanded=False):
                    _render_live_preview(edit_display, edit_fields, edit_tc)

                # Save / Active toggle
                st.markdown("---")
                save_col, active_col = st.columns([1, 1])
                with save_col:
                    if st.button("Save Changes", type="primary", key=f"save_{selected_type}"):
                        if not edit_fields:
                            st.error("At least one field is required.")
                        else:
                            _save_config(
                                selected_type, edit_display.strip(),
                                edit_fields, edit_tc, is_update=True,
                            )
                            # Clear cached field state
                            for k in list(st.session_state.keys()):
                                if k.startswith(f"edit_fields_{selected_type}") or k.startswith(f"edit_tc_{selected_type}"):
                                    del st.session_state[k]
                            st.rerun()

                with active_col:
                    is_active = cfg.get("active", True)
                    new_active = st.checkbox(
                        f"Active (currently {'enabled' if is_active else 'disabled'})",
                        value=is_active,
                        key=f"active_{selected_type}",
                    )
                    if new_active != is_active:
                        session.sql(
                            f"UPDATE {DB}.DOCUMENT_TYPE_CONFIG SET active = ?, updated_at = CURRENT_TIMESTAMP() WHERE doc_type = ?",
                            params=[new_active, selected_type],
                        ).collect()
                        st.success(f"{'Enabled' if new_active else 'Disabled'} **{selected_type}**")
                        st.rerun()

            with actions_tab:
                st.markdown("**Re-Extract All Documents**")
                st.caption(
                    "Clears all extracted data for this doc type and re-runs extraction "
                    "with the current prompt. Use after changing fields."
                )
                if st.button(f"Re-Extract {cfg['display_name']} Documents", key=f"reext_{selected_type}"):
                    with st.spinner(f"Re-extracting {selected_type} documents..."):
                        try:
                            result = session.sql(
                                f"CALL {DB}.SP_REEXTRACT_DOC_TYPE(?)",
                                params=[selected_type],
                            ).collect()
                            st.success(result[0][0] if result else "Done")
                        except Exception as e:
                            st.error(f"Re-extraction failed: {e}")

                st.divider()
                st.markdown("**Test Extraction**")
                st.caption("Upload a sample PDF to verify your config before re-extracting everything.")
                test_file = st.file_uploader(
                    "Upload test document",
                    type=["pdf"],
                    key=f"test_{selected_type}",
                )
                if test_file and st.button("Run Test", key=f"run_test_{selected_type}"):
                    with st.spinner("Running AI_EXTRACT on test document..."):
                        prompt = cfg.get("extraction_prompt", "")
                        if prompt:
                            result, err = _run_test_extraction(selected_type, prompt, test_file)
                            if err:
                                st.error(f"Test failed: {err}")
                            elif result:
                                st.success("Extraction successful!")
                                st.json(result)
                        else:
                            st.error("No extraction prompt configured.")

            with prompt_tab:
                st.markdown("**Current Extraction Prompt:**")
                st.code(cfg.get("extraction_prompt", ""), language="text")

                st.markdown("**Field Labels (JSON):**")
                st.json(cfg.get("field_labels") or {})

                st.markdown("**Review Fields (JSON):**")
                st.json(cfg.get("review_fields") or {})

                if cfg.get("table_extraction_schema"):
                    st.markdown("**Table Schema (JSON):**")
                    st.json(cfg["table_extraction_schema"])

                if cfg.get("validation_rules"):
                    st.markdown("**Validation Rules (JSON):**")
                    st.json(cfg["validation_rules"])

else:
    st.info("No document types configured yet. Create one below to get started.")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: AUTO-CLASSIFICATION REVIEW
# ══════════════════════════════════════════════════════════════════════════════

st.divider()

# Check if classification tables/columns exist
_has_classification = False
try:
    session.sql(f"SELECT classified_doc_type FROM {DB}.RAW_DOCUMENTS LIMIT 0").collect()
    _has_classification = True
except Exception:
    pass

if _has_classification:
    st.subheader("Auto-Classification Review")
    st.caption(
        "Document types discovered by the AI classifier. "
        "Approve to keep, edit fields to refine, or reject to remove."
    )

    # ── Classification stats ──────────────────────────────────────────────
    try:
        class_stats = session.sql(f"""
            SELECT
                COUNT(*) AS total_files,
                SUM(CASE WHEN classified_doc_type IS NOT NULL THEN 1 ELSE 0 END) AS classified,
                SUM(CASE WHEN classification_method = 'PENDING' OR classified_doc_type IS NULL THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN classification_method = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                ROUND(AVG(CASE WHEN classification_confidence IS NOT NULL THEN classification_confidence END), 2) AS avg_confidence
            FROM {DB}.RAW_DOCUMENTS
        """).to_pandas()

        if len(class_stats) > 0:
            cs = class_stats.iloc[0]
            cs1, cs2, cs3, cs4, cs5 = st.columns(5)
            cs1.metric("Total Files", f"{int(cs['TOTAL_FILES']):,}")
            cs2.metric("Classified", f"{int(cs['CLASSIFIED']):,}")
            cs3.metric("Pending", f"{int(cs['PENDING']):,}")
            cs4.metric("Failed", f"{int(cs['FAILED']):,}")
            cs5.metric("Avg Confidence", f"{cs['AVG_CONFIDENCE']:.0%}" if pd.notna(cs['AVG_CONFIDENCE']) else "—")
    except Exception:
        pass

    # ── New types needing review ──────────────────────────────────────────
    try:
        new_types = session.sql(f"""
            SELECT
                dtc.doc_type,
                dtc.display_name,
                dtc.generated_from_file,
                dtc.created_at,
                COUNT(DISTINCT rd.file_name) AS doc_count,
                ROUND(AVG(rd.classification_confidence), 2) AS avg_confidence
            FROM {DB}.DOCUMENT_TYPE_CONFIG dtc
            LEFT JOIN {DB}.RAW_DOCUMENTS rd ON rd.classified_doc_type = dtc.doc_type
            WHERE dtc.auto_generated = TRUE AND dtc.needs_review = TRUE
            GROUP BY dtc.doc_type, dtc.display_name, dtc.generated_from_file, dtc.created_at
            ORDER BY dtc.created_at DESC
        """).to_pandas()
    except Exception:
        new_types = pd.DataFrame()

    if len(new_types) > 0:
        st.warning(f"**{len(new_types)} auto-generated type(s) need review**")

        for _, nt_row in new_types.iterrows():
            dtype = nt_row["DOC_TYPE"]
            dname = nt_row["DISPLAY_NAME"]
            doc_count = int(nt_row["DOC_COUNT"])
            avg_conf = nt_row["AVG_CONFIDENCE"]
            from_file = nt_row["GENERATED_FROM_FILE"] or "—"

            with st.expander(
                f"🆕 {dtype} — \"{dname}\" ({doc_count} docs, {avg_conf:.0%} confidence)",
                expanded=True,
            ):
                rc1, rc2, rc3 = st.columns(3)
                rc1.metric("Documents", f"{doc_count}")
                rc2.metric("Avg Confidence", f"{avg_conf:.0%}" if pd.notna(avg_conf) else "—")
                rc3.caption(f"Discovered from: `{from_file}`")

                # Show current auto-generated config
                auto_cfg = get_doc_type_config(session, dtype)
                if auto_cfg:
                    st.markdown("**Auto-generated extraction prompt:**")
                    st.code(auto_cfg.get("extraction_prompt", ""), language="text")

                    # Show sample classified documents
                    try:
                        sample_docs = session.sql(f"""
                            SELECT file_name, classification_confidence, classified_at
                            FROM {DB}.RAW_DOCUMENTS
                            WHERE classified_doc_type = ?
                            ORDER BY classified_at DESC
                            LIMIT 5
                        """, params=[dtype]).to_pandas()
                        if len(sample_docs) > 0:
                            st.markdown("**Sample documents:**")
                            st.dataframe(
                                sample_docs,
                                column_config={
                                    "FILE_NAME": "File",
                                    "CLASSIFICATION_CONFIDENCE": st.column_config.NumberColumn("Confidence", format="%.0%%"),
                                    "CLASSIFIED_AT": st.column_config.DatetimeColumn("Classified", format="MMM D, h:mm a"),
                                },
                                hide_index=True, use_container_width=True,
                            )
                    except Exception:
                        pass

                # Action buttons
                act1, act2, act3 = st.columns(3)

                with act1:
                    if st.button("✅ Approve", key=f"approve_{dtype}", type="primary"):
                        session.sql(
                            f"UPDATE {DB}.DOCUMENT_TYPE_CONFIG "
                            f"SET needs_review = FALSE, updated_at = CURRENT_TIMESTAMP() "
                            f"WHERE doc_type = ?",
                            params=[dtype],
                        ).collect()
                        st.success(f"Approved **{dtype}**")
                        st.rerun()

                with act2:
                    if st.button("✏️ Edit Fields", key=f"edit_new_{dtype}"):
                        # Load this type into the edit section by switching to it
                        st.session_state["review_doc_type_edit"] = dtype
                        st.info(
                            f"Scroll up to **Edit Configuration** and select **{dtype}** "
                            "to customize fields, then save."
                        )

                with act3:
                    if st.button("🗑️ Reject & Delete", key=f"reject_{dtype}"):
                        # Deactivate (don't hard delete — preserve audit trail)
                        session.sql(
                            f"UPDATE {DB}.DOCUMENT_TYPE_CONFIG "
                            f"SET active = FALSE, needs_review = FALSE, "
                            f"updated_at = CURRENT_TIMESTAMP() "
                            f"WHERE doc_type = ?",
                            params=[dtype],
                        ).collect()
                        # Reset classified docs back to pending
                        session.sql(
                            f"UPDATE {DB}.RAW_DOCUMENTS "
                            f"SET classified_doc_type = NULL, classification_method = 'PENDING' "
                            f"WHERE classified_doc_type = ?",
                            params=[dtype],
                        ).collect()
                        st.warning(f"Rejected **{dtype}** — {doc_count} document(s) reset to pending")
                        st.rerun()
    else:
        st.success("No auto-generated types pending review.")

    # ── Classification history ────────────────────────────────────────────
    with st.expander("Classification Overview"):
        try:
            overview = session.sql(f"""
                SELECT
                    classified_doc_type AS doc_type,
                    COUNT(*) AS files,
                    ROUND(AVG(classification_confidence), 2) AS avg_confidence,
                    MIN(classified_at) AS first_classified,
                    MAX(classified_at) AS last_classified
                FROM {DB}.RAW_DOCUMENTS
                WHERE classified_doc_type IS NOT NULL
                GROUP BY classified_doc_type
                ORDER BY files DESC
            """).to_pandas()

            if len(overview) > 0:
                st.dataframe(
                    overview,
                    column_config={
                        "DOC_TYPE": "Document Type",
                        "FILES": "Files",
                        "AVG_CONFIDENCE": st.column_config.NumberColumn("Avg Confidence", format="%.0%%"),
                        "FIRST_CLASSIFIED": st.column_config.DatetimeColumn("First", format="MMM D"),
                        "LAST_CLASSIFIED": st.column_config.DatetimeColumn("Last", format="MMM D"),
                    },
                    hide_index=True, use_container_width=True,
                )
            else:
                st.info("No documents classified yet.")
        except Exception as e:
            st.error(f"Could not load classification overview: {e}")

    # ── Manual reclassification ───────────────────────────────────────────
    with st.expander("Run Classification"):
        st.caption("Classify unprocessed documents or reclassify a specific file.")
        rc1, rc2 = st.columns(2)
        with rc1:
            if st.button("Classify All Pending", key="classify_batch"):
                with st.spinner("Running batch classification..."):
                    try:
                        result = session.sql(f"CALL {DB}.SP_CLASSIFY_UNCLASSIFIED()").collect()
                        st.success(result[0][0] if result else "Done")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Classification failed: {e}")
        with rc2:
            reclass_file = st.text_input("Reclassify specific file", placeholder="invoice_001.pdf", key="reclass_file")
            if reclass_file and st.button("Reclassify", key="reclass_btn"):
                with st.spinner(f"Classifying {reclass_file}..."):
                    try:
                        result = session.sql(
                            f"CALL {DB}.SP_CLASSIFY_DOCUMENT(?)",
                            params=[reclass_file],
                        ).collect()
                        if result:
                            st.json(result[0][0] if hasattr(result[0][0], '__iter__') else str(result[0][0]))
                        st.rerun()
                    except Exception as e:
                        st.error(f"Classification failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: ADD NEW DOCUMENT TYPE
# ══════════════════════════════════════════════════════════════════════════════

st.divider()
st.subheader("Add New Document Type")

# ── Template picker ───────────────────────────────────────────────────────────
st.markdown("**Start from a template** — pick the closest match, then customize")

template_names = list(TEMPLATES.keys())
template_cols = st.columns(len(template_names))
for i, tname in enumerate(template_names):
    tmpl = TEMPLATES[tname]
    with template_cols[i]:
        field_count = len(tmpl.get("fields", []))
        btn_label = f"{tmpl.get('display_name') or 'Blank'}"
        if st.button(
            btn_label,
            key=f"tmpl_{tname}",
            use_container_width=True,
            type="primary" if st.session_state.get("selected_template") == tname else "secondary",
        ):
            st.session_state["selected_template"] = tname
            # Load template into session state
            st.session_state["new_doc_type"] = tname if tname != "Blank" else ""
            st.session_state["new_display_name"] = tmpl.get("display_name", "")
            # Reset field builders with template data
            st.session_state["new_fields"] = [f.copy() for f in tmpl.get("fields", [])]
            st.session_state["new_tc"] = [c.copy() for c in tmpl.get("table_columns", [])]
            st.rerun()
        st.caption(tmpl["description"][:50])

selected_template = st.session_state.get("selected_template")

if selected_template is not None:
    st.markdown("---")

    # ── Step 1: Basic Info ────────────────────────────────────────────────────
    step1, step2, step3, step4 = st.tabs([
        "① Basic Info", "② Fields", "③ Table Columns", "④ Review & Save"
    ])

    with step1:
        col_b1, col_b2 = st.columns(2)
        with col_b1:
            new_doc_type = st.text_input(
                "Document Type Code",
                value=st.session_state.get("new_doc_type", ""),
                placeholder="UTILITY_BILL",
                help="Uppercase with underscores (e.g. PURCHASE_ORDER). "
                     "This is the internal identifier — not shown to end users.",
                key="input_doc_type",
            )
        with col_b2:
            new_display_name = st.text_input(
                "Display Name",
                value=st.session_state.get("new_display_name", ""),
                placeholder="Utility Bill",
                help="Human-readable name shown in dropdowns and headers.",
                key="input_display_name",
            )

        # Sync back to session state
        st.session_state["new_doc_type"] = new_doc_type
        st.session_state["new_display_name"] = new_display_name

    # ── Step 2: Fields ────────────────────────────────────────────────────────
    with step2:
        st.markdown("Define the data fields that AI_EXTRACT should pull from this document type.")
        new_fields = _render_field_builder("new_fields", st.session_state.get("new_fields", []))

    # ── Step 3: Table Columns ─────────────────────────────────────────────────
    with step3:
        st.markdown(
            "If this document type has line items or tabular data, "
            "define the columns here. Leave empty if not applicable."
        )
        new_tc = _render_table_column_builder("new_tc", st.session_state.get("new_tc", []))

    # ── Step 4: Review & Save ─────────────────────────────────────────────────
    with step4:
        doc_type_clean = (new_doc_type or "").strip().upper().replace(" ", "_")
        display_clean = (new_display_name or "").strip()

        # Validation checks
        errors = []
        if not doc_type_clean:
            errors.append("Document Type Code is required.")
        elif not re.match(r'^[A-Z][A-Z0-9_]*$', doc_type_clean):
            errors.append("Type code must be uppercase letters, digits, and underscores (e.g. PURCHASE_ORDER).")
        if not display_clean:
            errors.append("Display Name is required.")
        if not new_fields:
            errors.append("At least one field is required — go to the Fields tab.")

        # Check for duplicate names within fields
        field_names = [f["name"] for f in new_fields]
        dupes = [n for n in field_names if field_names.count(n) > 1]
        if dupes:
            errors.append(f"Duplicate field names: {', '.join(set(dupes))}")

        # Check if type already exists
        existing = get_doc_type_config(session, doc_type_clean) if doc_type_clean else None
        if existing:
            errors.append(f"Document type '{doc_type_clean}' already exists. Edit it above instead.")

        if errors:
            for err in errors:
                st.error(err)
        else:
            _render_live_preview(display_clean, new_fields, new_tc)

            st.markdown("---")

            # Test extraction option
            st.markdown("**Optional: Test before saving**")
            test_file = st.file_uploader("Upload a sample PDF", type=["pdf"], key="new_test_upload")
            if test_file and st.button("Run Test Extraction", key="new_run_test"):
                with st.spinner("Running AI_EXTRACT..."):
                    prompt, _, _, _ = _build_config_from_fields(
                        doc_type_clean, display_clean, new_fields, new_tc or None,
                    )
                    result, err = _run_test_extraction(doc_type_clean, prompt, test_file)
                    if err:
                        st.error(f"Test failed: {err}")
                    elif result:
                        st.success("Test extraction succeeded!")
                        st.json(result)

            st.markdown("---")
            if st.button("Create Document Type", type="primary", key="create_final"):
                prompt, field_labels, review_fields, table_schema = _save_config(
                    doc_type_clean, display_clean, new_fields, new_tc, is_update=False,
                )

                # Show what was generated
                with st.expander("Generated Configuration"):
                    st.code(prompt, language="text")
                    st.json(field_labels)

                # Clean up session state
                for k in ["selected_template", "new_doc_type", "new_display_name", "new_fields", "new_tc"]:
                    if k in st.session_state:
                        del st.session_state[k]

                st.rerun()

else:
    st.caption("Select a template above to get started.")


render_nav_bar()
