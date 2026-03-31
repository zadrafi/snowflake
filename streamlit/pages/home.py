"""
Home — Landing page with pipeline architecture, features, and quick-start guide.
"""

import streamlit as st
import pandas as pd
from config import DB, get_session, inject_custom_css, sidebar_branding

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()


# ══════════════════════════════════════════════════════════════════════════════
# HERO
# ══════════════════════════════════════════════════════════════════════════════

st.markdown(
    '<div style="padding:1rem 0 0.5rem 0;">'
    '<h1 style="margin:0;line-height:1.1;font-size:2.2rem;">'
    'AI-Powered Document Extraction</h1>'
    '<p style="font-size:1.1rem;color:#64748b;margin-top:0.4rem;max-width:720px;">'
    'Extract structured data from invoices, utility bills, receipts, and more — '
    'powered by <strong style="color:#29B5E8;">Snowflake Cortex AI_EXTRACT</strong>. '
    'Documents are <strong>automatically classified</strong> and extracted with '
    'the right prompt — no manual type selection required.'
    '</p></div>',
    unsafe_allow_html=True,
)

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# LIVE STATUS
# ══════════════════════════════════════════════════════════════════════════════

try:
    status = session.sql(f"SELECT * FROM {DB}.V_EXTRACTION_STATUS").to_pandas()

    if len(status) > 0:
        s = status.iloc[0]
        total = int(s["TOTAL_FILES"])
        extracted = int(s["EXTRACTED_FILES"])
        pending = int(s["PENDING_FILES"])
        failed = int(s["FAILED_FILES"])
        pct = round(extracted / total * 100) if total > 0 else 0

        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric("Staged", f"{total:,}")
        sc2.metric("Extracted", f"{extracted:,}")
        sc3.metric("Pending", f"{pending:,}")
        sc4.metric("Failed", f"{failed:,}")
        sc5.metric("Progress", f"{pct}%")

        if s.get("LAST_EXTRACTION"):
            st.caption(f"Last extraction: {s['LAST_EXTRACTION']}")
except Exception:
    st.warning(
        "Could not load pipeline status. "
        "Run the setup scripts to initialize."
    )

# ── Classification stats ──────────────────────────────────────────────────
try:
    class_stats = session.sql(f"""
        SELECT
            COUNT(DISTINCT classified_doc_type) AS type_count,
            SUM(CASE WHEN classification_method = 'CORTEX_COMPLETE' THEN 1 ELSE 0 END) AS auto_classified,
            ROUND(AVG(CASE WHEN classification_confidence IS NOT NULL
                       THEN classification_confidence END), 2) AS avg_confidence
        FROM {DB}.RAW_DOCUMENTS
        WHERE classified_doc_type IS NOT NULL
    """).to_pandas()

    if len(class_stats) > 0 and class_stats.iloc[0]["TYPE_COUNT"] > 0:
        cc = class_stats.iloc[0]
        st.markdown(
            f'<div style="padding:8px 16px;background:#eff6ff;border:1px solid #bfdbfe;'
            f'border-radius:6px;font-size:0.85rem;color:#1e40af;">'
            f'&#129302; Auto-classification active — '
            f'<strong>{int(cc["TYPE_COUNT"])}</strong> doc types detected, '
            f'<strong>{int(cc["AUTO_CLASSIFIED"]):,}</strong> files auto-classified'
            f'</div>',
            unsafe_allow_html=True,
        )
except Exception:
    pass

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# ARCHITECTURE DIAGRAM
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("### How It Works")
st.caption("End-to-end pipeline: upload → classify → extract → serve")

st.graphviz_chart(
    """
    digraph architecture {
        rankdir=LR;
        bgcolor="transparent";
        node [shape=box, style="rounded,filled", fontname="Helvetica",
              fontsize=11, fillcolor="#e8f0fe", color="#4285f4",
              margin="0.15,0.08"];
        edge [color="#5f6368", fontname="Helvetica", fontsize=9];

        subgraph cluster_ingest {
            label="  Ingest  ";
            style="dashed"; color="#dadce0"; fontname="Helvetica"; fontsize=10;
            docs  [label="Documents\\n(PDF / image)", fillcolor="#fce8e6", color="#ea4335"];
            stage [label="Internal Stage\\n@DOCUMENT_STAGE"];
        }

        subgraph cluster_process {
            label="  Process  ";
            style="dashed"; color="#dadce0"; fontname="Helvetica"; fontsize=10;
            raw    [label="RAW_DOCUMENTS\\n(file tracking)"];
            stream [label="Stream\\n(change detect)"];
            task   [label="Scheduled Task\\n(5 min interval)"];
            classify [label="Cortex\\nAI_COMPLETE\\n(classify)", fillcolor="#fff3cd", color="#d97706"];
            config [label="DOC_TYPE_CONFIG\\n(prompts + fields)"];
            ai     [label="Cortex\\nAI_EXTRACT", fillcolor="#e6f4ea", color="#34a853"];
        }

        subgraph cluster_serve {
            label="  Serve  ";
            style="dashed"; color="#dadce0"; fontname="Helvetica"; fontsize=10;
            tables [label="EXTRACTED_FIELDS\\n+ TABLE_DATA"];
            views  [label="Analytical\\nViews"];
            app    [label="Dashboard\\n+ Viewer", fillcolor="#fef7e0", color="#fbbc04"];
        }

        docs     -> stage    [label="Upload"];
        stage    -> raw      [label="Register"];
        raw      -> stream;
        stream   -> task     [label="triggers"];
        task     -> classify [label="Step 1"];
        classify -> config   [label="match /\\ncreate type", style=dashed];
        config   -> ai       [label="prompt"];
        task     -> ai       [label="Step 2"];
        ai       -> tables   [label="JSON\\noutput"];
        tables   -> views;
        views    -> app;
        stage    -> app      [label="PDF render", style=dashed, color="#aaaaaa"];
    }
    """,
    use_container_width=True,
)


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE STEPS
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("### Pipeline Steps")

steps = [
    ("1. Upload",
     "Upload your documents using the **Process New** page, drag-and-drop in Snowsight, or the command line. "
     "Supported formats include PDF and image files.",
     None),
    ("2. Register",
     "New files are automatically detected and added to the processing queue. "
     "The system scans the upload folder for any documents that haven't been seen before and registers them for extraction.",
     None),
    ("3. Change Detection",
     "The pipeline is event-driven — it only wakes up and runs when new documents arrive. "
     "No polling, no wasted compute when there's nothing to process.",
     None),
    ("4. Auto-Classification",
     "AI reads the first page of each document and determines what type it is — invoice, receipt, utility bill, purchase order, etc. "
     "If it encounters a brand-new document type, it automatically creates a configuration for it and flags it for admin review.",
     None),
    ("5. Extraction",
     "Based on the document type, the system selects the right extraction template and pulls out all relevant fields — "
     "vendor name, dates, amounts, line items, and more.",
     None),
    ("6. Structured Output",
     "Extracted data is organized into two clean formats: "
     "**header fields** (vendor, total, dates) and **line items** (individual charges, quantities, prices). "
     "Both are ready for downstream reporting.",
     None),
    ("7. Dashboard & Analytics",
     "All extracted data flows into the dashboard, analytics, and review pages — "
     "giving you real-time visibility into spend, vendor activity, and extraction quality.",
     None),
]

for title, desc, code in steps:
    with st.expander(title, expanded=False):
        st.markdown(desc)
        if code:
            st.code(code, language="sql")


st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# KEY FEATURES
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("### Key Features")

fc1, fc2, fc3 = st.columns(3)

with fc1:
    st.markdown(
        '<div style="padding:16px;border:1px solid #e2e8f0;border-radius:8px;min-height:150px;">'
        '<div style="font-size:1.5rem;margin-bottom:6px;">&#129302;</div>'
        '<div style="font-size:0.95rem;font-weight:600;color:#1e293b;">Auto-Classification</div>'
        '<div style="font-size:0.78rem;color:#64748b;margin-top:4px;line-height:1.4;">'
        'Documents are automatically classified by type. '
        'New types are discovered and provisioned — no manual config needed.'
        '</div></div>',
        unsafe_allow_html=True,
    )

with fc2:
    st.markdown(
        '<div style="padding:16px;border:1px solid #e2e8f0;border-radius:8px;min-height:150px;">'
        '<div style="font-size:1.5rem;margin-bottom:6px;">&#127919;</div>'
        '<div style="font-size:0.95rem;font-weight:600;color:#1e293b;">Field Highlighting & Snip</div>'
        '<div style="font-size:0.78rem;color:#64748b;margin-top:4px;line-height:1.4;">'
        'Interactive PDF viewer with bounding boxes. '
        'Snip & Annotate mode for correcting missing fields with auto-mapping.'
        '</div></div>',
        unsafe_allow_html=True,
    )

with fc3:
    st.markdown(
        '<div style="padding:16px;border:1px solid #e2e8f0;border-radius:8px;min-height:150px;">'
        '<div style="font-size:1.5rem;margin-bottom:6px;">&#128200;</div>'
        '<div style="font-size:0.95rem;font-weight:600;color:#1e293b;">Full Observability</div>'
        '<div style="font-size:0.78rem;color:#64748b;margin-top:4px;line-height:1.4;">'
        'Spend analytics, extraction quality, pipeline timing, '
        'task history, and per-document credit attribution.'
        '</div></div>',
        unsafe_allow_html=True,
    )


st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# QUICK START + WHAT'S NEW
# ══════════════════════════════════════════════════════════════════════════════

with st.expander("Quick Start Guide", expanded=False):
    st.markdown("""
**First time setup:**
```sql
@sql/01_setup.sql                -- database, schema, warehouse, stage
@sql/02_tables.sql               -- tables
@sql/03_procedures.sql           -- SPs
@sql/04_tasks.sql                -- scheduled task
@sql/05_views.sql                -- analytical views
@sql/14_auto_classification.sql  -- auto-classification
```

**Upload your first document:**
```sql
PUT file://my_invoice.pdf @AI_EXTRACT_POC.DOCUMENTS.DOCUMENT_STAGE;
CALL SP_REGISTER_STAGED_FILES();
CALL SP_CLASSIFY_AND_EXTRACT();
```
    """)

with st.expander("What's New", expanded=False):
    st.markdown("""
**Top Navigation** — Horizontal nav bar replaces the sidebar menu and bottom nav buttons.

**Auto-Classification** — Documents classified at extraction time via Cortex AI_COMPLETE.
New types auto-provisioned and flagged for admin review.

**Snip & Annotate** — Draw rectangles on PDFs to correct fields with spatial auto-mapping.

**Pipeline Observability** — Latency tracking, task history, stale document detection.
    """)
