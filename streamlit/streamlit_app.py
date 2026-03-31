"""
AI_EXTRACT Document Processing — Entrypoint
Uses st.navigation(position="top") for a horizontal top nav bar.
This file acts as the router — each page is a st.Page reference.
"""

import streamlit as st

st.set_page_config(
    page_title="AI_EXTRACT",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════
# st.navigation replaces the pages/ directory auto-discovery.
# Pages are grouped into sections that appear as dropdown menus in the top bar.

pages = {
    "": [
        st.Page("pages/home.py", title="Home", icon="🏠", default=True),
    ],
    "Documents": [
        st.Page("pages/6_Process_New.py", title="Process New", icon="📤"),
        st.Page("pages/1_Document_Viewer.py", title="Doc Viewer", icon="📋"),
        st.Page("pages/3_Review.py", title="Review", icon="✅"),
    ],
    "Analytics": [
        st.Page("pages/0_Dashboard.py", title="Dashboard", icon="📊"),
        st.Page("pages/2_Analytics.py", title="Analytics", icon="📈"),
        st.Page("pages/8_Accuracy.py", title="Accuracy", icon="🎯"),
    ],
    "Settings": [
        st.Page("pages/4_Admin.py", title="Admin", icon="⚙️")
       # st.Page("pages/5_Cost.py", title="Cost", icon="💰"),
      #  st.Page("pages/7_Claude_PDF_Analysis.py", title="Claude", icon="🤖"),
    ],
}

pg = st.navigation(pages, position="top")
pg.run()
