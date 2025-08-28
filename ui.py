"""Streamlined main UI entry point for the Prompt Cleaner application."""

import os

# Bootstrap secrets to environment variables at app start
try:
    import streamlit as st
    xai = st.secrets.get("xai", {})
    key = xai.get("XAI_API_KEY") or st.secrets.get("XAI_API_KEY")
    url = xai.get("BASE_URL") or st.secrets.get("XAI_BASE_URL", "https://api.x.ai/v1")
    model = xai.get("MODEL") or st.secrets.get("XAI_MODEL", "grok-3-mini")

    if key: os.environ.setdefault("XAI_API_KEY", key)
    if url: os.environ.setdefault("XAI_BASE_URL", str(url))
    if model: os.environ.setdefault("XAI_MODEL", str(model))
except Exception:
    pass

import streamlit as st

# Import error handling
try:
    from ui.pages.database_tab import DatabaseTab
    from ui.pages.selection_tab import SelectionTab
except ImportError as e:
    st.error(f"Import Error: {e}")
    st.error("Missing dependencies or module import failure. Make sure project files are deployed.")
    st.stop()


def main() -> None:
    """Main application entry point."""
    # Configure page
    st.set_page_config(page_title="Prompt Cleaner UI", layout="wide")
    st.title("FYL.LA prompt management")
    
    # Create tabs
    tab_database, tab_selection = st.tabs(["Database", "Selection"])
    
    # Render database tab
    with tab_database:
        database_tab = DatabaseTab()
        database_tab.render()
    
    # Render selection tab
    with tab_selection:
        selection_tab = SelectionTab()
        selection_tab.render()


if __name__ == "__main__":
    main()
