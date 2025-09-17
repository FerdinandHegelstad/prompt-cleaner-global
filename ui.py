#!/usr/bin/env python3
"""Streamlined UI entry point for the Prompt Cleaner Global application."""

import streamlit as st
from ui.pages.database_tab import DatabaseTab
from ui.pages.selection_tab import SelectionTab


def main() -> None:
    """Main Streamlit application entry point."""
    st.set_page_config(
        page_title="FYL.LA database manager",
        page_icon="👨‍💻",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("FYL.LA database manager")

    # Create tab instances
    database_tab = DatabaseTab()
    selection_tab = SelectionTab()

    # Create tabs
    tab1, tab2 = st.tabs(["Database", "Selection"])

    with tab1:
        database_tab.render()

    with tab2:
        selection_tab.render()


if __name__ == "__main__":
    main()