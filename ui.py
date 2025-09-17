#!/usr/bin/env python3
"""Streamlined UI entry point for the Prompt Cleaner Global application."""

import streamlit as st
from ui.pages.database_tab import DatabaseTab
from ui.pages.selection_tab import SelectionTab
from ui.pages.input_tab import InputTab
from ui.pages.parametrics_tab import ParametricsTab


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
    input_tab = InputTab()
    parametrics_tab = ParametricsTab()

    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Database", "Selection", "Input", "Parametrics"])

    with tab1:
        database_tab.render()

    with tab2:
        selection_tab.render()

    with tab3:
        input_tab.render()

    with tab4:
        parametrics_tab.render()


if __name__ == "__main__":
    main()