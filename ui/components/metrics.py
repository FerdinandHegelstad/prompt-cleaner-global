"""Reusable metrics display components."""

import streamlit as st
from typing import Dict

from ui.services.data_service import DataService
from ui.services.session_service import SessionService


class MetricsDisplay:
    """Metrics display component for dashboard overview."""
    
    @staticmethod
    def render_four_column_metrics() -> None:
        """Render the four-column metrics display."""
        # Get raw file count
        raw_count, raw_status = DataService.get_raw_file_count()
        
        # Get cached counts from session
        counts = SessionService.get_data_counts()
        
        # Create four columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Unprocessed Lines", raw_status)
        with col2:
            st.metric("Items in Database", f"{counts['db_count']:,}")
        with col3:
            st.metric("Items in Discards", f"{counts['discards_count']:,}")
        with col4:
            st.metric("Items in User Selection", f"{counts['selection_count']:,}")
    
    @staticmethod
    def render_load_button() -> bool:
        """Render load button and return True if clicked."""
        load_col, _ = st.columns([1, 6])
        with load_col:
            return st.button("Load", width="stretch")
    
    @staticmethod
    def handle_load_action() -> bool:
        """Handle the load button action. Returns True if successful."""
        with st.spinner("Loading all data..."):
            try:
                data = DataService.load_all_data()
                SessionService.update_all_data(data)
                st.rerun()
                return True
            except Exception as e:
                st.error(f"Failed to load data: {e}")
                return False
