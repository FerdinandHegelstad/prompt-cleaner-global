"""Reusable metrics display components."""

import streamlit as st
from typing import Dict, List, Any

from ui.services.data_service import DataService
from ui.services.session_service import SessionService


class MetricsDisplay:
    """Metrics display component for dashboard overview."""
    
    @staticmethod
    def render_four_column_metrics() -> None:
        """Render the top-level metrics display."""
        # Get raw file count
        raw_count, raw_status = DataService.get_raw_file_count()
        
        # Get cached counts from session
        counts = SessionService.get_data_counts()
        
        # Calculate parameterized count from global records
        global_records = st.session_state.get("global_records", [])
        parameterized_count = sum(1 for r in global_records if "craziness" in r) if global_records else 0
        
        # Create five columns
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Unprocessed Lines", raw_status)
        with col2:
            st.metric("Items in Database", f"{counts['db_count']:,}")
        with col3:
            st.metric("Parameterized", f"{parameterized_count:,}")
        with col4:
            st.metric("Items in Discards", f"{counts['discards_count']:,}")
        with col5:
            st.metric("In User Selection", f"{counts['selection_count']:,}")
    
    @staticmethod
    def render_parametrics_metrics(records: List[Dict[str, Any]]) -> None:
        """Render parametrics-specific metrics row."""
        if not records:
            return
        
        parameterized = [r for r in records if "craziness" in r]
        total_count = len(records)
        param_count = len(parameterized)
        
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        
        with col1:
            if param_count > 0:
                c1 = len([r for r in parameterized if r.get("craziness") == 1])
                c2 = len([r for r in parameterized if r.get("craziness") == 2])
                c3 = len([r for r in parameterized if r.get("craziness") == 3])
                c4 = len([r for r in parameterized if r.get("craziness") == 4])
                p1 = round((c1 / param_count) * 100)
                p2 = round((c2 / param_count) * 100)
                p3 = round((c3 / param_count) * 100)
                p4 = round((c4 / param_count) * 100)
                distribution = f"{p1}% / {p2}% / {p3}% / {p4}%"
            else:
                distribution = "0% / 0% / 0% / 0%"
            st.metric("Craziness 1/2/3/4", distribution)
        
        with col2:
            boys = len([r for r in parameterized if r.get("madeFor") == "boys"])
            girls = len([r for r in parameterized if r.get("madeFor") == "girls"])
            st.metric("For Boys/Girls", f"{boys}/{girls}")
        
        with col3:
            sexual = len([r for r in parameterized if r.get("isSexual", False)])
            st.metric("Sexual Content", f"{sexual} / {param_count}")
        
        with col4:
            st.metric("Parameterized", f"{param_count} / {total_count}")
    
    @staticmethod
    def render_load_button() -> bool:
        """Render load button and return True if clicked."""
        load_col, _ = st.columns([1, 6])
        with load_col:
            return st.button("Load", width="stretch")
    
    @staticmethod
    def handle_load_action() -> bool:
        """Handle the load button action. Returns True if successful."""
        with st.spinner("Loading all data"):
            try:
                data = DataService.load_all_data()
                SessionService.update_all_data(data)
                st.rerun()
                return True
            except Exception as e:
                st.error(f"Failed to load data: {e}")
                return False
