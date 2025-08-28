"""Database tab UI implementation."""

import streamlit as st
from typing import List, Dict, Any

from database import DatabaseManager
from ui.components.common import UIHelpers
from ui.components.metrics import MetricsDisplay
from ui.components.tables import TableComponents
from ui.services.data_service import DataService, run_async
from ui.services.session_service import SessionService


class DatabaseTab:
    """Complete database tab functionality."""
    
    def __init__(self):
        self.metrics = MetricsDisplay()
        self.tables = TableComponents()
        self.ui_helpers = UIHelpers()
    
    def render(self) -> None:
        """Render the complete database tab."""
        # Render metrics
        self.metrics.render_four_column_metrics()
        
        # Render load button and handle action
        if self.metrics.render_load_button():
            self.metrics.handle_load_action()
        
        # Render database section
        self._render_database_section()
        
        # Render discards section
        self._render_discards_section()
    
    def _render_database_section(self) -> None:
        """Render the main database table section."""
        records = SessionService.get_global_records()
        
        if not self.ui_helpers.show_info_or_data(
            records, 
            "No entries found in the global database.",
            "Click Load to view the global database."
        ):
            return
        
        # Create and display editable table
        df_editor = self.tables.create_editor_dataframe(records)
        edited_df = self.tables.render_editable_table(df_editor, "global_db_editor")
        
        # Handle delete functionality
        if self.tables.render_delete_button(disabled=SessionService.is_writing()):
            self._handle_delete_action(edited_df, records)
    
    def _render_discards_section(self) -> None:
        """Render the discards table section."""
        st.subheader("Discards")
        discards_records = SessionService.get_discards_records()
        
        if not self.ui_helpers.show_info_or_data(
            discards_records,
            "No discarded items found.",
            "Click Load to view discarded items."
        ):
            return
        
        # Create and display read-only table
        discards_df = self.tables.create_readonly_dataframe(discards_records)
        self.tables.render_readonly_table(discards_df, height=400, title="Discards")
    
    def _handle_delete_action(self, edited_df, records: List[Dict[str, Any]]) -> None:
        """Handle deletion of selected items."""
        selected_normalized = self.tables.get_selected_items(edited_df, records)
        
        if not selected_normalized:
            self.ui_helpers.show_warning_message("No rows selected for deletion.")
            return
        
        try:
            SessionService.set_writing(True)
            
            with self.ui_helpers.with_spinner("Deleting from Cloud DB…"):
                db = DatabaseManager()
                removed = run_async(
                    db.remove_from_global_database_by_normalized(selected_normalized)
                )
            
            SessionService.set_writing(False)
            
            # Reload data and show success
            st.session_state.global_records = DataService.load_global_database()
            self.ui_helpers.show_success_message(
                f"Deleted {removed} item(s) from the global database."
            )
            
        except Exception as e:
            SessionService.set_writing(False)
            self.ui_helpers.show_error_message(f"Failed to delete selected rows: {str(e)}")
