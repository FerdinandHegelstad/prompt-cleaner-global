"""Database tab UI implementation."""

import streamlit as st
from typing import List, Dict, Any
import pandas as pd

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
        
        # Initialize original dataframe state for change detection
        if "db_original_df" not in st.session_state or st.session_state.get("db_records_hash") != hash(str(records)):
            df_editor = self.tables.create_editor_dataframe(records)
            st.session_state.db_original_df = df_editor.copy()
            st.session_state.db_records_hash = hash(str(records))
            st.session_state.db_records = records.copy()
        else:
            df_editor = st.session_state.db_original_df.copy()
        
        # Create and display editable table
        edited_df = self.tables.render_editable_table(df_editor, "global_db_editor")

        # Auto-save any changes detected
        self._handle_autosave_changes(edited_df, st.session_state.db_original_df, st.session_state.db_records)

        # Handle delete functionality
        if self.tables.render_delete_button(disabled=SessionService.is_writing()):
            self._handle_delete_action(edited_df, st.session_state.db_records)

    def _handle_autosave_changes(self, edited_df: pd.DataFrame, original_df: pd.DataFrame, records: List[Dict[str, Any]]) -> None:
        """Detect and automatically save any changes made to the cleaned column."""
        # Check if there are any changes
        if edited_df.equals(original_df):
            return
        
        # Prevent re-saving if we just saved (check session state flag)
        if st.session_state.get("db_just_saved", False):
            st.session_state.db_just_saved = False
            return
        
        # Find rows where cleaned text has changed
        changes_detected = False
        items_to_save = []
        
        for i in range(len(edited_df)):
            original_cleaned = str(original_df.iloc[i]["cleaned"]).strip()
            edited_cleaned = str(edited_df.iloc[i]["cleaned"]).strip()
            
            if original_cleaned != edited_cleaned:
                changes_detected = True
                if i < len(records):
                    items_to_save.append((i, records[i], edited_cleaned))
        
        if not changes_detected:
            return
        
        # Save all changes
        if SessionService.is_writing():
            return  # Don't save if already writing
        
        try:
            SessionService.set_writing(True)
            
            from text_utils import normalize
            
            saved_count = 0
            with self.ui_helpers.with_spinner("Auto-saving changes…"):
                db = DatabaseManager()
                
                for index, original_record, new_cleaned_text in items_to_save:
                    if not new_cleaned_text or not new_cleaned_text.strip():
                        continue  # Skip empty edits
                    
                    # Normalize the new text
                    normalized_text = normalize(new_cleaned_text).strip()
                    if not normalized_text:
                        continue  # Skip if normalization results in empty
                    
                    # Create the new item
                    new_item = {
                        "default": new_cleaned_text,
                        "cleaned": new_cleaned_text,
                        "normalized": normalized_text,
                        "occurrences": original_record.get("occurrences", 1)
                    }
                    
                    # Remove the old item
                    old_normalized = str(original_record.get("normalized") or "").strip()
                    if old_normalized:
                        run_async(db.remove_from_global_database_by_normalized([old_normalized]))
                    
                    # Add the new item
                    run_async(db.add_to_global_database(new_item))
                    saved_count += 1
            
            SessionService.set_writing(False)
            
            if saved_count > 0:
                # Set flag to prevent re-saving on next render
                st.session_state.db_just_saved = True
                # Reload data to get fresh state
                st.session_state.global_records = DataService.load_global_database()
                # Update records hash to trigger refresh of original_df on next render
                st.session_state.db_records_hash = hash(str(st.session_state.global_records))
                st.session_state.db_records = st.session_state.global_records.copy()
                # Clear original_df so it gets recreated from fresh data
                if "db_original_df" in st.session_state:
                    del st.session_state.db_original_df
                # Show success message
                st.success(f"✅ Auto-saved {saved_count} change(s)!")
                st.rerun()
                
        except Exception as e:
            SessionService.set_writing(False)
            st.error(f"Failed to auto-save changes: {str(e)}")

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
