"""Database tab UI implementation.

Unified view: database entries with parametric columns, parameterization
runner, clear-all-parametrics, and discards section.
"""

import streamlit as st
from typing import List, Dict, Any
import pandas as pd

from database import DatabaseManager
from cloud_storage import downloadJson, uploadJsonWithPreconditions
from ui.components.common import UIHelpers
from ui.components.metrics import MetricsDisplay
from ui.components.tables import TableComponents
from ui.services.data_service import DataService, run_async
from ui.services.parametrics_service import ParametricsService
from ui.services.session_service import SessionService


class DatabaseTab:
    """Complete database tab functionality with integrated parametrics."""
    
    def __init__(self):
        self.metrics = MetricsDisplay()
        self.tables = TableComponents()
        self.ui_helpers = UIHelpers()
    
    def render(self) -> None:
        """Render the complete database tab."""
        # Show confirmation dialogs if triggered
        if st.session_state.get("show_clear_parametrics_dialog"):
            self._clear_parametrics_dialog()
        if st.session_state.get("show_clear_previews_dialog"):
            self._clear_previews_dialog()

        # Render top-level metrics
        self.metrics.render_four_column_metrics()

        # Render load button and handle action
        if self.metrics.render_load_button():
            self.metrics.handle_load_action()

        # Action buttons row: Clear All Parametrics + Run Parameterization
        self._render_action_buttons()

        # Render database section (unified table with parametric columns)
        self._render_database_section()

        # Render discards section
        self._render_discards_section()
    
    # ------------------------------------------------------------------
    # Action buttons
    # ------------------------------------------------------------------

    def _render_action_buttons(self) -> None:
        """Render parameterization and preview controls."""
        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            if st.button("Clear All Parametrics", use_container_width=True, type="secondary"):
                self._handle_clear_all_action()

        with col2:
            num_items = st.number_input(
                "Items to parameterize",
                min_value=1,
                value=5,
                step=1,
                label_visibility="collapsed",
            )

        with col3:
            st.write("")  # vertical alignment spacer
            if st.button("Run Parameterization", type="primary", use_container_width=True):
                self._handle_parameterization_run(num_items)

        # Preview generation row
        pcol1, pcol2, pcol3 = st.columns([1, 1, 2])

        with pcol1:
            if st.button("Clear All Previews", use_container_width=True, type="secondary"):
                self._handle_clear_all_previews()

        with pcol2:
            num_preview_items = st.number_input(
                "Items to preview",
                min_value=1,
                max_value=5000,
                value=10,
                step=5,
                label_visibility="collapsed",
            )

        with pcol3:
            st.write("")  # vertical alignment spacer
            if st.button("Generate Previews", type="primary", use_container_width=True):
                self._handle_preview_run(num_preview_items)

    def _handle_clear_all_action(self) -> None:
        """Clear craziness/isSexual/madeFor from all entries (keeps entries)."""
        st.session_state.show_clear_parametrics_dialog = True

    @st.dialog("Clear All Parametrics")
    def _clear_parametrics_dialog(self) -> None:
        """Confirmation dialog for clearing all parametric fields."""
        try:
            parametrics_service = ParametricsService()
            all_entries = parametrics_service.load_all_database_entries()
            parameterized_count = sum(1 for e in all_entries if "craziness" in e)

            if parameterized_count == 0:
                st.info("No entries have parametric fields to clear.")
                if st.button("Close"):
                    st.rerun()
                return

            st.warning(
                f"This will remove craziness, isSexual, filler, and madeFor from **{parameterized_count}** entries. "
                "This action cannot be undone."
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Cancel", use_container_width=True):
                    st.session_state.show_clear_parametrics_dialog = False
                    st.rerun()
            with col2:
                if st.button("Confirm Clear", type="primary", use_container_width=True):
                    with st.spinner("Clearing all parametric fields…"):
                        cleared = parametrics_service.clear_all_parametric_fields()
                        st.session_state.global_records = DataService.load_global_database()
                        self._invalidate_df_cache()
                    st.session_state.show_clear_parametrics_dialog = False
                    st.toast(f"Cleared parametric fields from {cleared} entries.")
                    st.rerun()
        except Exception as e:
            st.error(f"Failed to clear parametrics: {str(e)}")

    def _handle_clear_all_previews(self) -> None:
        """Clear the 'preview' field from all database entries."""
        st.session_state.show_clear_previews_dialog = True

    @st.dialog("Clear All Previews")
    def _clear_previews_dialog(self) -> None:
        """Confirmation dialog for clearing all preview fields."""
        try:
            parametrics_service = ParametricsService()
            all_entries = parametrics_service.load_all_database_entries()
            preview_count = sum(1 for e in all_entries if "preview" in e)

            if preview_count == 0:
                st.info("No entries have previews to clear.")
                if st.button("Close"):
                    st.rerun()
                return

            st.warning(
                f"This will remove the preview field from **{preview_count}** entries. "
                "This action cannot be undone."
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Cancel", use_container_width=True):
                    st.session_state.show_clear_previews_dialog = False
                    st.rerun()
            with col2:
                if st.button("Confirm Clear", type="primary", use_container_width=True):
                    with st.spinner("Clearing all preview fields…"):
                        cleared = parametrics_service.clear_all_preview_fields()
                        st.session_state.global_records = DataService.load_global_database()
                        self._invalidate_df_cache()
                    st.session_state.show_clear_previews_dialog = False
                    st.toast(f"Cleared previews from {cleared} entries.")
                    st.rerun()
        except Exception as e:
            st.error(f"Failed to clear previews: {str(e)}")

    def _run_llm_script(self, script: str, num_items: int, spinner_label: str) -> None:
        """Run an LLM subprocess script and reload the database on success."""
        import subprocess
        import sys

        with self.ui_helpers.with_spinner(spinner_label):
            result = subprocess.run(
                [sys.executable, script, str(num_items)],
                capture_output=True,
                text=True,
                cwd=".",
            )

        if result.returncode == 0:
            st.session_state.global_records = DataService.load_global_database()
            self._invalidate_df_cache()
            st.rerun()
        else:
            st.error(f"{script} failed with error code {result.returncode}")
            if result.stderr:
                st.error(f"Error details: {result.stderr}")
            if result.stdout:
                st.text("Output:")
                st.text(result.stdout)

    def _handle_preview_run(self, num_items: int) -> None:
        """Run the LLM preview generation subprocess."""
        try:
            self._run_llm_script("llm_preview.py", num_items, f"Generating previews for {num_items} items...")
        except Exception as e:
            st.error(f"Failed to run preview generation: {str(e)}")

    def _handle_parameterization_run(self, num_items: int) -> None:
        """Run the LLM parameterization subprocess."""
        try:
            self._run_llm_script("llm_parameterization.py", num_items, f"Running parameterization for {num_items} items...")
        except Exception as e:
            st.error(f"Failed to run parameterization: {str(e)}")

    # ------------------------------------------------------------------
    # Main database table
    # ------------------------------------------------------------------

    def _render_database_section(self) -> None:
        """Render the main database table section with all columns."""
        records = SessionService.get_global_records()
        
        if not self.ui_helpers.show_info_or_data(
            records, 
            "No entries found in the global database.",
            "Click Load to view the global database."
        ):
            return
        
        # Parametrics-specific metrics row
        self.metrics.render_parametrics_metrics(records)
        
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

        # Auto-save any changes detected (prompt + parametric fields)
        self._handle_autosave_changes(edited_df, st.session_state.db_original_df, st.session_state.db_records)

        # Handle discard functionality
        if self.tables.render_discard_button(disabled=SessionService.is_writing()):
            self._handle_discard_action(edited_df, st.session_state.db_records)

    # ------------------------------------------------------------------
    # Autosave: detects prompt AND parametric field changes
    # ------------------------------------------------------------------

    def _handle_autosave_changes(self, edited_df: pd.DataFrame, original_df: pd.DataFrame, records: List[Dict[str, Any]]) -> None:
        """Detect and save changes to prompt text or parametric fields."""
        if edited_df.equals(original_df):
            return
        
        if st.session_state.get("db_just_saved", False):
            st.session_state.db_just_saved = False
            return
        
        prompt_changes: List[tuple] = []      # (index, original_record, new_prompt)
        parametric_changes: List[tuple] = []   # (index, original_record, prompt, craziness, isSexual, filler, madeFor)
        
        for i in range(len(edited_df)):
            if i >= len(records):
                break
            
            orig_prompt = str(original_df.iloc[i]["prompt"]).strip()
            edit_prompt = str(edited_df.iloc[i]["prompt"]).strip()
            
            # Check prompt change
            if orig_prompt != edit_prompt:
                prompt_changes.append((i, records[i], edit_prompt))
                continue  # if prompt changed, we re-add the whole entry
            
            # Check parametric field changes (NaN-safe comparisons)
            orig_craziness = original_df.iloc[i]["craziness"]
            orig_sexual = original_df.iloc[i]["isSexual"]
            orig_filler = original_df.iloc[i]["filler"]
            orig_madefor = str(original_df.iloc[i]["madeFor"]).strip()

            edit_craziness = edited_df.iloc[i]["craziness"]
            edit_sexual = edited_df.iloc[i]["isSexual"]
            edit_filler = edited_df.iloc[i]["filler"]
            edit_madefor = str(edited_df.iloc[i]["madeFor"]).strip()

            orig_c_nan = pd.isna(orig_craziness)
            edit_c_nan = pd.isna(edit_craziness)
            craziness_changed = (orig_c_nan != edit_c_nan) or (not orig_c_nan and not edit_c_nan and orig_craziness != edit_craziness)

            orig_s_nan = pd.isna(orig_sexual)
            edit_s_nan = pd.isna(edit_sexual)
            sexual_changed = (orig_s_nan != edit_s_nan) or (not orig_s_nan and not edit_s_nan and bool(orig_sexual) != bool(edit_sexual))

            orig_f_nan = pd.isna(orig_filler)
            edit_f_nan = pd.isna(edit_filler)
            filler_changed = (orig_f_nan != edit_f_nan) or (not orig_f_nan and not edit_f_nan and bool(orig_filler) != bool(edit_filler))

            madefor_changed = orig_madefor != edit_madefor

            if craziness_changed or sexual_changed or filler_changed or madefor_changed:
                parametric_changes.append((i, records[i], orig_prompt, edit_craziness, edit_sexual, edit_filler, edit_madefor))
        
        if not prompt_changes and not parametric_changes:
            return
        
        if SessionService.is_writing():
            return
        
        try:
            SessionService.set_writing(True)
            saved_count = 0
            
            with self.ui_helpers.with_spinner("Auto-saving changes…"):
                # Handle prompt text changes (remove + re-add + regenerate preview)
                if prompt_changes:
                    from llm_preview import generate_single_preview
                    db = DatabaseManager()
                    for _index, original_record, new_prompt_text in prompt_changes:
                        if not new_prompt_text or not new_prompt_text.strip():
                            continue
                        new_item = {
                            "prompt": new_prompt_text,
                            "occurrences": original_record.get("occurrences", 1),
                        }
                        for field in ("craziness", "isSexual", "filler", "madeFor"):
                            if field in original_record:
                                new_item[field] = original_record[field]
                        # Regenerate preview for the edited prompt
                        try:
                            new_preview = run_async(generate_single_preview(new_prompt_text))
                            if new_preview:
                                new_item["preview"] = new_preview
                        except Exception:
                            pass  # preview generation is best-effort
                        old_prompt = str(original_record.get("prompt") or "").strip()
                        if old_prompt:
                            run_async(db.remove_from_global_database_by_prompt([old_prompt]))
                        run_async(db.add_to_global_database(new_item))
                        saved_count += 1
                
                # Handle parametric field changes (in-place update)
                if parametric_changes:
                    parametrics_service = ParametricsService()
                    client = parametrics_service._get_client()
                    bucket_name = parametrics_service._bucket_name
                    object_name = parametrics_service._object_name
                    
                    current_data, generation = downloadJson(client, bucket_name, object_name)
                    if not isinstance(current_data, list):
                        current_data = []
                    
                    for _index, _original_record, prompt, new_craziness, new_sexual, new_filler, new_madefor in parametric_changes:
                        for item in current_data:
                            if str(item.get("prompt", "")).strip() == prompt:
                                if not pd.isna(new_craziness):
                                    item["craziness"] = int(new_craziness)
                                if not pd.isna(new_sexual):
                                    item["isSexual"] = bool(new_sexual)
                                if not pd.isna(new_filler):
                                    item["filler"] = bool(new_filler)
                                if new_madefor:
                                    item["madeFor"] = str(new_madefor).strip()
                                saved_count += 1
                                break
                    
                    if parametric_changes:
                        uploadJsonWithPreconditions(
                            client=client,
                            bucketName=bucket_name,
                            objectName=object_name,
                            data=current_data,
                            ifGenerationMatch=generation,
                        )
            
            SessionService.set_writing(False)
            
            if saved_count > 0:
                st.session_state.db_just_saved = True
                st.session_state.global_records = DataService.load_global_database()
                st.session_state.db_records_hash = hash(str(st.session_state.global_records))
                st.session_state.db_records = st.session_state.global_records.copy()
                self._invalidate_df_cache()
                st.success(f"Auto-saved {saved_count} change(s)!")
                st.rerun()
                
        except Exception as e:
            SessionService.set_writing(False)
            st.error(f"Failed to auto-save changes: {str(e)}")

    # ------------------------------------------------------------------
    # Discards
    # ------------------------------------------------------------------

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
        
        discards_df = self.tables.create_readonly_dataframe(discards_records)
        self.tables.render_readonly_table(discards_df, height=400, title="Discards")
    
    # ------------------------------------------------------------------
    # Discard (move from database to discards)
    # ------------------------------------------------------------------

    def _handle_discard_action(self, edited_df, records: List[Dict[str, Any]]) -> None:
        """Move selected items from the database to discards."""
        selected_prompts = self.tables.get_selected_items(edited_df, records)
        
        if not selected_prompts:
            self.ui_helpers.show_warning_message("No rows selected.")
            return
        
        # Build a lookup of full records so we preserve occurrences in discards
        selected_set = set(selected_prompts)
        items_to_discard = [
            r for r in records
            if str(r.get("prompt") or "").strip() in selected_set
        ]
        
        try:
            SessionService.set_writing(True)
            
            with self.ui_helpers.with_spinner("Moving to discards…"):
                db = DatabaseManager()
                
                # Add each item to discards first
                for item in items_to_discard:
                    run_async(db.add_to_discards({
                        "prompt": str(item.get("prompt") or "").strip(),
                        "occurrences": item.get("occurrences", 1),
                    }))
                
                # Then remove from the main database
                removed = run_async(
                    db.remove_from_global_database_by_prompt(selected_prompts)
                )
            
            SessionService.set_writing(False)
            
            st.session_state.global_records = DataService.load_global_database()
            self._invalidate_df_cache()
            self.ui_helpers.show_success_message(
                f"Moved {removed} item(s) to discards."
            )
            
        except Exception as e:
            SessionService.set_writing(False)
            self.ui_helpers.show_error_message(f"Failed to move selected rows to discards: {str(e)}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _invalidate_df_cache() -> None:
        """Remove cached dataframe state so it rebuilds on next render."""
        if "db_original_df" in st.session_state:
            del st.session_state["db_original_df"]
