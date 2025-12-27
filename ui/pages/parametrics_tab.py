"""Parametrics tab UI implementation."""

import streamlit as st
from typing import List, Dict, Any
import pandas as pd

from ui.components.common import UIHelpers
from ui.services.data_service import DataService
from ui.services.session_service import SessionService
from ui.services.parametrics_service import ParametricsService


class ParametricsComponents:
    """Components specific to parametrics table functionality."""
    
    @staticmethod
    def create_parametrics_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Build a dataframe for parametrics data display."""
        if not records:
            return pd.DataFrame()
        
        rows: List[Dict[str, Any]] = []
        for r in records:
            rows.append({
                "prompt": str(r.get("prompt") or "").strip(),
                "craziness": r.get("craziness", 0),
                "isSexual": r.get("isSexual", False),
                "madeFor": str(r.get("madeFor") or "").strip(),
            })
        
        return pd.DataFrame(rows)
    
    @staticmethod
    def create_parametrics_editor_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Build a dataframe with a selectable column for editing/deletion."""
        if not records:
            return pd.DataFrame()
        
        rows: List[Dict[str, Any]] = []
        for r in records:
            rows.append({
                "selected": False,
                "prompt": str(r.get("prompt") or "").strip(),
                "craziness": r.get("craziness", 0),
                "isSexual": r.get("isSexual", False),
                "madeFor": str(r.get("madeFor") or "").strip(),
            })
        
        df = pd.DataFrame(rows)
        return df[["selected", "prompt", "craziness", "isSexual", "madeFor"]] if not df.empty else df
    
    @staticmethod
    def render_parametrics_editor_table(df: pd.DataFrame, key: str, height: int = 600) -> pd.DataFrame:
        """Render an editable parametrics data table with selection column."""
        if df.empty:
            st.info("No parametrics data available.")
            return df
            
        return st.data_editor(
            df,
            key=key,
            width="stretch",
            height=height,
            num_rows="fixed",
            disabled=False,
            hide_index=True,
            column_config={
                "selected": st.column_config.CheckboxColumn(
                    "selected",
                    help="Check rows you want to delete",
                    default=False,
                ),
                "prompt": st.column_config.TextColumn(
                    "Prompt",
                    help="Edit prompt text in the Database tab",
                    width="large",
                    disabled=True,
                ),
                "craziness": st.column_config.NumberColumn(
                    "Craziness Level",
                    help="Click to edit level of craziness (1-4)",
                    min_value=1,
                    max_value=4,
                    step=1,
                    format="%d",
                    disabled=False,
                ),
                "isSexual": st.column_config.CheckboxColumn(
                    "Is Sexual",
                    help="Click to edit whether the prompt has sexual content",
                    disabled=False,
                ),
                "madeFor": st.column_config.TextColumn(
                    "Made For",
                    help="Click to edit target audience (boys/girls/both)",
                    disabled=False,
                ),
            },
        )
    
    @staticmethod
    def render_delete_button(disabled: bool = False) -> bool:
        """Render delete button and return True if clicked."""
        delete_col, _ = st.columns([1, 5])
        with delete_col:
            return st.button(
                "Delete selected",
                key="parametrics_delete_button",
                type="secondary",
                use_container_width=True,
                disabled=disabled,
            )
    
    @staticmethod
    def get_selected_parametrics_prompts(df: pd.DataFrame, records: List[Dict[str, Any]]) -> List[str]:
        """Extract prompt values from selected rows."""
        try:
            if df.empty:
                return []
            
            selected_mask = df["selected"] == True  # noqa: E712
            selected_indices = df[selected_mask].index.tolist()
            selected_prompts: List[str] = []
            
            for idx in selected_indices:
                if idx < len(records):
                    prompt = str(records[idx].get("prompt") or "").strip()
                    if prompt:
                        selected_prompts.append(prompt)
            
            return selected_prompts
        except Exception:
            return []
    
    @staticmethod
    def render_parametrics_table(df: pd.DataFrame, height: int = 600) -> None:
        """Render a parametrics data table with proper column configuration."""
            
        st.dataframe(
            df,
            width="stretch",
            height=height,
            hide_index=True,
            column_config={
                "prompt": st.column_config.TextColumn(
                    "Prompt",
                    help="The prompt text",
                    width="large"
                ),
                "craziness": st.column_config.NumberColumn(
                    "Craziness Level",
                    help="Level of craziness (1-10)",
                    min_value=1,
                    max_value=10,
                    step=1,
                    format="%d"
                ),
                "isSexual": st.column_config.CheckboxColumn(
                    "Is Sexual",
                    help="Whether the prompt has sexual content",
                ),
                "madeFor": st.column_config.TextColumn(
                    "Made For",
                    help="Target audience for the prompt",
                ),
            },
        )


class ParametricsTab:
    """Complete parametrics tab functionality."""
    
    def __init__(self):
        self.components = ParametricsComponents()
        self.ui_helpers = UIHelpers()
    
    def render(self) -> None:
        """Render the complete parametrics tab."""
        
        # Render load button and handle action
        if self._render_load_button():
            self._handle_load_action()
        
        # Render clear all button and handle action
        if self._render_clear_all_button():
            self._handle_clear_all_action()
        
        # Render parameterization runner section
        self._render_parameterization_runner()
        
        # Render parametrics section
        self._render_parametrics_section()
    
    def _render_load_button(self) -> bool:
        """Render load button and return True if clicked."""
        col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
        
        with col1:
            return st.button(
                "Load data",
                use_container_width=True
            )
    
    def _render_clear_all_button(self) -> bool:
        """Render clear all button and return True if clicked."""
        col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
        
        with col2:
            return st.button(
                "🗑️ Clear All",
                use_container_width=True,
                type="secondary"
            )
    
    def _handle_load_action(self) -> None:
        """Handle the load button click."""
        try:
            with self.ui_helpers.with_spinner("Loading parametrics data from cloud storage…"):
                # Load parametrics data
                parametrics_records = DataService.load_parametrics()
                
                # Update session state
                st.session_state.parametrics_records = parametrics_records
                
        except Exception as e:
            self.ui_helpers.show_error_message(f"Failed to load parametrics data: {str(e)}")
    
    def _handle_clear_all_action(self) -> None:
        """Handle the clear all button click."""
        try:
            parametrics_service = ParametricsService()
            current_count = len(parametrics_service.load_parametrics())
            
            if current_count == 0:
                self.ui_helpers.show_info_message("PARAMETRICS.json is already empty.")
                return
            
            # Confirm deletion
            if st.session_state.get("parametrics_clear_all_confirmed", False):
                # Actually perform the deletion
                with self.ui_helpers.with_spinner("Clearing all parameterized content…"):
                    # Load current data with generation for optimistic concurrency
                    client = parametrics_service._get_client()
                    bucket_name = parametrics_service._bucket_name
                    object_name = parametrics_service._object_name
                    
                    from cloud_storage import downloadJson, uploadJsonWithPreconditions
                    current_data, generation = downloadJson(client, bucket_name, object_name)
                    
                    # Upload empty array
                    uploadJsonWithPreconditions(
                        client=client,
                        bucketName=bucket_name,
                        objectName=object_name,
                        data=[],
                        ifGenerationMatch=generation
                    )
                    
                    # Reload data and show success
                    st.session_state.parametrics_records = DataService.load_parametrics()
                    st.session_state.parametrics_clear_all_confirmed = False
                    self.ui_helpers.show_success_message(
                        f"Successfully cleared {current_count} parameterized items from PARAMETRICS.json"
                    )
            else:
                # Show confirmation message
                st.session_state.parametrics_clear_all_confirmed = True
                self.ui_helpers.show_warning_message(
                    f"⚠️ About to delete ALL {current_count} parameterized items. Click the button again to confirm."
                )
                
        except Exception as e:
            st.session_state.parametrics_clear_all_confirmed = False
            self.ui_helpers.show_error_message(f"Failed to clear parametrics: {str(e)}")
    
    def _render_parameterization_runner(self) -> None:
        """Render the parameterization runner section."""
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Number input for amount
            num_items = st.number_input(
                "Number of items to parameterize",
                min_value=1,
                max_value=2000,
                value=5,
                step=1
            )
        
        with col2:
            st.write("")  # Spacing
            st.write("")  # Spacing
            
            # Run button with money flying away icon
            if st.button(
                "💰 Run Parameterization",
                type="primary",
                use_container_width=True
            ):
                self._handle_parameterization_run(num_items)
    
    def _handle_parameterization_run(self, num_items: int) -> None:
        """Handle the parameterization run button click."""
        try:
            import subprocess
            import sys

            # Run the parameterization script
            with self.ui_helpers.with_spinner(f"Running parameterization for {num_items} items..."):
                result = subprocess.run(
                    [sys.executable, "llm_parameterization.py", str(num_items)],
                    capture_output=True,
                    text=True,
                    cwd="."
                )
            
            # Display results
            if result.returncode == 0:
                # Parse the output to extract key metrics
                output_lines = result.stdout.split('\n')
                success_info = []
                
                # Reload parametrics data
                st.session_state.parametrics_records = DataService.load_parametrics()
                st.rerun()
                
            else:
                st.error(f"❌ Parameterization failed with error code {result.returncode}")
                if result.stderr:
                    st.error(f"Error details: {result.stderr}")
                if result.stdout:
                    st.text("Output:")
                    st.text(result.stdout)
                    
        except Exception as e:
            st.error(f"❌ Failed to run parameterization: {str(e)}")
    
    def _render_parametrics_section(self) -> None:
        """Render the parametrics table section."""
        records = SessionService.get_parametrics_records()
        
        if not records:
            st.info("No parametrics data found. Click Load to view parametrics data from cloud storage.")
            return
        # Show metrics with craziness getting 2 columns for breathing room
        col1, col2, col3 = st.columns([2, 1, 1])
        
        total_count = len(records)
        
        with col1:
            # Calculate craziness percentages
            craziness_1 = len([r for r in records if r.get("craziness") == 1])
            craziness_2 = len([r for r in records if r.get("craziness") == 2])
            craziness_3 = len([r for r in records if r.get("craziness") == 3])
            craziness_4 = len([r for r in records if r.get("craziness") == 4])
            
            if total_count > 0:
                pct_1 = round((craziness_1 / total_count) * 100)
                pct_2 = round((craziness_2 / total_count) * 100)
                pct_3 = round((craziness_3 / total_count) * 100)
                pct_4 = round((craziness_4 / total_count) * 100)
                craziness_distribution = f"{pct_1}% / {pct_2}% / {pct_3}% / {pct_4}%"
            else:
                craziness_distribution = "0% / 0% / 0% / 0%"
            
            st.metric("Craziness 1/2/3/4", craziness_distribution)
        
        with col2:
            boys_count = len([r for r in records if r.get("madeFor") == "boys"])
            girls_count = len([r for r in records if r.get("madeFor") == "girls"])
            st.metric("For Boys/Girls", f"{boys_count}/{girls_count}")
        
        with col3:
            sexual_count = len([r for r in records if r.get("isSexual", False)])
            st.metric("Sexual Content", f"{sexual_count} / {total_count}")
        
        # Initialize original dataframe state for change detection
        if "parametrics_original_df" not in st.session_state or st.session_state.get("parametrics_records_hash") != hash(str(records)):
            df_editor = self.components.create_parametrics_editor_dataframe(records)
            st.session_state.parametrics_original_df = df_editor.copy()
            st.session_state.parametrics_records_hash = hash(str(records))
            st.session_state.parametrics_records = records.copy()
        else:
            df_editor = st.session_state.parametrics_original_df.copy()
        
        # Create and display editable table
        edited_df = self.components.render_parametrics_editor_table(df_editor, "parametrics_editor")
        
        # Auto-save any changes detected
        self._handle_autosave_changes(edited_df, st.session_state.parametrics_original_df, st.session_state.parametrics_records)
        
        # Handle delete functionality
        if self.components.render_delete_button(disabled=SessionService.is_writing()):
            self._handle_delete_action(edited_df, st.session_state.parametrics_records)
    
    def _handle_autosave_changes(self, edited_df: pd.DataFrame, original_df: pd.DataFrame, records: List[Dict[str, Any]]) -> None:
        """Detect and automatically save any changes made to the parametrics fields (craziness, isSexual, madeFor)."""
        # Check if there are any changes
        if edited_df.equals(original_df):
            return
        
        # Prevent re-saving if we just saved (check session state flag)
        if st.session_state.get("parametrics_just_saved", False):
            st.session_state.parametrics_just_saved = False
            return
        
        # Find rows where parametrics fields have changed
        changes_detected = False
        items_to_save = []
        
        for i in range(len(edited_df)):
            original_prompt = str(original_df.iloc[i]["prompt"]).strip()
            original_craziness = original_df.iloc[i]["craziness"]
            original_isSexual = bool(original_df.iloc[i]["isSexual"])
            original_madeFor = str(original_df.iloc[i]["madeFor"]).strip()
            
            edited_craziness = edited_df.iloc[i]["craziness"]
            edited_isSexual = bool(edited_df.iloc[i]["isSexual"])
            edited_madeFor = str(edited_df.iloc[i]["madeFor"]).strip()
            
            # Check if any parametrics field changed (not prompt)
            if (original_craziness != edited_craziness or 
                original_isSexual != edited_isSexual or 
                original_madeFor != edited_madeFor):
                changes_detected = True
                if i < len(records):
                    items_to_save.append((
                        i, 
                        records[i], 
                        original_prompt,
                        edited_craziness,
                        edited_isSexual,
                        edited_madeFor
                    ))
        
        if not changes_detected:
            return
        
        # Save all changes
        if SessionService.is_writing():
            return  # Don't save if already writing
        
        try:
            SessionService.set_writing(True)
            
            saved_count = 0
            with self.ui_helpers.with_spinner("Auto-saving changes…"):
                parametrics_service = ParametricsService()
                
                # Load current data with generation for optimistic concurrency
                client = parametrics_service._get_client()
                bucket_name = parametrics_service._bucket_name
                object_name = parametrics_service._object_name
                
                from cloud_storage import downloadJson, uploadJsonWithPreconditions
                current_data, generation = downloadJson(client, bucket_name, object_name)
                
                if not isinstance(current_data, list):
                    current_data = []
                
                # Update each changed item
                for index, original_record, prompt, new_craziness, new_isSexual, new_madeFor in items_to_save:
                    # Find the item in current_data by matching the prompt
                    item_found = False
                    for item in current_data:
                        if str(item.get("prompt", "")).strip() == prompt:
                            # Update the parametrics fields
                            item["craziness"] = int(new_craziness)
                            item["isSexual"] = bool(new_isSexual)
                            item["madeFor"] = str(new_madeFor).strip()
                            item_found = True
                            saved_count += 1
                            break
                    
                    if not item_found:
                        # If not found by prompt, skip this item
                        continue
                
                if saved_count > 0:
                    # Save the updated data back
                    uploadJsonWithPreconditions(
                        client=client,
                        bucketName=bucket_name,
                        objectName=object_name,
                        data=current_data,
                        ifGenerationMatch=generation
                    )
            
            SessionService.set_writing(False)
            
            if saved_count > 0:
                # Set flag to prevent re-saving on next render
                st.session_state.parametrics_just_saved = True
                # Reload data to get fresh state
                fresh_records = DataService.load_parametrics()
                st.session_state.parametrics_records = fresh_records
                # Update records hash to trigger refresh of original_df on next render
                st.session_state.parametrics_records_hash = hash(str(fresh_records))
                # Clear original_df so it gets recreated from fresh data
                if "parametrics_original_df" in st.session_state:
                    del st.session_state.parametrics_original_df
                # Show success message
                st.success(f"✅ Auto-saved {saved_count} change(s)!")
                st.rerun()
                
        except Exception as e:
            SessionService.set_writing(False)
            st.error(f"Failed to auto-save changes: {str(e)}")
    
    def _handle_delete_action(self, edited_df: pd.DataFrame, records: List[Dict[str, Any]]) -> None:
        """Handle deletion of selected parametrics items."""
        selected_prompts = self.components.get_selected_parametrics_prompts(edited_df, records)
        
        if not selected_prompts:
            self.ui_helpers.show_warning_message("No rows selected for deletion.")
            return
        
        try:
            SessionService.set_writing(True)
            
            with self.ui_helpers.with_spinner("Deleting from Parametrics DB…"):
                parametrics_service = ParametricsService()
                removed = parametrics_service.delete_parametrics_by_prompts(selected_prompts)
            
            SessionService.set_writing(False)
            
            # Reload data and show success
            st.session_state.parametrics_records = DataService.load_parametrics()
            # Clear original_df to force refresh
            if "parametrics_original_df" in st.session_state:
                del st.session_state.parametrics_original_df
            self.ui_helpers.show_success_message(
                f"Deleted {removed} item(s) from the parametrics database."
            )
            
        except Exception as e:
            SessionService.set_writing(False)
            self.ui_helpers.show_error_message(f"Failed to delete selected rows: {str(e)}")
