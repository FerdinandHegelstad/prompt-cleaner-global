"""Selection tab UI implementation."""

import streamlit as st
from typing import Dict, Any, List

from ui.components.common import UIHelpers, WorkflowMessages
from ui.components.tables import BatchReviewComponents
from ui.services.data_service import SelectionService, run_async
from ui.services.session_service import BatchSessionService
from text_utils import normalize


class SelectionTab:
    """Complete selection tab functionality."""
    
    def __init__(self):
        self.selection_service = SelectionService()
        self.batch_session = BatchSessionService()
        self.ui_helpers = UIHelpers()
        self.workflow_messages = WorkflowMessages()
        self.batch_components = BatchReviewComponents()
    
    def render(self) -> None:
        """Render the complete selection tab."""
        st.subheader("Batch Review")
        
        # Initialize session state
        self.batch_session.initialize_batch_state()
        
        # Load initial batch if needed
        if not self.batch_session.get_batch_items():
            self._load_initial_batch()
            return
        
        # Display workflow messages if any
        self._display_workflow_messages()
        
        # Render batch items
        self._render_batch_items()
        
        # Handle fetch next action
        if self.batch_components.render_fetch_next_button():
            self._handle_fetch_next()

        # Manual input section
        self._render_manual_input()
    
    def _load_initial_batch(self) -> None:
        """Load the initial batch of items."""
        with self.ui_helpers.with_spinner("Loading batch items"):
            items = self.selection_service.fetch_batch_items(5)
            if items:
                self.batch_session.set_batch_items(items)
                st.rerun()
    
    def _display_workflow_messages(self) -> None:
        """Display any workflow-related messages."""
        error_msg = self.batch_session.has_workflow_error()
        warning_msg = self.batch_session.has_workflow_warning()
        
        if error_msg:
            st.error(error_msg)
            self.batch_session.clear_workflow_messages()
        
        if warning_msg:
            st.warning(warning_msg)
            self.batch_session.clear_workflow_messages()
    
    def _render_batch_items(self) -> None:
        """Render all batch items with discard checkboxes."""
        items = self.batch_session.get_batch_items()
        batch_id = self.batch_session.get_batch_id()
        
        for i, item in enumerate(items):
            if self.batch_components.render_batch_item(item, i, batch_id):
                self.batch_session.add_discard_action(f"discard_{i}")
    
    def _handle_fetch_next(self) -> None:
        """Handle the fetch next button action."""
        items = self.batch_session.get_batch_items()
        discard_actions = self.batch_session.get_discard_actions()
        
        # Process current batch
        if items:
            kept_count, discarded_count = self.selection_service.process_batch_items(
                items, discard_actions
            )
            self.workflow_messages.show_batch_processing_result(kept_count, discarded_count)
        
        # Clear current batch
        self.batch_session.clear_batch()
        
        # Fetch new items
        try:
            new_items = self.selection_service.fetch_batch_items(5)
            if new_items:
                self.batch_session.set_batch_items(new_items)
        except Exception as e:
            self.ui_helpers.show_error_message(f"Error fetching new items: {e}")
        
        # Force rerun to update UI
        st.rerun()

    def _render_manual_input(self) -> None:
        """Render the manual input section."""
        st.header("Manual input")

        # Text input field
        manual_input = st.text_input(
            "Prompt",
            key="manual_input"
        )

        # Add button
        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            if st.button("Add", type="primary", use_container_width=True):
                self._handle_manual_add(manual_input)

    def _handle_manual_add(self, input_text: str) -> None:
        """Handle adding manual input to the database."""
        if not input_text or not input_text.strip():
            st.warning("Please enter some text to add.")
            return

        try:
            # Clean and normalize the input
            cleaned_text = input_text.strip()
            normalized_text = normalize(cleaned_text).strip()

            if not normalized_text:
                st.error("The input text resulted in empty normalized text. Please try different text.")
                return

            # Create the item dictionary
            item = {
                "default": input_text.strip(),
                "cleaned": cleaned_text,
                "normalized": normalized_text,
                "occurrences": 1
            }

            # Add to global database
            db = self.selection_service.get_cached_db_manager()
            run_async(db.add_to_global_database(item))

            # Clear the input field by rerunning
            st.success(f"✅ Added to database: {cleaned_text[:50]}{'...' if len(cleaned_text) > 50 else ''}")
            # Reset manual input so the field is blank on rerun
            st.session_state["manual_input"] = ""
            st.rerun()

        except Exception as e:
            st.error(f"Error adding to database: {e}")
