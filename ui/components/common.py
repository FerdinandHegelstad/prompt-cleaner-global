"""Common UI utilities and helper functions."""

import streamlit as st
from typing import Optional


class UIHelpers:
    """Common UI helper functions."""
    
    @staticmethod
    def show_info_or_data(data: Optional[list], empty_message: str, load_message: str) -> bool:
        """Show appropriate message based on data state. Returns True if data exists."""
        if data is None:
            st.info(load_message)
            return False
        elif not data:
            st.info(empty_message)
            return False
        return True
    
    @staticmethod
    def show_success_message(message: str) -> None:
        """Show success message and rerun."""
        st.success(message)
        st.rerun()
    
    @staticmethod
    def show_error_message(message: str) -> None:
        """Show error message."""
        st.error(message)
    
    @staticmethod
    def show_warning_message(message: str) -> None:
        """Show warning message."""
        st.warning(message)
    
    @staticmethod
    def with_spinner(message: str):
        """Context manager for spinner display."""
        return st.spinner(message)


class WorkflowMessages:
    """Handle workflow-related messages and notifications."""
    
    @staticmethod
    def show_workflow_status(workflow_result: Optional[dict]) -> None:
        """Display workflow status messages based on result."""
        if not workflow_result:
            return
            
        status = workflow_result.get("status")
        message = workflow_result.get("message", "")
        
        if status == "all_failed":
            st.error(f"⚠️ **LLM Processing Failed**: {message}")
        elif status == "error":
            st.error(f"⚠️ **Workflow Error**: {message}")
        elif workflow_result.get("failed", 0) > 0:
            st.warning(f"⚠️ **Partial Success**: {message}")
        else:
            st.success(f"✅ **Success**: {message}")
    
    @staticmethod
    def show_batch_processing_result(kept_count: int, discarded_count: int) -> None:
        """Show results of batch processing."""
        if kept_count > 0 or discarded_count > 0:
            st.success(f"Processed {kept_count + discarded_count} items: {kept_count} kept, {discarded_count} discarded")
