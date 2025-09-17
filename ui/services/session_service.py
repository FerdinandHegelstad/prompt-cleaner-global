"""Session state management service for Streamlit."""

from typing import Any, Dict, List, Optional, Set
import streamlit as st


class SessionService:
    """Centralized session state management."""
    
    @staticmethod
    def get_data_counts() -> Dict[str, int]:
        """Get counts for all data types from session state."""
        return {
            "db_count": len(st.session_state.get("global_records", [])),
            "discards_count": len(st.session_state.get("discards_records", [])),
            "selection_count": len(st.session_state.get("user_selection_records", [])),
            "parametrics_count": len(st.session_state.get("parametrics_records", [])),
        }
    
    @staticmethod
    def update_all_data(data: Dict[str, List[Dict[str, Any]]]) -> None:
        """Update all data in session state."""
        for key, value in data.items():
            st.session_state[key] = value
    
    @staticmethod
    def get_global_records() -> Optional[List[Dict[str, Any]]]:
        """Get global database records from session."""
        return st.session_state.get("global_records")
    
    @staticmethod
    def get_discards_records() -> Optional[List[Dict[str, Any]]]:
        """Get discards records from session."""
        return st.session_state.get("discards_records")
    
    @staticmethod
    def get_parametrics_records() -> Optional[List[Dict[str, Any]]]:
        """Get parametrics records from session."""
        return st.session_state.get("parametrics_records")
    
    @staticmethod
    def is_writing() -> bool:
        """Check if currently writing to database."""
        return bool(st.session_state.get("isWriting", False))
    
    @staticmethod
    def set_writing(writing: bool) -> None:
        """Set writing state."""
        st.session_state.isWriting = writing


class BatchSessionService:
    """Session state management for batch review functionality."""
    
    @staticmethod
    def initialize_batch_state() -> None:
        """Initialize batch-related session state variables."""
        if "batch_items" not in st.session_state:
            st.session_state.batch_items = []
        if "discard_actions" not in st.session_state:
            st.session_state.discard_actions = set()
        if "batch_id" not in st.session_state:
            st.session_state.batch_id = 0
    
    @staticmethod
    def get_batch_items() -> List[Dict[str, Any]]:
        """Get current batch items."""
        return st.session_state.get("batch_items", [])
    
    @staticmethod
    def set_batch_items(items: List[Dict[str, Any]]) -> None:
        """Set batch items and increment batch ID."""
        st.session_state.batch_items = items
        st.session_state.batch_id = st.session_state.get("batch_id", 0) + 1
    
    @staticmethod
    def clear_batch() -> None:
        """Clear current batch and discard actions."""
        st.session_state.batch_items = []
        st.session_state.discard_actions = set()
    
    @staticmethod
    def get_batch_id() -> int:
        """Get current batch ID."""
        return st.session_state.get("batch_id", 0)
    
    @staticmethod
    def get_discard_actions() -> Set[str]:
        """Get current discard actions."""
        return st.session_state.get("discard_actions", set())
    
    @staticmethod
    def add_discard_action(action: str) -> None:
        """Add a discard action."""
        if "discard_actions" not in st.session_state:
            st.session_state.discard_actions = set()
        st.session_state.discard_actions.add(action)
    
    @staticmethod
    def has_workflow_error() -> Optional[str]:
        """Check for workflow errors."""
        return st.session_state.get("workflow_error")
    
    @staticmethod
    def has_workflow_warning() -> Optional[str]:
        """Check for workflow warnings."""
        return st.session_state.get("workflow_warning")
    
    @staticmethod
    def clear_workflow_messages() -> None:
        """Clear workflow error and warning messages."""
        if "workflow_error" in st.session_state:
            del st.session_state.workflow_error
        if "workflow_warning" in st.session_state:
            del st.session_state.workflow_warning
