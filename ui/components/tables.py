"""Reusable table components for data display and editing."""

from typing import Any, Dict, List
import pandas as pd
import streamlit as st


class TableComponents:
    """Reusable table components for different data types."""
    
    @staticmethod
    def create_editor_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Build a dataframe with a selectable column for editing/deletion."""
        rows: List[Dict[str, Any]] = []
        for r in records:
            rows.append({
                "selected": False,
                "cleaned": str(r.get("cleaned") or "").strip(),
                "occurrences": r.get("occurrences", 1),
            })
        df = pd.DataFrame(rows)
        return df[["selected", "cleaned", "occurrences"]] if not df.empty else df
    
    @staticmethod
    def create_readonly_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Build a read-only dataframe for display purposes."""
        rows: List[Dict[str, Any]] = []
        for r in records:
            rows.append({
                "cleaned": str(r.get("cleaned") or "").strip(),
                "occurrences": r.get("occurrences", 1),
            })
        return pd.DataFrame(rows)
    
    @staticmethod
    def render_editable_table(df: pd.DataFrame, key: str, height: int = 600) -> pd.DataFrame:
        """Render an editable data table with standard configuration."""
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
                "cleaned": st.column_config.TextColumn(
                    "cleaned",
                    disabled=True,
                ),
                "occurrences": st.column_config.NumberColumn(
                    "occurrences",
                    disabled=True,
                    help="Number of times this item has been encountered",
                ),
            },
        )
    
    @staticmethod
    def render_readonly_table(df: pd.DataFrame, height: int = 400, title: str = "Data") -> None:
        """Render a read-only data table."""
        st.dataframe(
            df,
            width="stretch",
            height=height,
            hide_index=True,
            column_config={
                "cleaned": st.column_config.TextColumn("Cleaned Text"),
                "occurrences": st.column_config.NumberColumn(
                    "Occurrences",
                    help=f"Number of times this item was processed"
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
                type="secondary",
                width="stretch",
                disabled=disabled,
            )
    
    @staticmethod
    def get_selected_items(df: pd.DataFrame, records: List[Dict[str, Any]]) -> List[str]:
        """Extract normalized values from selected rows."""
        try:
            selected_mask = df["selected"] == True  # noqa: E712
            selected_indices = df[selected_mask].index.tolist()
            selected_normalized: List[str] = []
            for idx in selected_indices:
                if idx < len(records):
                    norm_val = str(records[idx].get("normalized") or "").strip()
                    if norm_val:
                        selected_normalized.append(norm_val)
            return selected_normalized
        except Exception:
            return []


class BatchReviewComponents:
    """Components specific to batch review functionality."""
    
    @staticmethod
    def render_batch_item(item: Dict[str, Any], index: int, batch_id: int) -> bool:
        """Render a single batch item and return True if discarded."""
        col1, col2 = st.columns([1, 6])
        
        with col1:
            checkbox_key = f"discard_check_{batch_id}_{index}"
            discarded = st.checkbox("Discard", key=checkbox_key)
        
        with col2:
            cleaned_text = str(item.get("cleaned") or "").strip()
            st.text(cleaned_text or "(empty)")
        
        st.markdown("---")
        return discarded
    
    @staticmethod
    def render_fetch_next_button() -> bool:
        """Render fetch next button and return True if clicked."""
        return st.button("Fetch Next 5 Items (Keep rest)", width="stretch", type="primary")
