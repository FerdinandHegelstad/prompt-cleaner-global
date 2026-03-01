"""Reusable table components for data display and editing."""

from typing import Any, Dict, List
import pandas as pd
import streamlit as st


class TableComponents:
    """Reusable table components for different data types."""
    
    @staticmethod
    def create_editor_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Build a dataframe with all columns for editing/deletion."""
        rows: List[Dict[str, Any]] = []
        for r in records:
            rows.append({
                "selected": False,
                "prompt": str(r.get("prompt") or "").strip(),
                "preview": str(r.get("preview") or "").strip(),
                "occurrences": r.get("occurrences", 1),
                "craziness": r.get("craziness", None),
                "isSexual": r.get("isSexual", None),
                "madeFor": str(r.get("madeFor") or "").strip(),
            })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        return df[["selected", "prompt", "preview", "occurrences", "craziness", "isSexual", "madeFor"]]
    
    @staticmethod
    def create_readonly_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
        """Build a read-only dataframe for display purposes."""
        rows: List[Dict[str, Any]] = []
        for r in records:
            rows.append({
                "prompt": str(r.get("prompt") or "").strip(),
                "occurrences": r.get("occurrences", 1),
            })
        return pd.DataFrame(rows)
    
    @staticmethod
    def render_editable_table(df: pd.DataFrame, key: str, height: int = 600) -> pd.DataFrame:
        """Render an editable data table with inline editing enabled.

        Returns:
            edited_dataframe: The dataframe with any user edits applied
        """
        edited_df = st.data_editor(
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
                    help="Select rows to move to discards",
                    default=False,
                ),
                "prompt": st.column_config.TextColumn(
                    "Prompt",
                    help="Click to edit this text directly in the table",
                    width="large",
                    disabled=False,
                ),
                "preview": st.column_config.TextColumn(
                    "Preview",
                    help="LLM-generated preview with placeholders filled in",
                    width="large",
                    disabled=True,
                ),
                "occurrences": st.column_config.NumberColumn(
                    "Occurrences",
                    disabled=True,
                    help="Number of times this item has been encountered",
                ),
                "craziness": st.column_config.NumberColumn(
                    "Craziness",
                    help="Level of craziness (1-4)",
                    min_value=1,
                    max_value=4,
                    step=1,
                    format="%d",
                    disabled=False,
                ),
                "isSexual": st.column_config.CheckboxColumn(
                    "Sexual",
                    help="Whether the prompt has sexual content",
                    disabled=False,
                ),
                "madeFor": st.column_config.TextColumn(
                    "Made For",
                    help="Target audience (boys/girls/both)",
                    disabled=False,
                ),
            },
        )

        return edited_df
    
    @staticmethod
    def render_readonly_table(df: pd.DataFrame, height: int = 400, title: str = "Data") -> None:
        """Render a read-only data table."""
        st.dataframe(
            df,
            width="stretch",
            height=height,
            hide_index=True,
            column_config={
                "prompt": st.column_config.TextColumn("Prompt"),
                "occurrences": st.column_config.NumberColumn(
                    "Occurrences",
                    help="Number of times this item was processed"
                ),
            },
        )
    
    @staticmethod
    def render_discard_button(disabled: bool = False) -> bool:
        """Render discard button and return True if clicked."""
        col, _ = st.columns([1, 5])
        with col:
            return st.button(
                "Move to discards",
                key="database_discard_button",
                type="secondary",
                width="stretch",
                disabled=disabled,
            )
    
    @staticmethod
    def get_selected_items(df: pd.DataFrame, records: List[Dict[str, Any]]) -> List[str]:
        """Extract prompt values from selected rows."""
        try:
            selected_mask = df["selected"] == True  # noqa: E712
            selected_indices = df[selected_mask].index.tolist()
            selected_prompts: List[str] = []
            for idx in selected_indices:
                if idx < len(records):
                    prompt_val = str(records[idx].get("prompt") or "").strip()
                    if prompt_val:
                        selected_prompts.append(prompt_val)
            return selected_prompts
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
            prompt_text = str(item.get("prompt") or "").strip()
            st.text(prompt_text or "(empty)")
        
        st.markdown("---")
        return discarded
    
    @staticmethod
    def render_fetch_next_button() -> bool:
        """Render fetch next button and return True if clicked."""
        return st.button("Fetch Next 5 Items (Keep rest)", width="stretch", type="primary")
