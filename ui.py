#!ui.py
import asyncio
import time
from typing import Any, Dict, List, Optional

import pandas as pd  # type: ignore
import streamlit as st  # type: ignore

# --- External modules (your project) ---
try:
    from cloud_storage import (
        downloadJson,
        downloadTextFile,
        getStorageClient,
        loadCredentialsFromAptJson,
    )
    from config import (
        getAptJsonPath,
        getBucketName,
        getDatabaseObjectName,
        getRawStrippedObjectName,
    )
    from database import DatabaseManager
    from workflow import Workflow
except ImportError as e:
    st.error(f"Import Error: {e}")
    st.error("Missing dependencies or module import failure. Make sure project files are deployed.")
    st.stop()


# -----------------------------
# Async helper
# -----------------------------

def run_async(coro):
    """Run an async coroutine safely from Streamlit.

    Streamlit runs your script synchronously; this helper guards against the
    occasional "event loop already running" case (e.g., in some hosts/tests).
    """
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            loop.close()


# -----------------------------
# Global Database (GCS-backed)
# -----------------------------

def load_global_database() -> List[Dict[str, Any]]:
    bucket_name = getBucketName()
    object_name = getDatabaseObjectName()
    apt_json_path = getAptJsonPath()
    credentials = loadCredentialsFromAptJson(apt_json_path)
    client = getStorageClient(credentials)
    data, _generation = downloadJson(client, bucket_name, object_name)
    if not isinstance(data, list):
        return []
    return data


def to_editor_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Build a dataframe with a selectable column for editing/deletion.

    Columns: selected (bool), cleaned
    """
    rows: List[Dict[str, Any]] = []
    for r in records:
        rows.append({
            "selected": False,
            "cleaned": str(r.get("cleaned") or "").strip(),
        })
    df = pd.DataFrame(rows)
    return df[["selected", "cleaned"]] if not df.empty else df


# -----------------------------
# User Selection helpers
# -----------------------------

def get_user_selection_count() -> int:
    try:
        db = DatabaseManager()
        count = run_async(db.get_user_selection_count())
        return int(count)
    except Exception:
        return 0


def top_up_user_selection(target_capacity: int = 10, threshold: int = 7) -> int:
    """Ensure the USER_SELECTION queue has at least `threshold` items, by running
    the Workflow directly (no subprocess, no lock files).

    Returns the number of items added (best effort).
    """
    try:
        current = get_user_selection_count()
        if current >= threshold:
            return 0

        deficit = target_capacity - current
        overfetch = max(deficit, int((deficit * 3.0 + 0.9999)))  # multiplier 3x

        # Check for raw_stripped.txt existence in GCS before running workflow
        bucket_name = getBucketName()
        object_name = getRawStrippedObjectName()
        apt_path = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(apt_path)
        client = getStorageClient(credentials)
        content, _ = downloadTextFile(client, bucket_name, object_name)
        if not (content and content.strip()):
            return 0

        workflow = Workflow(object_name, overfetch)
        run_async(workflow.run())

        after = get_user_selection_count()
        return max(0, after - current)
    except Exception:
        return 0


def ensure_session_item_loaded() -> None:
    """Loads one item from USER_SELECTION into session for review."""
    if "currentSelectionItem" in st.session_state and st.session_state.currentSelectionItem is not None:
        return
    try:
        db = DatabaseManager()
        item = run_async(db.pop_user_selection_item())
        st.session_state.currentSelectionItem = item
    except Exception as e:
        st.error(f"Error loading selection item: {e}")
        st.session_state.currentSelectionItem = None


# -----------------------------
# Streamlit UI
# -----------------------------

def main() -> None:
    st.set_page_config(page_title="Prompt Cleaner UI", layout="wide")
    st.title("Prompt Cleaner")

    tab_global, tab_user_selection = st.tabs([
        "Global Database",
        "User Selection"
    ])

    # --- Global Database Tab ---
    with tab_global:
        st.subheader("Global Database: Cleaned Entries")
        st.caption("Loads from Google Cloud Storage only when you click Load.")

        # Status metrics
        try:
            st.metric("User Selection Queue", f"{get_user_selection_count():,}", help="Items waiting for review")
        except Exception as e:
            st.metric("User Selection Queue", "Error", help=f"Failed to load: {e}")

        # Raw file info
        try:
            bucket_name = getBucketName()
            object_name = getRawStrippedObjectName()
            apt_json_path = getAptJsonPath()
            credentials = loadCredentialsFromAptJson(apt_json_path)
            client = getStorageClient(credentials)

            content, generation = downloadTextFile(client, bucket_name, object_name)
            if content:
                lines = content.split('\n')
                total_lines = len(lines)
                non_empty_lines = len([ln for ln in lines if ln.strip()])
                file_size = len(content)

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Raw File Lines", f"{total_lines:,}", help="Total lines in raw_stripped.txt")
                with col2:
                    st.metric("Non-empty Lines", f"{non_empty_lines:,}", help="Lines with content")
                with col3:
                    st.metric("File Size", f"{file_size:,} bytes", help="Size of raw_stripped.txt")
            else:
                st.metric("Raw File Status", "Empty or missing", help="raw_stripped.txt not found or empty")
        except Exception as e:
            st.metric("Raw File Status", "Error", help=f"Failed to load: {e}")

        st.markdown("---")

        colA, colB = st.columns([1, 6])
        with colA:
            load_clicked = st.button("Load", use_container_width=True)
        if load_clicked:
            try:
                st.session_state.global_records = load_global_database()
            except Exception as e:
                st.error(f"Failed to load global database: {e}")

        records: Optional[List[Dict[str, Any]]] = st.session_state.get("global_records")  # type: ignore
        if records is None:
            st.info("Click Load to view the global database.")
        else:
            df_editor = to_editor_dataframe(records)
            if df_editor.empty:
                st.info("No entries found in the global database.")
            else:
                total_items = len(records)
                st.info(f"📊 **Total items in Global Database:** {total_items:,}")

                edited_df = st.data_editor(
                    df_editor,
                    key="global_db_editor",
                    use_container_width=True,
                    height=600,
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
                    },
                )

                delete_col, _ = st.columns([1, 5])
                with delete_col:
                    delete_clicked = st.button(
                        "Delete selected",
                        type="secondary",
                        use_container_width=True,
                        disabled=bool(st.session_state.get("isWriting")),
                    )

                if delete_clicked:
                    try:
                        selected_mask = edited_df["selected"] == True  # noqa: E712
                        selected_indices = edited_df[selected_mask].index.tolist()
                        selected_normalized: List[str] = []
                        for idx in selected_indices:
                            if idx < len(records):
                                norm_val = str(records[idx].get("normalized") or "").strip()
                                if norm_val:
                                    selected_normalized.append(norm_val)
                    except Exception:
                        selected_normalized = []

                    if not selected_normalized:
                        st.warning("No rows selected for deletion.")
                    else:
                        try:
                            st.session_state.isWriting = True
                            with st.spinner("Deleting from Cloud DB…"):
                                db = DatabaseManager()
                                removed = run_async(
                                    db.remove_from_global_database_by_normalized(selected_normalized)
                                )
                            st.session_state.isWriting = False
                            st.session_state.global_records = load_global_database()
                            st.success(f"Deleted {removed} item(s) from the global database.")
                            st.rerun()
                        except Exception as e:
                            st.session_state.isWriting = False
                            st.error(f"Failed to delete selected rows: {str(e)}")

    # --- User Selection Tab ---
    with tab_user_selection:
        st.subheader("User Selection")
        st.caption("Presents one locally selected item at a time without loading the global database.")

        # Check Cloud DB availability
        try:
            _ = load_global_database()  # lightweight sanity check
            st.success("✅ Cloud DB Available")
            cloud_ok = True
        except Exception as e:
            cloud_ok = False
            st.error(f"❌ Cloud DB Unavailable: {e}")
            st.error("Cannot check for duplicates or add new items until Cloud DB is available.")

        # Auto top-up (best effort), only if cloud is OK
        if cloud_ok:
            try:
                added = top_up_user_selection(target_capacity=10, threshold=7)
                count_now = get_user_selection_count()
                if count_now < 7:
                    st.info(f"Low on items ({count_now}/10) - Auto-population attempted (added {added}).")
                else:
                    st.info(f"Items ready: {count_now}")
            except Exception as e:
                error_msg = str(e)
                if "LLM" in error_msg:
                    st.error(f"🤖 **LLM Processing Error**: {error_msg}")
                    st.error("The workflow requires LLM processing and cannot proceed without it.")
                    st.info("💡 **Solution**: Check your xAI API key and credits, or wait for rate limits to reset.")
                else:
                    st.error(f"❌ **Workflow Error**: {error_msg}")

            manual_col = st.columns(1)[0]
            if manual_col.button("Top up queue now", use_container_width=True):
                try:
                    with st.spinner("🤖 Processing with LLM…"):
                        added = top_up_user_selection(target_capacity=10, threshold=10)
                    st.success(f"✅ Queued {added} new item(s) via LLM processing.")
                    st.rerun()
                except Exception as e:
                    error_msg = str(e)
                    if "LLM" in error_msg or "rate limit" in error_msg:
                        st.error(f"🤖 **LLM Processing Failed**: {error_msg}")
                        st.error("Cannot proceed without LLM processing. Please check your API key and credits.")
                    else:
                        st.error(f"❌ **Processing Error**: {error_msg}")
                    st.info("🔄 Try again later or contact support if the issue persists.")

        ensure_session_item_loaded()
        item: Optional[Dict[str, Any]] = st.session_state.get("currentSelectionItem")

        if not item:
            st.success("No items waiting for review in USER_SELECTION.json")
            return

        cleaned_text = str(item.get("cleaned") or "").strip()
        default_text = str(item.get("default") or "").strip()
        normalized_text = str(item.get("normalized") or "").strip()

        if "isWriting" not in st.session_state:
            st.session_state.isWriting = False

        st.write("Cleaned")
        st.code(cleaned_text or "(empty)")
        with st.expander("Details", expanded=False):
            st.write("Default")
            st.code(default_text or "(empty)")
            st.write("Normalized")
            st.code(normalized_text or "(empty)")

        col_keep, col_remove = st.columns(2)
        with col_keep:
            keep_clicked = st.button(
                "Keep — Add to Global DB",
                type="primary",
                use_container_width=True,
                disabled=bool(st.session_state.isWriting),
            )
        with col_remove:
            remove_clicked = st.button(
                "Remove — Discard",
                type="secondary",
                use_container_width=True,
                disabled=bool(st.session_state.isWriting),
            )

        if keep_clicked:
            try:
                st.session_state.isWriting = True
                with st.spinner("Writing to Cloud DB…"):
                    db = DatabaseManager()
                    run_async(db.add_to_global_database({
                        "default": default_text,
                        "cleaned": cleaned_text,
                        "normalized": normalized_text,
                    }))
                st.session_state.isWriting = False
                st.session_state.currentSelectionItem = None
                st.success("Added to global database.")
                st.rerun()
            except Exception as e:
                st.session_state.isWriting = False
                st.error(f"Failed to add to global database: {str(e)}")
                st.error("Possible causes: network issues, Cloud DB config problems, or duplicate entry.")

        if remove_clicked:
            st.session_state.currentSelectionItem = None
            st.info("Item discarded.")
            st.rerun()

if __name__ == "__main__":
    main()