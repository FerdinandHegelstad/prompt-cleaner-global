#!ui.py
import os

# Bootstrap secrets to environment variables at app start
# This must happen before importing modules that read config at import time
try:
    import streamlit as st
    xai = st.secrets.get("xai", {})
    # Accept both nested [xai] and flat keys
    key   = xai.get("XAI_API_KEY") or st.secrets.get("XAI_API_KEY")
    url   = xai.get("BASE_URL")    or st.secrets.get("XAI_BASE_URL", "https://api.x.ai/v1")
    model = xai.get("MODEL")       or st.secrets.get("XAI_MODEL", "grok-3-mini")

    if key:   os.environ.setdefault("XAI_API_KEY", key)
    if url:   os.environ.setdefault("XAI_BASE_URL", str(url))
    if model: os.environ.setdefault("XAI_MODEL", str(model))
except Exception:
    # If not running under Streamlit, st.secrets won't exist - that's fine.
    pass

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


@st.cache_resource
def get_cached_db_manager():
    """Cache the database manager to avoid recreation."""
    return DatabaseManager()



async def auto_populate_user_selection_if_needed() -> None:
    """Automatically populate USER_SELECTION queue from raw_stripped.txt if needed."""
    try:
        db = get_cached_db_manager()

        # Check if USER_SELECTION queue needs more items
        target_queue_size = 50
        queue_count = await db.userSelection.get_user_selection_count()

        if queue_count >= target_queue_size:
            return

        items_needed = target_queue_size - queue_count

        # Check if raw_stripped.txt has content
        bucket_name = getBucketName()
        object_name = getRawStrippedObjectName()
        apt_json_path = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(apt_json_path)
        client = getStorageClient(credentials)
        content, _ = downloadTextFile(client, bucket_name, object_name)

        if not content or not content.strip():
            return

        lines = content.split('\n')
        non_empty_lines = len([ln for ln in lines if ln.strip()])

        if non_empty_lines == 0:
            return

        # Create and run workflow
        items_to_process = min(items_needed, non_empty_lines)
        workflow = Workflow(object_name, items_to_process)
        workflow_result = await workflow.run()

        # Check if workflow had issues
        if workflow_result["status"] == "all_failed":
            st.session_state.workflow_error = f"⚠️ **LLM Processing Failed**: {workflow_result['message']}"
            return
        elif workflow_result["status"] == "error":
            st.session_state.workflow_error = f"⚠️ **Workflow Error**: {workflow_result['message']}"
            return
        elif workflow_result["failed"] > 0:
            st.session_state.workflow_warning = f"⚠️ **Partial Success**: {workflow_result['message']}"

    except Exception as e:
        pass

def fetch_batch_items(batch_size: int = 5) -> List[Dict[str, Any]]:
    """Fetch multiple items from USER_SELECTION for batch review."""
    print(f"DEBUG: fetch_batch_items called with batch_size={batch_size}")
    try:
        db = get_cached_db_manager()
        print("DEBUG: Got cached database manager")
        items = []

        # Check if we can get user selection count
        count = 0
        try:
            count = run_async(db.userSelection.get_user_selection_count())
            print(f"DEBUG: Current queue count: {count}")
        except Exception as count_error:
            print(f"DEBUG: Error getting queue count: {count_error}")

        # If queue is below threshold, try to auto-populate it
        target_queue_size = 50
        populate_threshold = 20
        if count < populate_threshold:
            print(f"DEBUG: Queue below threshold ({count} < {populate_threshold}), calling auto_populate to reach {target_queue_size}")
            try:
                run_async(auto_populate_user_selection_if_needed())
                print("DEBUG: auto_populate completed")
            except Exception as populate_error:
                print(f"DEBUG: Error in auto_populate: {populate_error}")

        print(f"DEBUG: Attempting to fetch {batch_size} items")
        
        # FIX: Fetch all items in a single async operation instead of multiple calls
        try:
            # Create a new async function to fetch multiple items
            async def fetch_multiple_items():
                fetched_items = []
                for i in range(batch_size):
                    try:
                        item = await db.pop_user_selection_item()
                        if item:
                            fetched_items.append(item)
                            print(f"DEBUG: Fetched item {i+1}: {item.get('cleaned', '')[:50]}...")
                        else:
                            print(f"DEBUG: No more items available (got {len(fetched_items)} items)")
                            break
                    except Exception as item_error:
                        print(f"DEBUG: Error fetching item {i+1}: {item_error}")
                        break
                return fetched_items
            
            # Run the async function once
            items = run_async(fetch_multiple_items())
            
        except Exception as fetch_error:
            print(f"DEBUG: Error in fetch_multiple_items: {fetch_error}")
            items = []

        print(f"DEBUG: Returning {len(items)} items")
        return items
    except Exception as e:
        print(f"DEBUG: Exception in fetch_batch_items: {e}")
        return []


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
        st.subheader("Batch Review")

        # Initialize session state variables (completely local, no network calls)
        if "batch_items" not in st.session_state:
            st.session_state.batch_items = []
        if "discard_actions" not in st.session_state:
            st.session_state.discard_actions = set()
        if "batch_id" not in st.session_state:
            st.session_state.batch_id = 0

        # Fetch items if none exist
        if not st.session_state.batch_items:
            print("DEBUG: No batch items, calling fetch_batch_items")
            with st.spinner("Loading batch items..."):
                items = fetch_batch_items(5)
                print(f"DEBUG: Initial fetch returned {len(items) if items else 0} items")
                if items:
                    st.session_state.batch_items = items
                    st.session_state.batch_id += 1  # Set initial batch_id
                    print(f"DEBUG: Set initial batch items, batch_id: {st.session_state.batch_id}")
                else:
                    print("DEBUG: No initial items, returning")
                    return

        # Display batch items
        if st.session_state.batch_items:
            # Display all items (don't process discards until Fetch Next is clicked)
            for i, item in enumerate(st.session_state.batch_items):
                col1, col2 = st.columns([1, 6])

                with col1:
                    discard_key = f"discard_{i}"
                    # Use a checkbox instead of button to avoid rerun delays
                    # Use batch_id in key to ensure checkboxes reset for new batches
                    checkbox_key = f"discard_check_{st.session_state.batch_id}_{i}"
                    if st.checkbox("Discard", key=checkbox_key):
                        st.session_state.discard_actions.add(discard_key)

                with col2:
                    cleaned_text = str(item.get("cleaned") or "").strip()
                    st.text(cleaned_text or "(empty)")

                st.markdown("---")

            # Fetch next button
            if st.button("Fetch Next 5 Items (Keep rest)", use_container_width=True, type="primary"):
                print("DEBUG: Fetch Next button clicked!")

                # Process discards and keep only non-discarded items
                if st.session_state.batch_items:
                    print(f"DEBUG: Processing {len(st.session_state.batch_items)} items")
                    try:
                        db = get_cached_db_manager()
                        print("DEBUG: Got database manager")
                        # Only keep items that are not marked for discard
                        kept_count = 0
                        for i, item in enumerate(st.session_state.batch_items):
                            discard_key = f"discard_{i}"
                            if discard_key not in st.session_state.discard_actions:
                                # Item not discarded, keep it
                                try:
                                    run_async(db.add_to_global_database(item))
                                    kept_count += 1
                                    print(f"DEBUG: Kept item {i}")
                                except Exception as e:
                                    print(f"DEBUG: Failed to keep item {i}: {e}")
                            else:
                                print(f"DEBUG: Discarded item {i}")
                    except Exception as e:
                        print(f"DEBUG: Database manager error: {e}")

                # Clear and fetch new items
                print("DEBUG: Clearing current batch")
                st.session_state.batch_items = []
                st.session_state.discard_actions.clear()

                # Fetch new items
                print("DEBUG: Fetching new items...")
                try:
                    items = fetch_batch_items(5)
                    print(f"DEBUG: fetch_batch_items returned {len(items) if items else 0} items")
                    if items:
                        st.session_state.batch_items = items
                        st.session_state.batch_id += 1  # Increment batch_id for unique checkbox keys
                        print(f"DEBUG: Set new batch items with cleared discard actions, new batch_id: {st.session_state.batch_id}")
                    else:
                        print("DEBUG: No items returned from fetch_batch_items")
                except Exception as e:
                    print(f"DEBUG: Error fetching new items: {e}")

                # Force rerun to update UI
                st.rerun()



if __name__ == "__main__":
    main()