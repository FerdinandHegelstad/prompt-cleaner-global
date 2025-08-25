#!ui.py
import os

# Bootstrap secrets to environment variables at app start
# This must happen before importing modules that read config at import time
try:
    import streamlit as st # type: ignore
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
import json
from typing import Any, Dict, List, Optional

import pandas as pd  # type: ignore
import streamlit as st  # type: ignore

# --- External modules (your project) ---
print("DEBUG: Starting imports...")
try:
    print("DEBUG: Importing cloud_storage...")
    from cloud_storage import (
        downloadJson,
        downloadTextFile,
        getStorageClient,
        loadCredentialsFromAptJson,
    )
    print("DEBUG: cloud_storage imported successfully")

    print("DEBUG: Importing config...")
    from config import (
        getAptJsonPath,
        getBucketName,
        getDatabaseObjectName,
        getRawStrippedObjectName,
        getUserSelectionObjectName,
    )
    print("DEBUG: config imported successfully")

    print("DEBUG: Importing database...")
    from database import DatabaseManager
    print("DEBUG: database imported successfully")

    print("DEBUG: Importing workflow...")
    from workflow import Workflow
    print("DEBUG: workflow imported successfully")

    print("DEBUG: All imports completed successfully!")
except ImportError as e:
    print(f"DEBUG: Import Error: {e}")
    import streamlit as st
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
    try:
        bucket_name = getBucketName()
        object_name = getDatabaseObjectName()
        apt_json_path = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(apt_json_path)
        client = getStorageClient(credentials)
        data, _generation = downloadJson(client, bucket_name, object_name)
        if not isinstance(data, list):
            print(f"DEBUG: DATABASE.json content is not a list: {type(data)}")
            return []
        return data
    except Exception as e:
        print(f"DEBUG: Failed to load global database: {str(e)}")
        raise Exception(f"Database Access Error: {str(e)}")


def load_user_selection() -> List[Dict[str, Any]]:
    """Load all user selection items from USER_SELECTION.json for preview."""
    try:
        bucket_name = getBucketName()
        object_name = getUserSelectionObjectName()
        apt_json_path = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(apt_json_path)
        client = getStorageClient(credentials)

        try:
            # Use downloadTextFile since USER_SELECTION.json is stored as text
            content, _generation = downloadTextFile(client, bucket_name, object_name)

            if not content.strip():
                print(f"DEBUG: USER_SELECTION.json is empty or has no content")
                return []

            try:
                data = json.loads(content)
                if not isinstance(data, list):
                    print(f"DEBUG: USER_SELECTION.json content is not a list: {type(data)}")
                    return []
                return data
            except json.JSONDecodeError as json_error:
                print(f"DEBUG: Failed to parse USER_SELECTION.json: {json_error}")
                return []
        except Exception as e:
            # Handle 404 error - file doesn't exist
            if "404" in str(e) or "No such object" in str(e):
                print("DEBUG: USER_SELECTION.json doesn't exist yet, returning empty list")
                return []
            else:
                # Re-raise other errors to show them in UI
                print(f"DEBUG: GCS access error: {str(e)}")
                raise Exception(f"GCS Access Error: {str(e)}")

    except Exception as e:
        # Re-raise configuration errors to show in UI
        raise Exception(f"Configuration Error: {str(e)}")


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


def to_user_selection_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Build a dataframe for user selection preview.

    Columns: cleaned, normalized, default (preview only, no selection)
    """
    rows: List[Dict[str, Any]] = []
    for r in records:
        rows.append({
            "cleaned": str(r.get("cleaned") or "").strip(),
            "normalized": str(r.get("normalized") or "").strip(),
            "default": str(r.get("default") or "").strip(),
        })
    df = pd.DataFrame(rows)
    return df if not df.empty else pd.DataFrame(columns=["cleaned", "normalized", "default"])





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
    try:
        db = get_cached_db_manager()
        items = []

        # Check if we can get user selection count
        count = 0
        try:
            count = run_async(db.userSelection.get_user_selection_count())
        except Exception as count_error:
            print(f"Error getting queue count: {count_error}")
            # Don't raise - continue with count=0

        # Auto-populate if queue is low
        if count < 20:
            try:
                run_async(auto_populate_user_selection_if_needed())
            except Exception as populate_error:
                print(f"Auto-populate error: {populate_error}")
                # Don't raise - continue without populating

        # Fetch items

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
                        else:
                            break
                    except Exception as item_error:
                        print(f"Fetch error for item {i}: {item_error}")
                        break
                return fetched_items

            # Run the async function once
            items = run_async(fetch_multiple_items())

        except Exception as fetch_error:
            print(f"Fetch error: {fetch_error}")
            items = []
            # Don't raise - return empty list to let UI handle it

        return items
    except Exception as e:
        print(f"Batch fetch error: {e}")
        return []


# -----------------------------
# Streamlit UI
# -----------------------------

def main() -> None:
    # DEBUG: Basic startup check
    st.info("🚀 **DEBUG**: App started successfully!")

    st.set_page_config(page_title="Prompt Cleaner UI", layout="wide")
    st.title("Prompt Cleaner")

    # DEBUG: Tab creation check
    st.info("📑 **DEBUG**: Creating tabs...")
    tab_selection, tab_data = st.tabs([
        "Selection",
        "Data"
    ])
    st.info("✅ **DEBUG**: Tabs created successfully!")

    # --- Selection Tab ---
    with tab_selection:
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
            with st.spinner("Loading batch items..."):
                try:
                    items = fetch_batch_items(5)
                    if items:
                        st.session_state.batch_items = items
                        st.session_state.batch_id += 1  # Set initial batch_id
                    else:
                        st.warning("No items found in user selection queue. The queue might be empty or there might be a configuration issue.")
                        return
                except Exception as e:
                    st.error(f"Failed to load batch items: {str(e)}")
                    st.info("This might be due to missing GCS credentials or configuration. Check your Streamlit Cloud secrets setup.")
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
                # Process discards and keep only non-discarded items
                if st.session_state.batch_items:
                    try:
                        db = get_cached_db_manager()
                        # Only keep items that are not marked for discard
                        kept_count = 0
                        for i, item in enumerate(st.session_state.batch_items):
                            discard_key = f"discard_{i}"
                            if discard_key not in st.session_state.discard_actions:
                                # Item not discarded, keep it
                                try:
                                    run_async(db.add_to_global_database(item))
                                    kept_count += 1
                                except Exception as e:
                                    print(f"Keep error: {e}")
                    except Exception as e:
                        print(f"Database error: {e}")

                # Clear and fetch new items
                st.session_state.batch_items = []
                st.session_state.discard_actions.clear()

                # Fetch new items
                try:
                    items = fetch_batch_items(5)
                    if items:
                        st.session_state.batch_items = items
                        st.session_state.batch_id += 1  # Increment batch_id for unique checkbox keys
                except Exception as e:
                    print(f"Fetch error: {e}")

                # Force rerun to update UI
                st.rerun()

    # --- Data Tab ---
    with tab_data:
        # DEBUG: Data tab check
        st.info("📊 **DEBUG**: Data tab rendered successfully!")

        # User Selection Preview Section
        st.subheader("User Selection")

        colA, colB = st.columns([1, 6])
        with colA:
            load_user_clicked = st.button("Load", use_container_width=True)
        if load_user_clicked:
            with st.spinner("Loading user selection..."):
                # DEBUG: Show configuration info directly in UI
                st.info("🔍 **DEBUG INFO**: Checking GCS configuration...")
                try:
                    bucket_name = getBucketName()
                    st.info(f"✅ Bucket name: {bucket_name}")
                except Exception as config_error:
                    st.error(f"❌ GCS_BUCKET configuration error: {str(config_error)}")
                    st.stop()

                try:
                    credentials = loadCredentialsFromAptJson(getAptJsonPath())
                    st.info("✅ Credentials loaded successfully")
                except Exception as cred_error:
                    st.error(f"❌ Credentials error: {str(cred_error)}")
                    st.stop()

                try:
                    st.session_state.user_selection_records = load_user_selection()
                    if not st.session_state.user_selection_records:
                        st.warning("User selection queue is empty. The USER_SELECTION.json file either doesn't exist or contains no data.")
                        st.info("This is normal if no items have been processed yet. Try running the Selection tab first to process some items.")
                    else:
                        st.success(f"Successfully loaded {len(st.session_state.user_selection_records)} items from user selection queue.")
                except Exception as e:
                    error_msg = str(e)
                    st.error(f"Failed to load user selection: {error_msg}")

                    # Provide specific guidance based on error type
                    if "Configuration Error" in error_msg:
                        st.error("❌ **Configuration Issue**: Check your Streamlit Cloud secrets setup")
                        st.info("**Required secrets:** GCS_BUCKET, and either APT.json file or GCS service account credentials")
                    elif "GCS Access Error" in error_msg:
                        st.error("❌ **GCS Access Issue**: Your credentials may not have proper permissions")
                        st.info("**Check:** Service account has 'Storage Object Viewer' permission on the bucket")
                    elif "404" in error_msg or "No such object" in error_msg:
                        st.warning("ℹ️ **File Not Found**: USER_SELECTION.json doesn't exist yet")
                        st.info("**Solution:** Run the Selection tab first to create and populate the file")
                    else:
                        st.info("**General Fix:** Check your GCS credentials and bucket configuration in Streamlit Cloud secrets")

        user_selection_records: Optional[List[Dict[str, Any]]] = st.session_state.get("user_selection_records")  # type: ignore
        if user_selection_records is None:
            st.info("Click 'Load' to view the user selection queue.")
        else:
            user_df = to_user_selection_dataframe(user_selection_records)
            if user_df.empty:
                st.info("No items found in user selection queue.")
            else:
                total_user_items = len(user_selection_records)
                st.info(f"**Total items in User Selection Queue:** {total_user_items:,}")

                # Display the dataframe with all columns
                st.data_editor(
                    user_df,
                    key="user_selection_preview",
                    use_container_width=True,
                    height=600,  # Fixed height
                    num_rows="fixed",
                    disabled=True,  # Read-only preview
                    hide_index=True,
                )

        st.markdown("---")

        st.subheader("Global database")
        colA, colB = st.columns([1, 6])
        with colA:
            load_global_clicked = st.button("Load Database", use_container_width=True)
        if load_global_clicked:
            with st.spinner("Loading global database..."):
                # DEBUG: Show configuration info directly in UI
                st.info("🔍 **DEBUG INFO**: Checking GCS configuration...")
                try:
                    bucket_name = getBucketName()
                    st.info(f"✅ Bucket name: {bucket_name}")
                except Exception as config_error:
                    st.error(f"❌ GCS_BUCKET configuration error: {str(config_error)}")
                    st.stop()

                try:
                    credentials = loadCredentialsFromAptJson(getAptJsonPath())
                    st.info("✅ Credentials loaded successfully")
                except Exception as cred_error:
                    st.error(f"❌ Credentials error: {str(cred_error)}")
                    st.stop()

                try:
                    st.session_state.global_records = load_global_database()
                    if not st.session_state.global_records:
                        st.warning("Global database is empty. The DATABASE.json file either doesn't exist or contains no approved items.")
                        st.info("This is normal if no items have been approved yet. Use the Selection tab to review and approve items.")
                    else:
                        st.success(f"Successfully loaded {len(st.session_state.global_records)} items from global database.")
                except Exception as e:
                    error_msg = str(e)
                    st.error(f"Failed to load global database: {error_msg}")

                    # Provide specific guidance based on error type
                    if "Configuration Error" in error_msg:
                        st.error("❌ **Configuration Issue**: Check your Streamlit Cloud secrets setup")
                        st.info("**Required secrets:** GCS_BUCKET, and either APT.json file or GCS service account credentials")
                    elif "Database Access Error" in error_msg:
                        st.error("❌ **GCS Access Issue**: Your credentials may not have proper permissions")
                        st.info("**Check:** Service account has 'Storage Object Viewer' permission on the bucket")
                    elif "404" in error_msg or "No such object" in error_msg:
                        st.warning("ℹ️ **File Not Found**: DATABASE.json doesn't exist yet")
                        st.info("**Solution:** Use the Selection tab to review and approve items, which will create the database")
                    else:
                        st.info("**General Fix:** Check your GCS credentials and bucket configuration in Streamlit Cloud secrets")

        records: Optional[List[Dict[str, Any]]] = st.session_state.get("global_records")  # type: ignore
        if records is None:
            st.info("Click 'Load Database' to view the global database.")
        else:
            df_editor = to_editor_dataframe(records)
            if df_editor.empty:
                st.info("No entries found in the global database.")
            else:
                total_items = len(records)
                st.info(f" **Total items in Global Database:** {total_items:,}")

                edited_df = st.data_editor(
                    df_editor,
                    key="global_db_editor",
                    use_container_width=True,
                    height=600,
                    num_rows="fixed",
                    disabled=False,
                    hide_index=True,
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

if __name__ == "__main__":
    main()