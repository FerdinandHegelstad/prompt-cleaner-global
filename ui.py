#!ui.py
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd  # type: ignore
import streamlit as st  # type: ignore
import matplotlib.pyplot as plt  # type: ignore
import numpy as np  # type: ignore

try:
    from cloud_storage import downloadJson, downloadTextFile, getStorageClient, loadCredentialsFromAptJson, uploadTextFile
    from config import getAptJsonPath, getBucketName, getDatabaseObjectName, getRawStrippedObjectName
    from database import DatabaseManager
    from probability_sampler import analyze_prompt_lengths, get_distribution_curve, load_length_statistics
    from remove_lines import remove_lines_containing
except ImportError as e:
    st.error(f"Import Error: {e}")
    st.error("This might be due to missing dependencies or module loading issues.")
    st.stop()


# -----------------------------
# Global Database (GCS-backed)
# -----------------------------
def loadGlobalDatabase() -> List[Dict[str, Any]]:
    bucketName = getBucketName()
    objectName = getDatabaseObjectName()
    aptJsonPath = getAptJsonPath()
    credentials = loadCredentialsFromAptJson(aptJsonPath)
    client = getStorageClient(credentials)
    data, _generation = downloadJson(client, bucketName, objectName)
    if not isinstance(data, list):
        return []
    return data


def toCleanedDataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    cleanedValues = [str(r.get("cleaned") or "").strip() for r in records]
    cleanedValues = [v for v in cleanedValues if v]
    return pd.DataFrame({"cleaned": cleanedValues})


def toEditorDataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Build a dataframe with a selectable column for editing/deletion.

    Columns: selected (bool), cleaned
    """
    rows: List[Dict[str, Any]] = []
    for r in records:
        rows.append(
            {
                "selected": False,
                "cleaned": str(r.get("cleaned") or "").strip(),
            }
        )
    df = pd.DataFrame(rows)
    # Ensure expected order
    return df[["selected", "cleaned"]] if not df.empty else df


# -----------------------------
# Local User Selection helpers
# -----------------------------
USER_SELECTION_FILE = "USER_SELECTION.json"
PREFETCH_LOCK_FILE = ".prefetcher.lock"


def _getProjectPaths() -> Dict[str, Path]:
    base = Path(__file__).resolve().parent
    return {
        "base": base,
        "userSelection": base / USER_SELECTION_FILE,
        "rawStripped": base / "raw_stripped.txt",
        "workflow": base / "workflow.py",
        "lock": base / PREFETCH_LOCK_FILE,
    }


def popOneUserSelectionItem() -> Optional[Dict[str, Any]]:
    """Remove and return one item from GCS-based user selection.

    Returns None if empty or error. Uses DatabaseManager for GCS operations.
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        db = DatabaseManager()
        item = loop.run_until_complete(db.pop_user_selection_item())
        return item
    except Exception:
        return None


def ensureSessionItemLoaded() -> None:
    """Ensure there is a current item in session state for review.

    Removes one item from local selection if needed and stores it for the user
    to decide Keep/Remove without additional loads.
    """
    if "currentSelectionItem" in st.session_state and st.session_state.currentSelectionItem is not None:
        return

    # Handle async call properly for Streamlit
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        db = DatabaseManager()
        item = loop.run_until_complete(db.pop_user_selection_item())
        st.session_state.currentSelectionItem = item
    except Exception as e:
        st.error(f"Error loading selection item: {e}")
        st.session_state.currentSelectionItem = None


def _countUserSelection() -> int:
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        db = DatabaseManager()
        count = loop.run_until_complete(db.get_user_selection_count())
        return count
    except Exception:
        return 0


def triggerTopUpIfLow(targetCapacity: int = 10, threshold: int = 7) -> None:
    """If USER_SELECTION.json has fewer than `threshold` items, ensure a background
    prefetcher process is running to top up to `targetCapacity`.

    Prefetcher is responsible for lock creation/removal; we only check for lock existence
    to avoid spawning duplicates.
    """
    paths = _getProjectPaths()
    count = _countUserSelection()
    if count >= threshold:
        return

    # Fire-and-forget subprocess running the workflow to top up
    prefetcherPath = paths["base"] / "prefetcher.py"
    if not prefetcherPath.exists():
        return

    # If a prefetcher is already running (lock present), do nothing
    if paths["lock"].exists():
        return

    try:
        # Set environment variable for the subprocess
        env = os.environ.copy()
        env['GCS_BUCKET'] = os.environ.get('GCS_BUCKET', 'unfiltered_database')

        subprocess.Popen(
            [sys.executable, str(prefetcherPath), str(targetCapacity)],
            cwd=str(paths["base"]),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
        )
    except Exception:
        pass


# -----------------------------
# Streamlit UI
# -----------------------------
def main() -> None:
    st.set_page_config(page_title="Prompt Cleaner UI", layout="wide")
    st.title("Prompt Cleaner")

    tabGlobal, tabUserSelection, tabDistribution, tabRawStripped = st.tabs(["Global Database", "User Selection", "Prompt Distribution", "Raw File Management"])  # top-level tabs

    # --- Global Database Tab ---
    with tabGlobal:
        st.subheader("Global Database: Cleaned Entries")
        st.caption("Loads from Google Cloud Storage only when you click Load.")
        colA, colB = st.columns([1, 6])
        with colA:
            loadClicked = st.button("Load", use_container_width=True)
            refreshClicked = st.button("Refresh", use_container_width=True)
        if refreshClicked:
            # Soft refresh: reload current records if any were previously loaded
            try:
                st.session_state.global_records = loadGlobalDatabase()
                st.success("Refreshed.")
            except Exception as e:
                st.error(f"Failed to refresh global database: {e}")
            st.rerun()
        if loadClicked:
            try:
                st.session_state.global_records = loadGlobalDatabase()
            except Exception as e:
                st.error(f"Failed to load global database: {e}")

        records: Optional[List[Dict[str, Any]]] = st.session_state.get("global_records")  # type: ignore
        if records is None:
            st.info("Click Load to view the global database.")
        else:
            df_editor = toEditorDataframe(records)
            if df_editor.empty:
                st.info("No entries found in the global database.")
            else:
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
                    deleteClicked = st.button(
                        "Delete selected",
                        type="secondary",
                        use_container_width=True,
                        disabled=bool(st.session_state.get("isWriting")),
                    )

                if deleteClicked:
                    # Compute selected normalized values from original records
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
                                removed = asyncio.run(
                                    db.remove_from_global_database_by_normalized(selected_normalized)
                                )
                            st.session_state.isWriting = False
                            # Reload from remote to reflect the latest state
                            st.session_state.global_records = loadGlobalDatabase()
                            st.success(f"Deleted {removed} item(s) from the global database.")
                            st.rerun()
                        except Exception as e:
                            st.session_state.isWriting = False
                            st.error(f"Failed to delete selected rows: {str(e)}")

    # --- User Selection Tab ---
    with tabUserSelection:
        st.subheader("User Selection")
        st.caption(
            "Presents one locally selected item at a time without loading the global database."
        )

        # Check for Cloud DB availability and show status
        try:
            # Test Cloud DB connection by trying to load a small amount of data
            test_records = loadGlobalDatabase()
            cloud_db_status = "✅ Cloud DB Available"
        except Exception as e:
            cloud_db_status = f"❌ Cloud DB Unavailable: {str(e)}"
            st.error(f"**Cloud Database Error**: {str(e)}")
            st.error("The system cannot check for duplicates or add new items until the Cloud DB is available.")
            st.error("Please check your internet connection and Cloud DB configuration.")

        st.info(cloud_db_status)

        # Only trigger prefetch if Cloud DB is available
        if "❌" not in cloud_db_status:
            # Manual prefetch button for cloud environment
            col_prefetch, _ = st.columns([2, 3])
            with col_prefetch:
                if st.button("🔄 Populate User Selection", use_container_width=True):
                    try:
                        # Run prefetcher manually
                        import subprocess
                        env = os.environ.copy()
                        env['GCS_BUCKET'] = os.environ.get('GCS_BUCKET', 'unfiltered_database')

                        result = subprocess.run([
                            'python3', '-m', 'prefetcher', '5'
                        ], cwd=os.path.dirname(__file__),
                        env=env, capture_output=True, text=True, timeout=30)

                        if result.returncode == 0:
                            st.success("✅ Successfully populated User Selection!")
                            st.rerun()
                        else:
                            st.error(f"❌ Prefetch failed: {result.stderr}")
                    except Exception as e:
                        st.error(f"❌ Error running prefetch: {e}")

            # Also try automatic prefetch (may not work in cloud)
            triggerTopUpIfLow(targetCapacity=10)
        else:
            st.warning("Prefetching disabled due to Cloud DB unavailability.")

        ensureSessionItemLoaded()
        item: Optional[Dict[str, Any]] = st.session_state.get("currentSelectionItem")

        if not item:
            st.success("No items waiting for review in USER_SELECTION.json")
            return

        cleanedText = str(item.get("cleaned") or "").strip()
        defaultText = str(item.get("default") or "").strip()
        normalizedText = str(item.get("normalized") or "").strip()

        # Busy flag to block UI during DB writes
        if "isWriting" not in st.session_state:
            st.session_state.isWriting = False

        st.write("Cleaned")
        st.code(cleanedText or "(empty)")
        with st.expander("Details", expanded=False):
            st.write("Default")
            st.code(defaultText or "(empty)")
            st.write("Normalized")
            st.code(normalizedText or "(empty)")

        colKeep, colRemove = st.columns(2)
        with colKeep:
            keepClicked = st.button(
                "Keep — Add to Global DB",
                type="primary",
                use_container_width=True,
                disabled=bool(st.session_state.isWriting),
            )
        with colRemove:
            removeClicked = st.button(
                "Remove — Discard",
                use_container_width=True,
                disabled=bool(st.session_state.isWriting),
            )

        if keepClicked:
            try:
                st.session_state.isWriting = True
                with st.spinner("Writing to Cloud DB…"):
                    db = DatabaseManager()
                    asyncio.run(db.add_to_global_database({
                        "default": defaultText,
                        "cleaned": cleanedText,
                        "normalized": normalizedText,
                    }))
                st.session_state.isWriting = False
                st.session_state.currentSelectionItem = None
                st.success("Added to global database.")
                st.rerun()
            except Exception as e:
                st.session_state.isWriting = False
                st.error(f"**Failed to add to global database**: {str(e)}")
                st.error("This could be due to:")
                st.error("- Network connectivity issues")
                st.error("- Cloud DB configuration problems")
                st.error("- Duplicate entry (already exists in database)")
                st.error("Please check your connection and try again.")

        if removeClicked:
            st.session_state.currentSelectionItem = None
            st.info("Item discarded.")
            st.rerun()

    # --- Prompt Distribution Tab ---
    with tabDistribution:
        st.subheader("Prompt Length Distribution Analysis")
        st.caption("Shows the probability distribution used for sampling prompts by character length.")

        col1, col2 = st.columns([1, 1])

        with col1:
            if st.button("Analyze Distribution", use_container_width=True):
                with st.spinner("Analyzing prompt lengths..."):
                    try:
                        # Load or calculate statistics
                        stats = load_length_statistics()
                        if not stats:
                            stats = analyze_prompt_lengths("raw_stripped.txt")

                        st.session_state.distribution_stats = stats
                        st.success("Analysis complete!")
                    except Exception as e:
                        st.error(f"Error analyzing distribution: {e}")

        with col2:
            if st.button("Refresh Statistics", use_container_width=True):
                with st.spinner("Recalculating statistics..."):
                    try:
                        stats = analyze_prompt_lengths("raw_stripped.txt")
                        st.session_state.distribution_stats = stats
                        st.success("Statistics refreshed!")
                    except Exception as e:
                        st.error(f"Error refreshing statistics: {e}")

        # Display statistics
        if 'distribution_stats' in st.session_state:
            stats = st.session_state.distribution_stats

            st.markdown("### Statistics")
            col_stats1, col_stats2, col_stats3, col_stats4, col_stats5 = st.columns(5)

            with col_stats1:
                st.metric("Total Prompts", f"{int(stats['count']):,}")
            with col_stats2:
                st.metric("Average Length", f"{stats['mean']:.1f} chars")
            with col_stats3:
                st.metric("Std Deviation", f"{stats['std']:.1f} chars")
            with col_stats4:
                st.metric("Min Length", f"{stats['min']} chars")
            with col_stats5:
                st.metric("Max Length", f"{stats['max']} chars")

            # Generate and display the distribution curve
            st.markdown("### Probability Distribution Curve")

            # Create the plot
            curve_points = get_distribution_curve(
                mean=stats['mean'],
                std=stats['std'],
                min_length=int(stats['min']),
                max_length=int(stats['max'])
            )

            # Convert to numpy arrays for plotting
            x_values = [point[0] for point in curve_points]
            y_values = [point[1] for point in curve_points]

            # Create the plot
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(x_values, y_values, 'b-', linewidth=3, label='Custom Curve Probability')
            ax.fill_between(x_values, y_values, alpha=0.4, color='blue')

            # Add vertical line at mean (peak)
            ax.axvline(x=stats['mean'], color='red', linestyle='--', linewidth=2,
                      label=f'Peak ({stats["mean"]:.1f})')

            # Add vertical line at target (200 chars)
            ax.axvline(x=200, color='green', linestyle=':', linewidth=2,
                      label='Target (200 chars)')

            # Add vertical line at 100 chars for reference
            ax.axvline(x=100, color='purple', linestyle=':', linewidth=1,
                      label='Reference (100 chars)')

            ax.set_xlabel('Prompt Length (characters)')
            ax.set_ylabel('Probability Density')
            ax.set_title('Custom Probability Distribution (Peaks at Mean, Gradually Decreases)')
            ax.legend()
            ax.grid(True, alpha=0.3)

            # Set better x-axis limits to show the gradual decline
            ax.set_xlim(0, 250)

            # Display the plot
            st.pyplot(fig)

            # Add explanation
            st.markdown("""
            ### How the Custom Curve Sampling Works

            The custom probability distribution is designed to create a "circle shaped curve from top" that gradually decreases:

            - **Peak probability**: Maximum probability at the mean (29.7 characters) - this is where selection peaks
            - **Steep initial drop**: Sharp decline for prompts much shorter than the mean
            - **Gradual decline**: Smooth, gradual decrease from the peak towards 200 characters
            - **Target landing**: The curve approaches zero around 200 characters as requested

            ### Key Features:

            - 🎯 **Peaks at mean**: Highest selection probability for prompts around 29.7 characters
            - 📉 **Gradual decline**: Unlike the symmetric bell curve, this gradually decreases to the right
            - 🎨 **Custom shape**: Designed to create the "circle shaped curve from top" effect you wanted
            - 🎲 **Weighted sampling**: Each prompt gets a weight based on its position on this custom curve

            The algorithm calculates a weight for each prompt based on its position on this custom curve, then uses weighted random sampling without replacement to select items.
            """)

    # --- Raw File Management Tab ---
    with tabRawStripped:
        st.subheader("Raw Stripped File Management")
        st.caption("Remove lines from raw_strippped.txt in cloud storage that contain specific words/phrases.")

        # Input field for parameters
        st.markdown("### Enter words/phrases to remove")
        st.caption("Enter words or phrases separated by commas. Lines containing these (as whole words) will be removed.")

        # Text input for removal parameters
        removal_params = st.text_input(
            "Words/phrases to remove:",
            placeholder="e.g., spam, advertisement, promotional",
            help="Enter comma-separated words or phrases to remove lines containing them"
        )

        # Display current status
        try:
            bucket_name = getBucketName()
            object_name = getRawStrippedObjectName()
            st.info(f"📁 Target file: `{object_name}` in bucket `{bucket_name}`")
        except Exception as e:
            st.error(f"❌ Configuration error: {e}")
            st.stop()

        col1, col2 = st.columns([2, 1])

        with col1:
            # Button to execute removal
            run_button = st.button(
                "🔧 Remove Lines",
                type="primary",
                use_container_width=True,
                disabled=not removal_params.strip()
            )

        with col2:
            # Button to view current file info
            info_button = st.button(
                "📊 File Info",
                use_container_width=True
            )

        # Handle file info request
        if info_button:
            try:
                with st.spinner("Loading file information..."):
                    apt_json_path = getAptJsonPath()
                    credentials = loadCredentialsFromAptJson(apt_json_path)
                    client = getStorageClient(credentials)
                    bucket_name = getBucketName()
                    object_name = getRawStrippedObjectName()

                    # Download the file to get info
                    content, generation = downloadTextFile(client, bucket_name, object_name)

                    if content:
                        lines = content.split('\n')
                        total_lines = len(lines)
                        non_empty_lines = len([line for line in lines if line.strip()])

                        st.success("✅ File information loaded!")
                        col_info1, col_info2, col_info3 = st.columns(3)
                        with col_info1:
                            st.metric("Total Lines", f"{total_lines:,}")
                        with col_info2:
                            st.metric("Non-empty Lines", f"{non_empty_lines:,}")
                        with col_info3:
                            st.metric("File Size", f"{len(content):,} bytes")
                    else:
                        st.warning("⚠️ File is empty or doesn't exist")

            except Exception as e:
                st.error(f"❌ Error loading file information: {e}")

        # Handle removal execution
        if run_button and removal_params.strip():
            try:
                # Parse the input parameters
                params = [param.strip() for param in removal_params.split(',') if param.strip()]

                if not params:
                    st.warning("⚠️ No valid parameters provided")
                else:
                    with st.spinner("Processing file..."):
                        # Get cloud storage configuration
                        apt_json_path = getAptJsonPath()
                        credentials = loadCredentialsFromAptJson(apt_json_path)
                        client = getStorageClient(credentials)
                        bucket_name = getBucketName()
                        object_name = getRawStrippedObjectName()

                        # Download the file
                        st.text("📥 Downloading file from cloud...")
                        content, generation = downloadTextFile(client, bucket_name, object_name)

                        if not content:
                            st.warning("⚠️ File is empty or doesn't exist")
                        else:
                            # Save content to temporary file for processing
                            import tempfile
                            import os

                            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as temp_file:
                                temp_file.write(content)
                                temp_file_path = temp_file.name

                            try:
                                # Count original lines
                                original_lines = len(content.split('\n'))

                                # Apply removal function
                                st.text(f"🔧 Removing lines containing: {', '.join(params)}")
                                remove_lines_containing(temp_file_path, params)

                                # Read the processed content
                                with open(temp_file_path, 'r', encoding='utf-8') as f:
                                    processed_content = f.read()

                                # Count remaining lines
                                remaining_lines = len(processed_content.split('\n'))
                                removed_lines = original_lines - remaining_lines

                                # Upload back to cloud
                                st.text("📤 Uploading modified file to cloud...")
                                uploadTextFile(client, bucket_name, object_name, processed_content, generation)

                                st.success(f"✅ Successfully processed file!")
                                st.info(f"📊 Lines removed: {removed_lines:,} | Lines remaining: {remaining_lines:,}")

                                # Show preview of changes
                                with st.expander("🔍 Preview Changes", expanded=False):
                                    st.markdown("**Parameters removed:**")
                                    for param in params:
                                        st.markdown(f"- `{param}`")

                                    if removed_lines > 0:
                                        st.markdown(f"**Summary:** Removed {removed_lines:,} lines containing the specified words/phrases")
                                    else:
                                        st.info("No lines were removed - no matches found")

                            finally:
                                # Clean up temporary file
                                os.unlink(temp_file_path)

            except Exception as e:
                st.error(f"❌ Error processing file: {e}")
                st.error("Please check your cloud storage configuration and try again.")

        # Additional help section
        with st.expander("ℹ️ How it works", expanded=False):
            st.markdown("""
            ### How Line Removal Works

            1. **Download**: The raw_strippped.txt file is downloaded from Google Cloud Storage
            2. **Process**: Lines containing any of the specified words/phrases are removed
            3. **Upload**: The modified file is uploaded back to cloud storage

            ### Matching Rules

            - **Case-insensitive**: 'Spam' matches 'SPAM', 'spam', 'SpAm', etc.
            - **Whole words only**: 'test' matches 'test' but not 'testing' or 'attest'
            - **Multiple parameters**: You can specify multiple words/phrases separated by commas
            - **Exact removal**: Only lines containing the specified terms are removed

            ### Example Usage

            If you enter: `spam, advertisement, promotional`

            - ✅ Removes: "This is spam content"
            - ✅ Removes: "Check out this advertisement"
            - ✅ Removes: "Promotional material here"
            - ❌ Keeps: "This is a normal message"
            """)


if __name__ == "__main__":
    main()


