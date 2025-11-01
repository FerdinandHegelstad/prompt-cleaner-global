"""Input tab UI implementation for managing raw_stripped.txt and REMOVE_LINES.txt files."""

import streamlit as st
import tempfile
import os
from typing import List

from cloud_storage import loadCredentialsFromAptJson, getStorageClient, downloadTextFile, uploadTextFile
from config import getBucketName, getAptJsonPath, getRawStrippedObjectName, getRemoveLinesObjectName
from text_utils import strip_file
from ui.components.common import UIHelpers


class InputTab:
    """Input tab functionality for file management."""
    
    def __init__(self):
        self.ui_helpers = UIHelpers()
    
    def render(self) -> None:
        """Render the complete input tab."""
        
        # Section 1: Add to dataset input
        self._render_upload_section()
        
        st.divider()
        
        # Section 2: Remove lines containing
        self._render_remove_lines_section()
    
    def _render_upload_section(self) -> None:
        """Render the file upload section."""
        st.subheader("Add raw content to unsorted dataset")
        
        uploaded_file = st.file_uploader(
            "Upload a file",
            type=['txt']
            )
        
        if uploaded_file is not None:
            if st.button("Add to dataset", type="primary"):
                self._handle_file_upload(uploaded_file)
    
    def _render_remove_lines_section(self) -> None:
        """Render the remove lines section."""
        st.subheader("Remove lines containing")
        
        # Load current remove list
        remove_strings = self._load_remove_strings()
        
        # Display current remove strings
        if remove_strings:
            for i, remove_string in enumerate(remove_strings):
                st.text(f"{remove_string}")
        
        # Add new remove string
        col1, col2 = st.columns([3, 1])
        with col1:
            new_string = st.text_input(
                "Add line"
            )
        with col2:
            st.write("")  # Spacing
            if st.button("Add"):
                if new_string.strip():
                    self._add_remove_string(new_string.strip())
                    st.rerun()
                else:
                    st.warning("Please enter a string to add.")
        
        # Remove lines button
        if remove_strings:
            if st.button("Remove lines containing these strings", type="secondary"):
                self._handle_remove_lines(remove_strings)
    
    def _handle_file_upload(self, uploaded_file) -> None:
        """Handle the uploaded file processing and appending."""
        try:
            with self.ui_helpers.with_spinner("Processing and uploading file..."):
                # Save uploaded file to temporary location
                with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as temp_file:
                    content = uploaded_file.read().decode('utf-8')
                    temp_file.write(content)
                    temp_file_path = temp_file.name
                
                try:
                    # Strip the file using text_utils
                    stripped_file_path = strip_file(temp_file_path)
                    
                    # Read the stripped content
                    with open(stripped_file_path, 'r', encoding='utf-8') as f:
                        stripped_content = f.read().strip()
                    
                    if not stripped_content:
                        st.warning("No content remaining after text stripping.")
                        return
                    
                    # Download current raw_stripped.txt from cloud
                    credentials = loadCredentialsFromAptJson(getAptJsonPath())
                    client = getStorageClient(credentials)
                    bucket_name = getBucketName()
                    object_name = getRawStrippedObjectName()

                    current_content, generation = downloadTextFile(client, bucket_name, object_name)

                    # Split content into lines for duplicate checking
                    existing_lines = set()
                    if current_content:
                        existing_lines = set(line.strip() for line in current_content.split('\n') if line.strip())

                    # Split new content into lines and filter out duplicates
                    new_lines_list = [line.strip() for line in stripped_content.split('\n') if line.strip()]
                    unique_new_lines = [line for line in new_lines_list if line not in existing_lines]

                    # Only add content if there are new unique lines
                    if unique_new_lines:
                        new_unique_content = '\n'.join(unique_new_lines)
                        if current_content:
                            updated_content = current_content + "\n" + new_unique_content
                        else:
                            updated_content = new_unique_content

                        # Upload back to cloud
                        uploadTextFile(client, bucket_name, object_name, updated_content, generation)

                        # Count new lines added
                        new_lines = len(unique_new_lines)
                        duplicates_filtered = len(new_lines_list) - new_lines

                        # After adding new content, apply remove lines logic if configured
                        remove_stats = self._apply_remove_lines_logic_after_upload(client, bucket_name, object_name)

                        # Show comprehensive success message
                        self._show_comprehensive_upload_stats(new_lines, duplicates_filtered, remove_stats)
                    else:
                        duplicates_filtered = len(new_lines_list)
                        # Even if no new lines were added, we should still show remove stats if applicable
                        remove_strings = self._load_remove_strings()
                        remove_stats = {'removed_count': 0, 'remaining_count': 0} if not remove_strings else {'removed_count': 0, 'remaining_count': 0}
                        self._show_comprehensive_upload_stats(0, duplicates_filtered, remove_stats)
                    
                finally:
                    # Clean up temporary files
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
                    if os.path.exists(stripped_file_path):
                        os.unlink(stripped_file_path)
                        
        except Exception as e:
            self.ui_helpers.show_error_message(f"Failed to process file: {str(e)}")
    
    def _load_remove_strings(self) -> List[str]:
        """Load remove strings from cloud storage (REMOVE_LINES.txt).
        
        If cloud file doesn't exist and local remove.txt exists, migrates local to cloud.
        """
        try:
            credentials = loadCredentialsFromAptJson(getAptJsonPath())
            client = getStorageClient(credentials)
            bucket_name = getBucketName()
            object_name = getRemoveLinesObjectName()
            
            content, generation = downloadTextFile(client, bucket_name, object_name)
            
            # If cloud file doesn't exist, try to migrate from local file
            if not content:
                local_file_path = "remove.txt"
                if os.path.exists(local_file_path):
                    try:
                        with open(local_file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        if content:
                            # Upload to cloud
                            uploadTextFile(client, bucket_name, object_name, content, generation)
                            lines = [line.strip() for line in content.split('\n') if line.strip()]
                            return lines
                    except Exception as e:
                        print(f"Warning: Failed to migrate local remove.txt to cloud: {e}")
                return []
            
            lines = [line.strip() for line in content.split('\n') if line.strip()]
            return lines
        except Exception:
            return []
    
    def _add_remove_string(self, new_string: str) -> None:
        """Add a new string to the REMOVE_LINES.txt file in cloud storage."""
        try:
            remove_strings = self._load_remove_strings()
            if new_string not in remove_strings:
                remove_strings.append(new_string)
                
                # Upload updated list to cloud storage
                credentials = loadCredentialsFromAptJson(getAptJsonPath())
                client = getStorageClient(credentials)
                bucket_name = getBucketName()
                object_name = getRemoveLinesObjectName()
                
                # Download to get generation for optimistic concurrency
                _, generation = downloadTextFile(client, bucket_name, object_name)
                
                # Upload updated content
                updated_content = '\n'.join(remove_strings) + '\n'
                uploadTextFile(client, bucket_name, object_name, updated_content, generation)
            else:
                st.warning("String already exists in remove list.")
        except Exception as e:
            self.ui_helpers.show_error_message(f"Failed to add string: {str(e)}")
    
    def _handle_remove_lines(self, remove_strings: List[str]) -> None:
        """Remove lines containing any of the remove strings from raw_stripped.txt."""
        try:
            with self.ui_helpers.with_spinner("Removing lines from raw_stripped.txt..."):
                # Download current raw_stripped.txt from cloud
                credentials = loadCredentialsFromAptJson(getAptJsonPath())
                client = getStorageClient(credentials)
                bucket_name = getBucketName()
                object_name = getRawStrippedObjectName()
                
                current_content, generation = downloadTextFile(client, bucket_name, object_name)
                
                if not current_content:
                    st.warning("raw_stripped.txt is empty or doesn't exist.")
                    return
                
                # Process lines
                lines = current_content.split('\n')
                original_count = len(lines)
                kept_lines = []
                
                for line in lines:
                    should_remove = False
                    line_lower = line.lower()
                    line_stripped = line.strip()

                    # Check if any remove string is contained in the line (case-insensitive)
                    # OR if the line is identical to any remove string (case-insensitive)
                    for remove_string in remove_strings:
                        remove_string_lower = remove_string.lower()
                        remove_string_stripped = remove_string.strip()

                        # Remove if: 1) contains the string, or 2) is identical to the string
                        if (remove_string_lower in line_lower or
                            line_stripped.lower() == remove_string_stripped.lower()):
                            should_remove = True
                            break

                    if not should_remove:
                        kept_lines.append(line)
                
                # Upload updated content back to cloud
                updated_content = '\n'.join(kept_lines)
                uploadTextFile(client, bucket_name, object_name, updated_content, generation)
                
                removed_count = original_count - len(kept_lines)
                self.ui_helpers.show_success_message(
                    f"Removed {removed_count} lines from raw_stripped.txt ({len(kept_lines)} lines remaining)"
                )
                
        except Exception as e:
            self.ui_helpers.show_error_message(f"Failed to remove lines: {str(e)}")

    def _apply_remove_lines_logic_after_upload(self, client, bucket_name: str, object_name: str) -> dict:
        """Apply remove lines logic after upload and return stats."""
        try:
            # Get remove strings
            remove_strings = self._load_remove_strings()
            if not remove_strings:
                return {'removed_count': 0, 'remaining_count': 0}

            # Download current content
            current_content, generation = downloadTextFile(client, bucket_name, object_name)

            if not current_content:
                return {'removed_count': 0, 'remaining_count': 0}

            # Process lines (same logic as _handle_remove_lines)
            lines = current_content.split('\n')
            original_count = len(lines)
            kept_lines = []

            for line in lines:
                should_remove = False
                line_lower = line.lower()
                line_stripped = line.strip()

                # Check if any remove string is contained in the line (case-insensitive)
                # OR if the line is identical to any remove string (case-insensitive)
                for remove_string in remove_strings:
                    remove_string_lower = remove_string.lower()
                    remove_string_stripped = remove_string.strip()

                    # Remove if: 1) contains the string, or 2) is identical to the string
                    if (remove_string_lower in line_lower or
                        line_stripped.lower() == remove_string_stripped.lower()):
                        should_remove = True
                        break

                if not should_remove:
                    kept_lines.append(line)

            # Upload updated content back to cloud
            updated_content = '\n'.join(kept_lines)
            uploadTextFile(client, bucket_name, object_name, updated_content, generation)

            removed_count = original_count - len(kept_lines)
            return {'removed_count': removed_count, 'remaining_count': len(kept_lines)}

        except Exception as e:
            # If remove logic fails, don't fail the entire upload - just return zero stats
            print(f"Warning: Remove lines logic failed: {str(e)}")
            return {'removed_count': 0, 'remaining_count': 0}

    def _show_comprehensive_upload_stats(self, new_lines: int, duplicates_filtered: int, remove_stats: dict) -> None:
        """Show comprehensive upload statistics."""
        removed_count = remove_stats.get('removed_count', 0)

        if new_lines > 0:
            message_parts = [f"Added {new_lines} new lines to dataset"]
            if duplicates_filtered > 0:
                message_parts.append(f"{duplicates_filtered} duplicate lines filtered")
            if removed_count > 0:
                message_parts.append(f"{removed_count} lines removed by filters")
            self.ui_helpers.show_success_message(". ".join(message_parts) + ".")
        else:
            if duplicates_filtered > 0:
                message = f"No new lines added. All {duplicates_filtered} lines were duplicates"
                if removed_count > 0:
                    message += f" and {removed_count} lines were removed by filters"
                self.ui_helpers.show_success_message(message + ".")
            else:
                if removed_count > 0:
                    self.ui_helpers.show_success_message(f"Upload processed. {removed_count} lines removed by filters.")
                else:
                    self.ui_helpers.show_success_message("Upload processed successfully.")
