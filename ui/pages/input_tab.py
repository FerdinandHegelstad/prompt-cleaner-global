"""Input tab UI implementation for managing raw_stripped.txt and remove.txt files."""

import streamlit as st
import tempfile
import os
from typing import List

from cloud_storage import loadCredentialsFromAptJson, getStorageClient, downloadTextFile, uploadTextFile
from config import getBucketName, getAptJsonPath, getRawStrippedObjectName
from text_utils import strip_file
from ui.components.common import UIHelpers


class InputTab:
    """Input tab functionality for file management."""
    
    def __init__(self):
        self.ui_helpers = UIHelpers()
        self.remove_file_path = "remove.txt"  # Local file
    
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
                    
                    # Append new content
                    if current_content:
                        updated_content = current_content + "\n" + stripped_content
                    else:
                        updated_content = stripped_content
                    
                    # Upload back to cloud
                    uploadTextFile(client, bucket_name, object_name, updated_content, generation)
                    
                    # Count new lines added
                    new_lines = len(stripped_content.split('\n'))
                    
                finally:
                    # Clean up temporary files
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
                    if os.path.exists(stripped_file_path):
                        os.unlink(stripped_file_path)
                        
        except Exception as e:
            self.ui_helpers.show_error_message(f"Failed to process file: {str(e)}")
    
    def _load_remove_strings(self) -> List[str]:
        """Load remove strings from local remove.txt file."""
        try:
            if os.path.exists(self.remove_file_path):
                with open(self.remove_file_path, 'r', encoding='utf-8') as f:
                    lines = [line.strip() for line in f.readlines() if line.strip()]
                    return lines
            return []
        except Exception:
            return []
    
    def _add_remove_string(self, new_string: str) -> None:
        """Add a new string to the remove.txt file."""
        try:
            remove_strings = self._load_remove_strings()
            if new_string not in remove_strings:
                remove_strings.append(new_string)
                with open(self.remove_file_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(remove_strings) + '\n')
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
                    
                    # Check if any remove string is contained in the line (case-insensitive)
                    for remove_string in remove_strings:
                        if remove_string.lower() in line_lower:
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
