"""Data service for loading and managing cloud storage data."""

import asyncio
from typing import Any, Dict, List, Optional

from pandas.core.frame import console

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
    getDiscardsObjectName,
    getRawStrippedObjectName,
    getUserSelectionObjectName,
)
from database import DatabaseManager
from workflow import Workflow


def run_async(coro):
    """Run an async coroutine safely from Streamlit."""
    try:
        # Check if there's already a running loop
        loop = asyncio.get_running_loop()
        # If we're already in an async context, we need to create a task
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    except RuntimeError:
        # No running loop, safe to use asyncio.run
        return asyncio.run(coro)


class DataService:
    """Centralized data operations service."""
    
    @staticmethod
    def load_json_from_storage(object_name: str) -> List[Dict[str, Any]]:
        """Generic function to load JSON data from cloud storage."""
        try:
            bucket_name = getBucketName()
            apt_json_path = getAptJsonPath()
            credentials = loadCredentialsFromAptJson(apt_json_path)
            client = getStorageClient(credentials)
            data, _generation = downloadJson(client, bucket_name, object_name)
            if not isinstance(data, list):
                return []
            return data
        except Exception:
            return []

    @staticmethod
    def load_global_database() -> List[Dict[str, Any]]:
        """Load global database from cloud storage."""
        return DataService.load_json_from_storage(getDatabaseObjectName())

    @staticmethod
    def load_discards() -> List[Dict[str, Any]]:
        """Load discards from cloud storage."""
        return DataService.load_json_from_storage(getDiscardsObjectName())

    @staticmethod
    def load_user_selection() -> List[Dict[str, Any]]:
        """Load user selection from cloud storage."""
        return DataService.load_json_from_storage(getUserSelectionObjectName())

    @staticmethod
    def get_raw_file_count() -> tuple[int, str]:
        """Get raw file line count and status message."""
        try:
            bucket_name = getBucketName()
            object_name = getRawStrippedObjectName()
            apt_json_path = getAptJsonPath()
            credentials = loadCredentialsFromAptJson(apt_json_path)
            client = getStorageClient(credentials)
            content, _ = downloadTextFile(client, bucket_name, object_name)
            if content:
                lines = content.split('\n')
                return len(lines), f"{len(lines):,}"
            else:
                return 0, "Empty or missing"
        except Exception:
            return 0, "Error"

    @staticmethod
    def load_all_data() -> Dict[str, List[Dict[str, Any]]]:
        """Load all data sources at once."""
        return {
            "global_records": DataService.load_global_database(),
            "discards_records": DataService.load_discards(),
            "user_selection_records": DataService.load_user_selection(),
        }


class SelectionService:
    """Service for managing user selection workflow."""
    
    def __init__(self):
        self._cached_db = None
    
    def get_cached_db_manager(self) -> DatabaseManager:
        """Cache the database manager to avoid recreation."""
        print("DEBUG: Getting cached db manager")
        if self._cached_db is None:
            print("DEBUG: Creating new db manager")
            self._cached_db = DatabaseManager()
        print("DEBUG: Returning cached db manager")
        return self._cached_db

    async def auto_populate_user_selection_if_needed(self) -> None:
        """Automatically populate USER_SELECTION queue from raw_stripped.txt if needed."""
        try:
            db = self.get_cached_db_manager()

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

            # Return workflow result for UI handling
            return workflow_result

        except Exception:
            return None

    def fetch_batch_items(self, batch_size: int = 5) -> List[Dict[str, Any]]:
        """Fetch multiple items from USER_SELECTION for batch review."""
        try:
            print("DEBUG: Fetching batch items")
            db = self.get_cached_db_manager()
            print("DEBUG: Got cached db manager")
            items = []

            # Check current queue count
            count = 0
            print("DEBUG: Getting user selection count")
            try:
                print("DEBUG: Running async get_user_selection_count")
                count = run_async(db.userSelection.get_user_selection_count())
            except Exception:
                print("DEBUG: Error getting user selection count")
                pass

            # Auto-populate if below threshold
            target_queue_size = 50
            populate_threshold = 20
            if count < populate_threshold:
                try:
                    run_async(self.auto_populate_user_selection_if_needed())
                except Exception:
                    pass

            print("DEBUG: Fetching items")
            # Fetch items
            for i in range(batch_size):
                try:
                    item = run_async(db.pop_user_selection_item())
                    if item:
                        items.append(item)
                    else:
                        break
                except Exception:
                    break

            return items
        except Exception:
            return []

    def process_batch_items(self, items: List[Dict[str, Any]], discard_actions: set) -> tuple[int, int]:
        """Process batch items: keep non-discarded, discard selected ones."""
        try:
            db = self.get_cached_db_manager()
            kept_count = 0
            discarded_count = 0
            
            for i, item in enumerate(items):
                discard_key = f"discard_{i}"
                if discard_key not in discard_actions:
                    # Keep item
                    try:
                        run_async(db.add_to_global_database(item))
                        kept_count += 1
                    except Exception:
                        pass
                else:
                    # Discard item
                    try:
                        run_async(db.add_to_discards(item))
                        discarded_count += 1
                    except Exception:
                        pass
            
            return kept_count, discarded_count
        except Exception:
            return 0, 0
