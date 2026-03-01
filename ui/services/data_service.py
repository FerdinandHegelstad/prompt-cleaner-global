"""Data service for loading and managing cloud storage data."""

import asyncio
from typing import Any, Dict, List, Optional

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
        loop = asyncio.get_running_loop()
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    except RuntimeError:
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
        if self._cached_db is None:
            self._cached_db = DatabaseManager()
        return self._cached_db

    async def auto_populate_user_selection_if_needed(self) -> Optional[Dict[str, Any]]:
        """Automatically populate USER_SELECTION queue from raw_stripped.txt if needed."""
        try:
            db = self.get_cached_db_manager()

            target_queue_size = 50
            queue_count = await db.userSelection.get_user_selection_count()

            if queue_count >= target_queue_size:
                return

            items_needed = target_queue_size - queue_count

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

            items_to_process = min(items_needed, non_empty_lines)
            workflow = Workflow(items_to_process)
            workflow_result = await workflow.run()

            return workflow_result

        except Exception:
            return None

    def fetch_batch_items(self, batch_size: int = 5) -> List[Dict[str, Any]]:
        """Fetch multiple items from USER_SELECTION for batch review."""
        try:
            db = self.get_cached_db_manager()
            items = []

            count = 0
            try:
                count = run_async(db.userSelection.get_user_selection_count())
            except Exception:
                pass

            target_queue_size = 50
            populate_threshold = 20
            print(f"Queue count: {count}, threshold: {populate_threshold}")
            if count < populate_threshold:
                print(f"Auto-populating user selection (queue too low: {count} < {populate_threshold})")
                try:
                    result = run_async(self.auto_populate_user_selection_if_needed())
                    if result:
                        print(f"Workflow result: {result}")
                except Exception as e:
                    print(f"Auto-populate failed: {e}")
            else:
                print(f"Queue has enough items ({count} >= {populate_threshold}), no processing needed")

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
        """Process batch items: keep non-discarded, discard selected ones.
        
        Items are in {"prompt": "..."} format. When keeping, we add with occurrences=1.
        When discarding, we add with occurrences=1.
        """
        try:
            db = self.get_cached_db_manager()
            kept_count = 0
            discarded_count = 0
            
            for i, item in enumerate(items):
                discard_key = f"discard_{i}"
                prompt_val = item.get("prompt", "")
                
                if discard_key not in discard_actions:
                    # Keep item - add to global database
                    try:
                        db_item = {"prompt": prompt_val, "occurrences": 1}
                        run_async(db.add_to_global_database(db_item))
                        kept_count += 1
                    except Exception:
                        pass
                else:
                    # Discard item
                    try:
                        discard_item = {"prompt": prompt_val, "occurrences": 1}
                        run_async(db.add_to_discards(discard_item))
                        discarded_count += 1
                    except Exception:
                        pass
            
            return kept_count, discarded_count
        except Exception:
            return 0, 0
