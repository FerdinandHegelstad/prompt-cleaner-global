"""Workflow for ingesting raw prompts: sample from GCS, clean via LLM, deduplicate, store."""

import asyncio
import random
import sys
from typing import List

from database import DatabaseManager
from models import Item
from llm import call_llm
from text_utils import normalize
from cloud_storage import downloadTextFile, getStorageClient, loadCredentialsFromAptJson, uploadTextFile
from config import getAptJsonPath, getBucketName, getRawStrippedObjectName

class Workflow:
    """Manages the workflow for processing items."""

    def __init__(self, stripped_file: str, x: int):
        self.stripped_file = stripped_file
        self.x = x
        self.db_manager = DatabaseManager()
        # GCS configuration
        self.bucket_name = None
        self.object_name = None
        self.client = None
        self.current_generation = None

    async def _initialize_gcs(self):
        """Initialize GCS client and configuration."""
        if self.client is not None:
            return

        self.bucket_name = getBucketName()
        self.object_name = getRawStrippedObjectName()
        apt_path = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(apt_path)
        self.client = getStorageClient(credentials)

    async def run(self) -> dict:
        """Runs the workflow asynchronously and returns status information."""
        try:
            items = await self._select_and_remove_items()
            if not items:
                return {"status": "no_items", "message": "No items to process", "processed": 0, "failed": 0}

            item_objs = [Item(raw=s) for s in items]
            results = await self._process_items(item_objs)

            successful = sum(1 for result in results if result["success"])
            failed = len(results) - successful

            if failed == len(results):
                return {"status": "all_failed", "message": "All items failed processing", "processed": successful, "failed": failed}
            elif failed > 0:
                return {"status": "partial_success", "message": f"Processed {successful} items, {failed} failed", "processed": successful, "failed": failed}
            else:
                return {"status": "success", "message": f"Successfully processed {successful} items", "processed": successful, "failed": failed}

        except Exception as e:
            return {"status": "error", "message": f"Workflow error: {str(e)}", "processed": 0, "failed": 0}

    async def _select_and_remove_items(self) -> List[str]:
        """Selects items using random sampling and removes them from the GCS stripped file.

        Returns:
            List of selected items.
        """
        await self._initialize_gcs()
        assert self.client is not None
        assert self.bucket_name is not None
        assert self.object_name is not None

        # Download content from GCS
        content, generation = downloadTextFile(self.client, self.bucket_name, self.object_name)
        self.current_generation = generation
        lines = content.splitlines()
        items = [line for line in lines if line]

        if len(items) < self.x:
            selected_items = items
            remaining = []
        else:
            # Use simple random sampling
            selected_items = random.sample(items, self.x)

            # Remove selected items from the original list
            remaining = [item for item in items if item not in selected_items]

        # Upload updated content to GCS
        updated_content = '\n'.join(remaining) + '\n'
        uploadTextFile(self.client, self.bucket_name, self.object_name, updated_content, self.current_generation)

        return selected_items

    async def _process_items(self, item_objs: List[Item]) -> List[dict]:
        """Processes items concurrently and returns results."""
        results = await asyncio.gather(*[self._process_single_item(item) for item in item_objs])
        return results

    async def _process_single_item(self, item: Item) -> dict:
        """Processes a single item: cleans via LLM, checks db, adds if unique.

        Args:
            item: The Item to process.

        Returns:
            Dictionary with success status and message.
        """
        try:
            print(f"DEBUG: Processing item: {item.raw[:50]}", flush=True)
            import sys
            sys.stdout.flush()

            # Call LLM to clean the raw text
            cleaned = await call_llm(item.raw)

            # Post-process the LLM output to enforce one non-empty line
            cleaned = cleaned.strip()
            if not cleaned:
                return {"success": False, "message": "LLM returned empty result"}

            # If multiple lines were returned, keep the first
            if "\n" in cleaned:
                cleaned = cleaned.splitlines()[0].strip()

            # Strip simple surrounding quotes
            if (cleaned.startswith('"') and cleaned.endswith('"')) or (cleaned.startswith("'") and cleaned.endswith("'")):
                cleaned = cleaned[1:-1].strip()

            item.prompt = cleaned

            # Verify normalization produces a non-empty result
            normalized = normalize(cleaned).strip()
            if not normalized:
                return {"success": False, "message": "Normalization resulted in empty string"}

            # Check for duplicates against Cloud DB, discards, and user selection
            # The DB layer internally normalizes the prompt for dedup
            try:
                exists = await self.db_manager.exists_in_database(item.prompt)
            except Exception as e:
                exists = False

            if not exists:
                # Add to user selection for human review
                await self.db_manager.add_to_user_selection(item.to_dict())
                return {"success": True, "message": "Item processed and added to selection"}
            else:
                # Item is duplicate - increment occurrence count
                try:
                    await self.db_manager.increment_occurrence_count(item.prompt)
                    print(f"⚠️  Prompt already exists: '{item.raw[:80]}'")
                    return {"success": True, "message": "Item skipped (duplicate, occurrence incremented)"}
                except Exception as e:
                    print(f"⚠️  Prompt already exists: '{item.raw[:80]}'")
                    return {"success": True, "message": "Item skipped (duplicate)"}

        except Exception as e:
            error_msg = f"Error processing item '{item.raw[:50]}': {e}"
            return {"success": False, "message": error_msg}

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python workflow.py <stripped_file> <X>")
        sys.exit(1)
    stripped_file = sys.argv[1]
    x = int(sys.argv[2])
    workflow = Workflow(stripped_file, x)
    asyncio.run(workflow.run())
