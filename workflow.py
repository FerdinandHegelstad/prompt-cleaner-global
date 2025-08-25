#!workflow.py
import asyncio
import os
import random
import sys
from pathlib import Path
from typing import List

from database import DatabaseManager
from analyse_item import Item
from llm import call_llm
from text_utils import normalize
from probability_sampler import analyze_prompt_lengths, probabilistic_sample, load_length_statistics
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

            item_objs = [Item(default=s) for s in items]
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
        """Selects items using probability-based sampling and removes them from the GCS stripped file.

        Uses normal distribution based on prompt length statistics to favor items
        with lengths closer to the mean (bell curve sampling).

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
            # Load or calculate length statistics
            stats = load_length_statistics()
            if not stats:
                # Fallback: calculate statistics from in-memory content
                lengths = [len(s) for s in items if s]
                if lengths:
                    import statistics
                    stats = {
                        'count': len(lengths),
                        'mean': statistics.mean(lengths),
                        'std': statistics.stdev(lengths) if len(lengths) > 1 else 1.0,
                        'min': min(lengths),
                        'max': max(lengths)
                    }
                else:
                    stats = {'count': 0, 'mean': 1.0, 'std': 1.0, 'min': 0, 'max': 0}

            # Use probability-based sampling
            selected_items = probabilistic_sample(
                items=items,
                n=self.x,
                mean=stats['mean'],
                std=stats['std']
            )

            # Remove selected items from the original list
            remaining = [item for item in items if item not in selected_items]

        # Upload updated content to GCS
        updated_content = '\n'.join(remaining) + '\n'
        uploadTextFile(self.client, self.bucket_name, self.object_name, updated_content, self.current_generation)

        return selected_items

    async def _process_items(self, item_objs: List[Item]) -> List[dict]:
        """Processes items concurrently and returns results.

        Args:
            item_objs: List of Item objects.

        Returns:
            List of result dictionaries with success status.
        """
        results = await asyncio.gather(*[self._process_single_item(item) for item in item_objs])
        return results

    async def _process_single_item(self, item: Item) -> dict:
        """Processes a single item: cleans, normalizes, checks db, adds if unique.

        This function will attempt LLM processing with better error handling.
        If LLM fails, it will skip the item instead of crashing the entire workflow.

        Args:
            item: The Item to process.

        Returns:
            Dictionary with success status and message.
        """
        try:
            # Call LLM - this will either succeed or raise an exception
            cleaned = await call_llm(item.default)

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

            item.cleaned = cleaned
            normalized = normalize(cleaned).strip()
            if not normalized:
                return {"success": False, "message": "Normalization resulted in empty string"}

            item.normalized = normalized

            # Always check for duplicates against Cloud DB
            try:
                exists = await self.db_manager.exists_in_database(item.normalized) # type: ignore
            except Exception as e:
                # Log but don't crash - continue with processing
                print(f"Duplicate check error: {e}")
                exists = False

            if not exists:
                # Add locally for user review only. The UI's Keep action will
                # perform the append to the global database explicitly.
                await self.db_manager.add_to_user_selection(item.to_dict())
                return {"success": True, "message": "Item processed and added to selection"}
            else:
                return {"success": True, "message": "Item skipped (duplicate)"}

        except Exception as e:
            # Log the error but continue processing other items
            error_msg = f"Error processing item '{item.default[:50]}...': {e}"
            print(f"Workflow error: {error_msg}")
            return {"success": False, "message": error_msg}

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python workflow.py <stripped_file> <X>")
        sys.exit(1)
    stripped_file = sys.argv[1]
    x = int(sys.argv[2])
    workflow = Workflow(stripped_file, x)
    asyncio.run(workflow.run())