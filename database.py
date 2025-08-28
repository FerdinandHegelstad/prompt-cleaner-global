#!database.py
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from cloud_storage import (
    downloadJson,
    downloadTextFile,
    getStorageClient,
    loadCredentialsFromAptJson,
    uploadJsonWithPreconditions,
    uploadTextFile,
)
from config import getAptJsonPath, getBucketName, getDatabaseObjectName, getUserSelectionObjectName, getDiscardsObjectName
from text_utils import build_dedup_key


class UserSelectionStore:
    """Shared store for user selections in Google Cloud Storage."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._client = None
        self._bucketName: Optional[str] = None
        self._objectName: Optional[str] = None
        self._aptPath: Optional[str] = None
        self._currentGeneration: Optional[int] = None
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        # Resolve config lazily to allow local-only workflows without env set
        self._bucketName = getBucketName()
        self._objectName = getUserSelectionObjectName()
        self._aptPath = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(self._aptPath)
        self._client = getStorageClient(credentials)
        self._initialized = True

    async def _load_json(self) -> List[Dict[str, Any]]:
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        assert self._bucketName is not None
        assert self._objectName is not None

        # Download from GCS
        content, generation = downloadTextFile(self._client, self._bucketName, self._objectName)
        self._currentGeneration = generation

        if not content.strip():
            return []

        try:
            data = json.loads(content)
            if not isinstance(data, list):
                return []
            return data
        except Exception:
            return []

    async def _save_json(self, data: List[Dict[str, Any]]) -> None:
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        assert self._bucketName is not None
        assert self._objectName is not None

        # Upload to GCS with optimistic concurrency
        jsonText = json.dumps(data, ensure_ascii=False, indent=4)

        attempt = 0
        maxRetries = 5
        backoffSeconds = 0.2

        while True:
            try:
                uploadTextFile(
                    self._client,
                    self._bucketName,
                    self._objectName,
                    jsonText,
                    self._currentGeneration
                )
                return
            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)
                # Refresh data and generation
                await self._load_json()

    async def add_to_user_selection(self, item: Dict[str, Any]) -> None:
        # Guard against empty records
        defaultVal = (item.get('default') or '').strip()
        cleanedVal = (item.get('cleaned') or '').strip()
        normalizedVal = (item.get('normalized') or '').strip()
        if not defaultVal or not cleanedVal or not normalizedVal:
            return

        async with self._lock:
            data = await self._load_json()

            # Prevent duplicates by dedup key
            candidateKey = build_dedup_key(normalizedVal)
            if candidateKey:
                for existing in data:
                    existingNorm = str(existing.get('normalized') or '')
                    if build_dedup_key(existingNorm) == candidateKey:
                        return

            data.append({'default': defaultVal, 'cleaned': cleanedVal, 'normalized': normalizedVal})
            await self._save_json(data)

    async def get_user_selection_count(self) -> int:
        """Get the count of items in user selection."""
        data = await self._load_json()
        return len(data)



    async def pop_user_selection_item(self) -> Optional[Dict[str, Any]]:
        """Remove and return one item from user selection."""
        async with self._lock:
            data = await self._load_json()
            if not data:
                return None

            item = data.pop(0)
            await self._save_json(data)
            return item


class DiscardedItemsStore:
    """Store for discarded items in Google Cloud Storage."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._client = None
        self._bucketName: Optional[str] = None
        self._objectName: Optional[str] = None
        self._aptPath: Optional[str] = None
        self._currentGeneration: Optional[int] = None
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        # Resolve config lazily to allow local-only workflows without env set
        self._bucketName = getBucketName()
        self._objectName = getDiscardsObjectName()
        self._aptPath = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(self._aptPath)
        self._client = getStorageClient(credentials)
        self._initialized = True

    async def _load_json(self) -> List[Dict[str, Any]]:
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        assert self._bucketName is not None
        assert self._objectName is not None

        # Download from GCS
        content, generation = downloadTextFile(self._client, self._bucketName, self._objectName)
        self._currentGeneration = generation

        if not content.strip():
            return []

        try:
            data = json.loads(content)
            if not isinstance(data, list):
                return []
            return data
        except Exception:
            return []

    async def _save_json(self, data: List[Dict[str, Any]]) -> None:
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        assert self._bucketName is not None
        assert self._objectName is not None

        # Upload to GCS with optimistic concurrency
        jsonText = json.dumps(data, ensure_ascii=False, indent=4)

        attempt = 0
        maxRetries = 5
        backoffSeconds = 0.2

        while True:
            try:
                uploadTextFile(
                    self._client,
                    self._bucketName,
                    self._objectName,
                    jsonText,
                    self._currentGeneration
                )
                return
            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)
                # Refresh data and generation
                await self._load_json()

    async def add_to_discards(self, item: Dict[str, Any]) -> None:
        """Add an item to the discards store."""
        # Guard against empty records
        defaultVal = (item.get('default') or '').strip()
        cleanedVal = (item.get('cleaned') or '').strip()
        normalizedVal = (item.get('normalized') or '').strip()
        if not defaultVal or not cleanedVal or not normalizedVal:
            return

        async with self._lock:
            data = await self._load_json()

            # Check for existing item and increment occurrences if found
            candidateKey = build_dedup_key(normalizedVal)
            if candidateKey:
                for existing in data:
                    existingNorm = str(existing.get('normalized') or '')
                    if build_dedup_key(existingNorm) == candidateKey:
                        # Increment occurrences of existing item
                        existing['occurrences'] = existing.get('occurrences', 1) + 1
                        await self._save_json(data)
                        return

            # Add new item with occurrences field
            new_item = {
                'default': defaultVal, 
                'cleaned': cleanedVal, 
                'normalized': normalizedVal,
                'occurrences': item.get('occurrences', 1)
            }
            data.append(new_item)
            await self._save_json(data)

    async def exists_in_discards(self, normalized: str) -> bool:
        """Check if an item exists in discards using dedup key matching."""
        data = await self._load_json()
        candidateKey = build_dedup_key(str(normalized or ''))
        if not candidateKey:
            return False
        
        for existing in data:
            existingNorm = str(existing.get('normalized') or '')
            if build_dedup_key(existingNorm) == candidateKey:
                return True
        return False

    async def increment_discarded_item_occurrences(self, normalized: str) -> None:
        """Increment occurrences for an existing discarded item."""
        async with self._lock:
            data = await self._load_json()
            candidateKey = build_dedup_key(str(normalized or ''))
            if not candidateKey:
                return

            for existing in data:
                existingNorm = str(existing.get('normalized') or '')
                if build_dedup_key(existingNorm) == candidateKey:
                    existing['occurrences'] = existing.get('occurrences', 1) + 1
                    await self._save_json(data)
                    return


class GlobalDatabaseStore:
    """Global database backed by Google Cloud Storage with optimistic concurrency."""

    def __init__(self) -> None:
        self._client = None
        # Defer environment/config resolution until initialize()
        self._bucketName: Optional[str] = None
        self._objectName: Optional[str] = None
        self._aptPath: Optional[str] = None
        self._normalizedCache: Set[str] = set()
        self._dedupCache: Set[str] = set()
        self._currentGeneration: Optional[int] = None
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        # Resolve config lazily to allow local-only workflows without env set
        self._bucketName = getBucketName()
        self._objectName = getDatabaseObjectName()
        self._aptPath = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(self._aptPath)
        self._client = getStorageClient(credentials)
        await self._refresh_cache()
        self._initialized = True

    async def _refresh_cache(self) -> None:
        assert self._client is not None
        assert self._bucketName is not None
        assert self._objectName is not None
        data, generation = downloadJson(self._client, self._bucketName, self._objectName)
        self._normalizedCache = {str(item.get('normalized')) for item in data if 'normalized' in item}
        # Build dedup cache from normalized values
        self._dedupCache = set()
        for item in data:
            normVal = str(item.get('normalized') or '')
            if normVal:
                self._dedupCache.add(build_dedup_key(normVal))
        self._currentGeneration = generation

    async def exists_in_database(self, normalized: str) -> bool:
        if not self._initialized:
            await self.initialize()
        # Refresh cache to ensure we check against the latest global DB before
        # deciding whether to surface an item to the user.
        await self._refresh_cache()
        # Use dedup key comparison (including possible empty key) to ignore specified words
        candidateKey = build_dedup_key(str(normalized or ''))
        return candidateKey in self._dedupCache

    async def add_to_database(self, item: Dict[str, Any], maxRetries: int = 5) -> None:
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        attempt = 0
        backoffSeconds = 0.2
        while True:
            try:
                data, generation = downloadJson(self._client, self._bucketName, self._objectName)
                
                # Migrate existing items without occurrences field
                self._migrate_items_if_needed(data)
                
                normalizedValue = str(item.get('normalized') or '')
                # Check for duplicates and increment occurrences if found
                if normalizedValue:
                    candidateKey = build_dedup_key(normalizedValue)
                    # Compute against current data without requiring full cache refresh
                    for d in data:
                        existingNorm = str(d.get('normalized') or '')
                        if build_dedup_key(existingNorm) == candidateKey:
                            # Increment occurrences of existing item
                            d['occurrences'] = d.get('occurrences', 1) + 1
                            uploadJsonWithPreconditions(
                                self._client,
                                self._bucketName,
                                self._objectName,
                                data,
                                generation,
                            )
                            # Update caches and return early
                            self._normalizedCache.add(normalizedValue)
                            self._dedupCache.add(candidateKey)
                            await self._refresh_cache()
                            return
                
                # Ensure item has occurrences field
                if 'occurrences' not in item:
                    item['occurrences'] = 1
                    
                data.append(item)
                uploadJsonWithPreconditions(
                    self._client,
                    self._bucketName,
                    self._objectName,
                    data,
                    generation,
                )
                if normalizedValue:
                    self._normalizedCache.add(normalizedValue)
                    candidateKey = build_dedup_key(normalizedValue)
                    if candidateKey:
                        self._dedupCache.add(candidateKey)
                await self._refresh_cache()
                return
            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)

    def _migrate_items_if_needed(self, data: List[Dict[str, Any]]) -> None:
        """Add occurrences field to items that don't have it."""
        for item in data:
            if 'occurrences' not in item:
                item['occurrences'] = 1

    async def increment_database_item_occurrences(self, normalized: str, maxRetries: int = 5) -> None:
        """Increment occurrences for an existing database item."""
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        
        attempt = 0
        backoffSeconds = 0.2
        while True:
            try:
                data, generation = downloadJson(self._client, self._bucketName, self._objectName)
                
                # Migrate existing items without occurrences field
                self._migrate_items_if_needed(data)
                
                candidateKey = build_dedup_key(str(normalized or ''))
                if not candidateKey:
                    return

                # Find and increment the matching item
                for item in data:
                    existingNorm = str(item.get('normalized') or '')
                    if build_dedup_key(existingNorm) == candidateKey:
                        item['occurrences'] = item.get('occurrences', 1) + 1
                        uploadJsonWithPreconditions(
                            self._client,
                            self._bucketName,
                            self._objectName,
                            data,
                            generation,
                        )
                        await self._refresh_cache()
                        return
                
                # Item not found, nothing to increment
                return
                
            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)

    async def remove_from_database_by_normalized(self, normalizedValues: List[str], maxRetries: int = 5) -> int:
        """Remove items from the global DB matching the provided normalized values.

        Removal is based on the dedup key computed from each normalized value,
        ensuring that logically equivalent entries are removed consistently.

        Returns the count of removed items.
        """
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        # Build set of dedup keys to remove
        candidateKeys: Set[str] = set()
        for norm in normalizedValues:
            normStr = str(norm or "").strip()
            if not normStr:
                continue
            key = build_dedup_key(normStr)
            if key:
                candidateKeys.add(key)

        if not candidateKeys:
            return 0

        attempt = 0
        backoffSeconds = 0.2
        while True:
            try:
                data, generation = downloadJson(self._client, self._bucketName, self._objectName)
                # Partition items into keep/remove by dedup key
                toKeep: List[Dict[str, Any]] = []
                removedCount = 0
                for item in data:
                    normVal = str(item.get('normalized') or '')
                    key = build_dedup_key(normVal) if normVal else ''
                    if key and key in candidateKeys:
                        removedCount += 1
                    else:
                        toKeep.append(item)

                if removedCount == 0:
                    # Nothing to do
                    await self._refresh_cache()
                    return 0

                uploadJsonWithPreconditions(
                    self._client,
                    self._bucketName,
                    self._objectName,
                    toKeep,
                    generation,
                )
                # Refresh caches after successful write
                await self._refresh_cache()
                return removedCount
            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)


class DatabaseManager:
    """High-level facade combining global DB, user selection store, and discards store."""

    def __init__(self) -> None:
        self.userSelection = UserSelectionStore()
        self.globalStore = GlobalDatabaseStore()
        self.discardsStore = DiscardedItemsStore()

    async def exists_in_database(self, normalized: str) -> bool:
        """Check if item exists in either global database or discards."""
        exists_in_global = await self.globalStore.exists_in_database(normalized)
        exists_in_discards = await self.discardsStore.exists_in_discards(normalized)
        return exists_in_global or exists_in_discards

    async def add_to_user_selection(self, item: Dict[str, Any]) -> None:
        await self.userSelection.add_to_user_selection(item)

    async def add_to_global_database(self, item: Dict[str, Any]) -> None:
        await self.globalStore.add_to_database(item)

    async def add_to_discards(self, item: Dict[str, Any]) -> None:
        await self.discardsStore.add_to_discards(item)

    async def remove_from_global_database_by_normalized(self, normalizedValues: List[str]) -> int:
        return await self.globalStore.remove_from_database_by_normalized(normalizedValues)

    async def increment_occurrence_count(self, normalized: str) -> None:
        """Increment occurrence count for existing items in database or discards."""
        exists_in_global = await self.globalStore.exists_in_database(normalized)
        if exists_in_global:
            await self.globalStore.increment_database_item_occurrences(normalized)
        else:
            exists_in_discards = await self.discardsStore.exists_in_discards(normalized)
            if exists_in_discards:
                await self.discardsStore.increment_discarded_item_occurrences(normalized)

    async def pop_user_selection_item(self) -> Optional[Dict[str, Any]]:
        return await self.userSelection.pop_user_selection_item()