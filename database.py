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
from config import getAptJsonPath, getBucketName, getDatabaseObjectName, getUserSelectionObjectName
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
        print(f"🔍 DEBUG _load_json: Starting for {self._objectName}")
        if not self._initialized:
            print("🔍 DEBUG _load_json: Not initialized, initializing...")
            await self.initialize()
            print("🔍 DEBUG _load_json: Initialization completed")
        assert self._client is not None
        assert self._bucketName is not None
        assert self._objectName is not None

        print(f"🔍 DEBUG _load_json: About to download from {self._bucketName}/{self._objectName}")
        try:
            # Download from GCS
            content, generation = downloadTextFile(self._client, self._bucketName, self._objectName)
            self._currentGeneration = generation
            print(f"🔍 DEBUG _load_json: Downloaded content length: {len(content)}")

            if not content.strip():
                print("🔍 DEBUG _load_json: Content is empty, returning []")
                return []

            try:
                data = json.loads(content)
                print(f"🔍 DEBUG _load_json: Parsed JSON type: {type(data)}")
                if not isinstance(data, list):
                    print(f"🔍 DEBUG _load_json: Data is not a list: {type(data)}, returning []")
                    return []
                print(f"🔍 DEBUG _load_json: Returning {len(data)} items")
                return data
            except Exception as json_error:
                print(f"❌ DEBUG _load_json: JSON parsing error: {json_error}, returning []")
                return []
        except Exception as e:
            # Handle 404 error - file doesn't exist, create empty file
            if "404" in str(e) or "No such object" in str(e):
                print(f"🔍 DEBUG _load_json: {self._objectName} doesn't exist (404), creating empty file")
                await self._save_json([])
                return []
            else:
                # Re-raise other errors
                print(f"❌ DEBUG _load_json: Critical GCS error: {str(e)}")
                raise

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
        print("🔍 DEBUG get_user_selection_count: Starting...")
        data = await self._load_json()
        count = len(data)
        print(f"🔍 DEBUG get_user_selection_count: Returning count = {count}")
        return count



    async def pop_user_selection_item(self) -> Optional[Dict[str, Any]]:
        """Remove and return one item from user selection."""
        async with self._lock:
            data = await self._load_json()
            if not data:
                return None

            item = data.pop(0)
            await self._save_json(data)
            return item


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
                normalizedValue = str(item.get('normalized') or '')
                # Prevent duplicates if concurrent writers added in the meantime
                if normalizedValue:
                    candidateKey = build_dedup_key(normalizedValue)
                    # Compute against current data without requiring full cache refresh
                    exists = False
                    for d in data:
                        existingNorm = str(d.get('normalized') or '')
                        if build_dedup_key(existingNorm) == candidateKey:
                            exists = True
                            break
                    if exists:
                        # Update caches and return early
                        self._normalizedCache.add(normalizedValue)
                        self._dedupCache.add(candidateKey)
                        await self._refresh_cache()
                        return
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
    """High-level facade combining global DB and shared user selection store."""

    def __init__(self) -> None:
        self.userSelection = UserSelectionStore()
        self.globalStore = GlobalDatabaseStore()

    async def exists_in_database(self, normalized: str) -> bool:
        return await self.globalStore.exists_in_database(normalized)

    async def add_to_user_selection(self, item: Dict[str, Any]) -> None:
        await self.userSelection.add_to_user_selection(item)

    async def add_to_global_database(self, item: Dict[str, Any]) -> None:
        await self.globalStore.add_to_database(item)

    async def remove_from_global_database_by_normalized(self, normalizedValues: List[str]) -> int:
        return await self.globalStore.remove_from_database_by_normalized(normalizedValues)



    async def pop_user_selection_item(self) -> Optional[Dict[str, Any]]:
        return await self.userSelection.pop_user_selection_item()