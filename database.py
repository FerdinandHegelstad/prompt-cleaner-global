"""Database layer: GCS-backed stores for prompts, user selection, and discards."""

import asyncio
import json
import time
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
from text_utils import build_dedup_key, normalize


def _prompt_dedup_key(prompt: str) -> str:
    """Compute a dedup key from a prompt string (normalize then dedup)."""
    return build_dedup_key(normalize(str(prompt or '')))


class UserSelectionStore:
    """Shared store for user selections in Google Cloud Storage.

    New format: each entry is {"prompt": "..."}.
    """

    def __init__(self) -> None:
        self._lock = None
        self._client = None
        self._bucketName: Optional[str] = None
        self._objectName: Optional[str] = None
        self._aptPath: Optional[str] = None
        self._currentGeneration: Optional[int] = None
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        self._lock = asyncio.Lock()
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

        jsonText = json.dumps(data, ensure_ascii=False, indent=4)
        uploadTextFile(
            self._client,
            self._bucketName,
            self._objectName,
            jsonText,
            self._currentGeneration
        )

    async def add_to_user_selection(self, item: Dict[str, Any]) -> None:
        """Add an item to user selection. Expects {"prompt": "..."}."""
        promptVal = (item.get('prompt') or '').strip()
        if not promptVal:
            return

        if not self._initialized:
            await self.initialize()

        async with self._lock:
            data = await self._load_json()

            # Prevent duplicates by dedup key
            candidateKey = _prompt_dedup_key(promptVal)
            if candidateKey:
                for existing in data:
                    existingPrompt = str(existing.get('prompt') or '')
                    if _prompt_dedup_key(existingPrompt) == candidateKey:
                        return

            data.append({'prompt': promptVal})
            await self._save_json(data)

    async def get_user_selection_count(self) -> int:
        """Get the count of items in user selection."""
        data = await self._load_json()
        return len(data)

    async def exists_in_user_selection(self, prompt: str) -> bool:
        """Check if an item exists in user selection using dedup key matching."""
        data = await self._load_json()
        candidateKey = _prompt_dedup_key(prompt)
        if not candidateKey:
            return False

        for existing in data:
            existingPrompt = str(existing.get('prompt') or '')
            if _prompt_dedup_key(existingPrompt) == candidateKey:
                return True
        return False

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
    """Store for discarded items in Google Cloud Storage.

    New format: each entry is {"prompt": "...", "occurrences": N}.
    """

    def __init__(self) -> None:
        self._lock = None
        self._client = None
        self._bucketName: Optional[str] = None
        self._objectName: Optional[str] = None
        self._aptPath: Optional[str] = None
        self._currentGeneration: Optional[int] = None
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        self._lock = asyncio.Lock()
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

        jsonText = json.dumps(data, ensure_ascii=False, indent=4)
        uploadTextFile(
            self._client,
            self._bucketName,
            self._objectName,
            jsonText,
            self._currentGeneration
        )

    async def add_to_discards(self, item: Dict[str, Any]) -> None:
        """Add an item to the discards store. Expects {"prompt": "...", "occurrences": N}."""
        promptVal = (item.get('prompt') or '').strip()
        if not promptVal:
            return

        if not self._initialized:
            await self.initialize()

        async with self._lock:
            data = await self._load_json()

            # Check for existing item and increment occurrences if found
            candidateKey = _prompt_dedup_key(promptVal)
            if candidateKey:
                for existing in data:
                    existingPrompt = str(existing.get('prompt') or '')
                    if _prompt_dedup_key(existingPrompt) == candidateKey:
                        existing['occurrences'] = existing.get('occurrences', 1) + 1
                        await self._save_json(data)
                        return

            # Add new item
            new_item = {
                'prompt': promptVal,
                'occurrences': item.get('occurrences', 1),
            }
            data.append(new_item)
            await self._save_json(data)

    async def exists_in_discards(self, prompt: str) -> bool:
        """Check if an item exists in discards using dedup key matching on prompt."""
        data = await self._load_json()
        candidateKey = _prompt_dedup_key(prompt)
        if not candidateKey:
            return False

        for existing in data:
            existingPrompt = str(existing.get('prompt') or '')
            if _prompt_dedup_key(existingPrompt) == candidateKey:
                return True
        return False

    async def increment_discarded_item_occurrences(self, prompt: str) -> None:
        """Increment occurrences for an existing discarded item."""
        async with self._lock:
            data = await self._load_json()
            candidateKey = _prompt_dedup_key(prompt)
            if not candidateKey:
                return

            for existing in data:
                existingPrompt = str(existing.get('prompt') or '')
                if _prompt_dedup_key(existingPrompt) == candidateKey:
                    existing['occurrences'] = existing.get('occurrences', 1) + 1
                    await self._save_json(data)
                    return


class GlobalDatabaseStore:
    """Global database backed by Google Cloud Storage with optimistic concurrency.

    New unified format: {prompt, occurrences, craziness?, isSexual?, madeFor?}.
    """

    _CACHE_TTL_SECONDS = 5.0

    def __init__(self) -> None:
        self._client = None
        self._bucketName: Optional[str] = None
        self._objectName: Optional[str] = None
        self._aptPath: Optional[str] = None
        self._dedupCache: Set[str] = set()
        self._currentGeneration: Optional[int] = None
        self._cacheTimestamp: float = 0.0
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        self._bucketName = getBucketName()
        self._objectName = getDatabaseObjectName()
        self._aptPath = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(self._aptPath)
        self._client = getStorageClient(credentials)
        await self._refresh_cache()
        self._initialized = True

    async def _refresh_cache(self, force: bool = False) -> None:
        """Rebuild the in-memory dedup cache from GCS.

        Skips the download when the cache is younger than ``_CACHE_TTL_SECONDS``
        unless *force* is True (used after writes to ensure consistency).
        """
        if not force and (time.monotonic() - self._cacheTimestamp) < self._CACHE_TTL_SECONDS:
            return

        assert self._client is not None
        assert self._bucketName is not None
        assert self._objectName is not None
        data, generation = downloadJson(self._client, self._bucketName, self._objectName)
        self._dedupCache = set()
        for item in data:
            promptVal = str(item.get('prompt') or '')
            if promptVal:
                key = _prompt_dedup_key(promptVal)
                if key:
                    self._dedupCache.add(key)
        self._currentGeneration = generation
        self._cacheTimestamp = time.monotonic()

    async def exists_in_database(self, prompt: str) -> bool:
        """Check if a prompt exists in the database (dedup key match)."""
        if not self._initialized:
            await self.initialize()
        await self._refresh_cache()
        candidateKey = _prompt_dedup_key(prompt)
        return candidateKey in self._dedupCache

    async def add_to_database(self, item: Dict[str, Any], maxRetries: int = 5) -> None:
        """Add item to database. Expects {prompt, occurrences, ...}."""
        if not self._initialized:
            await self.initialize()
        assert self._client is not None
        attempt = 0
        backoffSeconds = 0.2
        while True:
            try:
                data, generation = downloadJson(self._client, self._bucketName, self._objectName)

                promptValue = str(item.get('prompt') or '')
                # Check for duplicates and increment occurrences if found
                if promptValue:
                    candidateKey = _prompt_dedup_key(promptValue)
                    for d in data:
                        existingPrompt = str(d.get('prompt') or '')
                        if _prompt_dedup_key(existingPrompt) == candidateKey:
                            d['occurrences'] = d.get('occurrences', 1) + 1
                            uploadJsonWithPreconditions(
                                self._client,
                                self._bucketName,
                                self._objectName,
                                data,
                                generation,
                            )
                            self._dedupCache.add(candidateKey)
                            await self._refresh_cache(force=True)
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
                if promptValue:
                    candidateKey = _prompt_dedup_key(promptValue)
                    if candidateKey:
                        self._dedupCache.add(candidateKey)
                await self._refresh_cache(force=True)
                return
            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)

    async def increment_database_item_occurrences(self, prompt: str, maxRetries: int = 5) -> None:
        """Increment occurrences for an existing database item found by prompt."""
        if not self._initialized:
            await self.initialize()
        assert self._client is not None

        attempt = 0
        backoffSeconds = 0.2
        while True:
            try:
                data, generation = downloadJson(self._client, self._bucketName, self._objectName)

                candidateKey = _prompt_dedup_key(prompt)
                if not candidateKey:
                    return

                for item in data:
                    existingPrompt = str(item.get('prompt') or '')
                    if _prompt_dedup_key(existingPrompt) == candidateKey:
                        item['occurrences'] = item.get('occurrences', 1) + 1
                        uploadJsonWithPreconditions(
                            self._client,
                            self._bucketName,
                            self._objectName,
                            data,
                            generation,
                        )
                        await self._refresh_cache(force=True)
                        return

                return

            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)

    async def remove_from_database_by_prompt(self, promptValues: List[str], maxRetries: int = 5) -> int:
        """Remove items from the global DB matching the provided prompt values.

        Matching uses dedup keys computed from each prompt value.
        Returns the count of removed items.
        """
        if not self._initialized:
            await self.initialize()
        assert self._client is not None

        candidateKeys: Set[str] = set()
        for prompt in promptValues:
            promptStr = str(prompt or "").strip()
            if not promptStr:
                continue
            key = _prompt_dedup_key(promptStr)
            if key:
                candidateKeys.add(key)

        if not candidateKeys:
            return 0

        attempt = 0
        backoffSeconds = 0.2
        while True:
            try:
                data, generation = downloadJson(self._client, self._bucketName, self._objectName)
                toKeep: List[Dict[str, Any]] = []
                removedCount = 0
                for item in data:
                    promptVal = str(item.get('prompt') or '')
                    key = _prompt_dedup_key(promptVal) if promptVal else ''
                    if key and key in candidateKeys:
                        removedCount += 1
                    else:
                        toKeep.append(item)

                if removedCount == 0:
                    await self._refresh_cache(force=True)
                    return 0

                uploadJsonWithPreconditions(
                    self._client,
                    self._bucketName,
                    self._objectName,
                    toKeep,
                    generation,
                )
                await self._refresh_cache(force=True)
                return removedCount
            except Exception:
                attempt += 1
                if attempt > maxRetries:
                    raise
                await asyncio.sleep(backoffSeconds)
                backoffSeconds = min(backoffSeconds * 2, 2.0)

    async def update_item_parametrics(self, prompt: str, parametrics: Dict[str, Any], maxRetries: int = 5) -> bool:
        """Update parametric fields (craziness, isSexual, madeFor) for an entry matched by prompt.

        Returns True if the item was found and updated, False otherwise.
        """
        if not self._initialized:
            await self.initialize()
        assert self._client is not None

        candidateKey = _prompt_dedup_key(prompt)
        if not candidateKey:
            return False

        attempt = 0
        backoffSeconds = 0.2
        while True:
            try:
                data, generation = downloadJson(self._client, self._bucketName, self._objectName)

                found = False
                for item in data:
                    existingPrompt = str(item.get('prompt') or '')
                    if _prompt_dedup_key(existingPrompt) == candidateKey:
                        # Merge parametric fields
                        for key in ('craziness', 'isSexual', 'madeFor'):
                            if key in parametrics:
                                item[key] = parametrics[key]
                        found = True
                        break

                if not found:
                    return False

                uploadJsonWithPreconditions(
                    self._client,
                    self._bucketName,
                    self._objectName,
                    data,
                    generation,
                )
                await self._refresh_cache(force=True)
                return True

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

    async def exists_in_database(self, prompt: str) -> bool:
        """Check if item exists in global database, discards, or user selection."""
        exists_in_global = await self.globalStore.exists_in_database(prompt)
        exists_in_discards = await self.discardsStore.exists_in_discards(prompt)
        exists_in_user_selection = await self.userSelection.exists_in_user_selection(prompt)
        return exists_in_global or exists_in_discards or exists_in_user_selection

    async def add_to_user_selection(self, item: Dict[str, Any]) -> None:
        await self.userSelection.add_to_user_selection(item)

    async def add_to_global_database(self, item: Dict[str, Any]) -> None:
        await self.globalStore.add_to_database(item)

    async def add_to_discards(self, item: Dict[str, Any]) -> None:
        await self.discardsStore.add_to_discards(item)

    async def remove_from_global_database_by_prompt(self, promptValues: List[str]) -> int:
        return await self.globalStore.remove_from_database_by_prompt(promptValues)

    async def increment_occurrence_count(self, prompt: str) -> None:
        """Increment occurrence count for existing items in database or discards."""
        exists_in_global = await self.globalStore.exists_in_database(prompt)
        if exists_in_global:
            await self.globalStore.increment_database_item_occurrences(prompt)
        else:
            exists_in_discards = await self.discardsStore.exists_in_discards(prompt)
            if exists_in_discards:
                await self.discardsStore.increment_discarded_item_occurrences(prompt)

    async def update_item_parametrics(self, prompt: str, parametrics: Dict[str, Any]) -> bool:
        """Update parametric fields for a database entry."""
        return await self.globalStore.update_item_parametrics(prompt, parametrics)

    async def pop_user_selection_item(self) -> Optional[Dict[str, Any]]:
        return await self.userSelection.pop_user_selection_item()
