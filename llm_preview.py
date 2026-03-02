#!/usr/bin/env python3
"""
LLM Preview Generation Workflow

Processes database entries that have no 'preview' field by sending them
through AI_ROOM_PROMPT.txt with fixed test data, then storing the filled-in
result back into DATABASE.json.

Usage: python llm_preview.py <number_of_items>
"""

import asyncio
import json
import random
import sys
from typing import Any, Dict, List, Optional

from cloud_storage import (
    downloadJson,
    getStorageClient,
    loadCredentialsFromAptJson,
    uploadJsonWithPreconditions,
)
from config import (
    getAptJsonPath,
    getBucketName,
    getDatabaseObjectName,
    getXaiApiKey,
    getXaiBaseUrl,
    getXaiModel,
)
from database import _prompt_dedup_key
from openai import AsyncOpenAI

# ---------------------------------------------------------------------------
# Constants: fixed test data for preview generation
# ---------------------------------------------------------------------------

PLAYERS = "Ferdy, Markus, Stine, Tuva, Erik"
DRINKING_LEVEL = "velg mellom 3 - 8 slurker, og hvis oppgaven er seriøst, kan du bruke shot eller chug"
CUSTOM_INSTRUCTIONS = ""
BATCH_SIZE = 5  # matches production behaviour


def _load_system_prompt() -> str:
    """Load AI_ROOM_PROMPT.txt and fill in the static template variables."""
    with open("prompts/AI_ROOM_PROMPT.txt", "r", encoding="utf-8") as f:
        template = f.read()
    # Replace static placeholders (input_to_modify and generation_history are per-call)
    template = template.replace("{{players_array:inline}}", PLAYERS)
    template = template.replace("{{drinking_level}}", DRINKING_LEVEL)
    template = template.replace("{{custom_instructions}}", CUSTOM_INSTRUCTIONS)
    return template


def _build_user_message(prompts: List[str], history: str = "") -> str:
    """Build the user message with INPUT PROMPTS and HISTORY filled in.

    The system prompt already contains the template, but the input_to_modify
    and generation_history sections are injected via the user message so they
    can change per call.
    """
    input_block = "\n".join(prompts)
    return f"INPUT PROMPTS:\n{input_block}\nEND OF INPUT PROMPTS\n\n===\n\nHISTORY:\n{history}"


# ---------------------------------------------------------------------------
# LLM client
# ---------------------------------------------------------------------------

class PreviewLLM:
    """LLM client for generating prompt previews."""

    def __init__(self):
        self.client: Optional[AsyncOpenAI] = None
        self.system_prompt = self._build_system_prompt()
        print("\n" + "=" * 60)
        print("SYSTEM PROMPT BEING USED:")
        print("=" * 60)
        print(self.system_prompt)
        print("=" * 60 + "\n")

    @staticmethod
    def _build_system_prompt() -> str:
        """Load the prompt template and strip the per-call sections.

        Everything up to (but not including) the INPUT PROMPTS block becomes
        the system message.  The per-call parts are sent in the user message.
        """
        raw = _load_system_prompt()
        # Split at the INPUT PROMPTS marker so the system prompt is stable
        marker = "INPUT PROMPTS:"
        idx = raw.find(marker)
        if idx != -1:
            return raw[:idx].rstrip()
        return raw

    def _get_client(self) -> AsyncOpenAI:
        if self.client is None:
            api_key = getXaiApiKey()
            if not api_key:
                raise RuntimeError("XAI API key not available")
            self.client = AsyncOpenAI(api_key=api_key, base_url=getXaiBaseUrl())
        return self.client

    async def generate_previews(
        self,
        prompts: List[str],
        history: str = "",
        max_retries: int = 3,
    ) -> Optional[List[str]]:
        """Send a batch of prompts and return the filled-in lines.

        Returns a list of filled-in strings (one per input prompt) or None on
        failure.
        """
        client = self._get_client()
        model = getXaiModel()
        user_msg = _build_user_message(prompts, history)

        for attempt in range(max_retries + 1):
            try:
                temp = 0.0 if attempt == 0 else min(0.1 + (attempt * 0.1), 0.3)
                print(f"  Attempt {attempt + 1}/{max_retries + 1}, temperature={temp}")

                response = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": user_msg},
                    ],
                    temperature=temp,
                    max_tokens=2000,
                )

                text = (response.choices[0].message.content or "").strip()
                if not text:
                    print("  Empty response from LLM")
                    continue

                # Log input/output grouped together
                print("\n" + "-" * 40)
                print("INPUT:")
                for j, p in enumerate(prompts):
                    print(f"  [{j+1}] {p}")
                print("OUTPUT:")
                for j, ln in enumerate(text.splitlines()):
                    if ln.strip():
                        print(f"  [{j+1}] {ln.strip()}")
                print("-" * 40)

                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

                if len(lines) != len(prompts):
                    print(
                        f"  Line count mismatch: expected {len(prompts)}, got {len(lines)}"
                    )
                    # Over-delivery is fine: truncate and accept
                    if len(lines) >= len(prompts):
                        return lines[: len(prompts)]
                    # Under-delivery: retry if we have attempts left
                    if attempt < max_retries:
                        print(f"  Retrying due to line count mismatch …")
                        continue
                    # All retries exhausted: pad with None
                    lines.extend([None] * (len(prompts) - len(lines)))

                return lines

            except Exception as e:
                error_msg = str(e).lower()
                if "429" in error_msg or "rate limit" in error_msg:
                    wait = (2 ** attempt) * 60
                    print(f"  Rate limited. Waiting {wait}s …")
                    await asyncio.sleep(wait)
                else:
                    wait = (2 ** attempt) * 10
                    print(f"  Error: {e}. Waiting {wait}s …")
                    await asyncio.sleep(wait)

        print(f"  Failed after {max_retries + 1} attempts")
        return None


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------

class PreviewWorkflow:
    """Generates previews for database entries missing the 'preview' field."""

    def __init__(self, num_items: int):
        self.num_items = num_items
        self.llm = PreviewLLM()
        self.client = None
        self.bucket_name = None
        self.database_object = None

    def _get_storage_client(self):
        if self.client is None:
            credentials = loadCredentialsFromAptJson(getAptJsonPath())
            self.client = getStorageClient(credentials)
            self.bucket_name = getBucketName()
            self.database_object = getDatabaseObjectName()
        return self.client

    async def run(self) -> Dict[str, int]:
        print(f"Starting preview generation for up to {self.num_items} items")

        database_entries = await self._load_database_entries()
        if not database_entries:
            print("No database entries found")
            return {"processed": 0, "skipped": 0, "failed": 0, "added": 0}

        print(f"Found {len(database_entries)} database entries")

        available = [e for e in database_entries if e.get("prompt", "").strip() and "preview" not in e]
        already_done = len(database_entries) - len(available)
        print(f"Already have preview: {already_done}")
        print(f"Needing preview: {len(available)}")

        selected = self._select_random_items(available, self.num_items)
        print(f"Selected {len(selected)} items for processing")

        if not selected:
            print("No new items to process")
            return {"processed": 0, "skipped": already_done, "failed": 0, "added": 0}

        stats = await self._process_items(selected)

        print(f"Workflow completed:")
        print(f"  Processed: {stats['processed']}")
        print(f"  Skipped:   {stats['skipped']}")
        print(f"  Failed:    {stats['failed']}")
        print(f"  Added:     {stats['added']}")
        return stats

    async def _load_database_entries(self) -> List[Dict[str, Any]]:
        try:
            client = self._get_storage_client()
            data, _ = downloadJson(client, self.bucket_name, self.database_object)
            return data if isinstance(data, list) else []
        except Exception as e:
            print(f"Error loading database: {e}")
            return []

    @staticmethod
    def _select_random_items(items: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
        if len(items) <= n:
            return items
        return random.sample(items, n)

    async def _process_items(self, items: List[Dict[str, Any]]) -> Dict[str, int]:
        stats = {"processed": 0, "skipped": 0, "failed": 0, "added": 0}
        pending_updates: List[Dict[str, Any]] = []
        history_lines: List[str] = []

        # Process in batches of BATCH_SIZE
        for batch_start in range(0, len(items), BATCH_SIZE):
            batch = items[batch_start : batch_start + BATCH_SIZE]
            prompts = [e["prompt"].strip() for e in batch]
            batch_label = f"{batch_start + 1}-{batch_start + len(batch)}/{len(items)}"
            print(f"Processing batch {batch_label}")

            history_str = "\n".join(history_lines[-20:]) if history_lines else ""
            results = await self.llm.generate_previews(prompts, history=history_str)
            stats["processed"] += len(batch)

            if results is None:
                stats["failed"] += len(batch)
                print(f"  Batch failed entirely")
                continue

            for i, (entry, preview_line) in enumerate(zip(batch, results)):
                if preview_line:
                    pending_updates.append({
                        "prompt": entry["prompt"].strip(),
                        "preview": preview_line,
                    })
                    stats["added"] += 1
                    history_lines.append(preview_line)
                    print(f"  [{batch_start + i + 1}] OK")
                else:
                    stats["failed"] += 1
                    print(f"  [{batch_start + i + 1}] Failed (no result)")

            # Incremental save every 5 successful items
            if len(pending_updates) >= 5:
                await self._apply_updates(pending_updates)
                pending_updates = []

        # Final save
        if pending_updates:
            await self._apply_updates(pending_updates)

        return stats

    async def _apply_updates(self, updates: List[Dict[str, Any]], max_retries: int = 5) -> bool:
        if not updates:
            return True
        try:
            client = self._get_storage_client()
            attempt = 0
            backoff = 0.2
            while True:
                try:
                    data, generation = downloadJson(client, self.bucket_name, self.database_object)
                    update_map = {_prompt_dedup_key(u["prompt"]): u["preview"] for u in updates}

                    updated_count = 0
                    for entry in data:
                        key = _prompt_dedup_key(entry.get("prompt", "").strip())
                        if key and key in update_map:
                            entry["preview"] = update_map[key]
                            updated_count += 1

                    if updated_count > 0:
                        uploadJsonWithPreconditions(
                            client=client,
                            bucketName=self.bucket_name,
                            objectName=self.database_object,
                            data=data,
                            ifGenerationMatch=generation,
                        )
                        print(f"  Saved {updated_count} previews to DATABASE.json")
                    return True

                except Exception:
                    attempt += 1
                    if attempt > max_retries:
                        raise
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 2.0)

        except Exception as e:
            print(f"Error updating database: {e}")
            return False


# ---------------------------------------------------------------------------
# Single-prompt preview (used by the Streamlit autosave flow)
# ---------------------------------------------------------------------------

_singleton_llm: Optional[PreviewLLM] = None


def _get_llm() -> PreviewLLM:
    global _singleton_llm
    if _singleton_llm is None:
        _singleton_llm = PreviewLLM()
    return _singleton_llm


async def generate_single_preview(prompt_text: str) -> Optional[str]:
    """Generate a preview for a single prompt. Returns the filled string or None."""
    llm = _get_llm()
    results = await llm.generate_previews([prompt_text])
    if results and results[0]:
        return results[0]
    return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

async def main():
    if len(sys.argv) != 2:
        print("Usage: python llm_preview.py <number_of_items>")
        print("Example: python llm_preview.py 50")
        sys.exit(1)

    try:
        num_items = int(sys.argv[1])
        if num_items <= 0:
            print("Number of items must be positive")
            sys.exit(1)
    except ValueError:
        print("Number of items must be a valid integer")
        sys.exit(1)

    workflow = PreviewWorkflow(num_items)
    await workflow.run()


if __name__ == "__main__":
    asyncio.run(main())
