#!/usr/bin/env python3
"""
LLM Parameterization Workflow

Processes database entries that have not yet been parameterized and updates
them in-place with parametric data (craziness, isSexual, madeFor) using LLM.

Usage: python llm_parameterization.py <number_of_items>
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
from openai import AsyncOpenAI


# JSON Schema for validation
PARAMETRICS_SCHEMA = {
    "type": "object",
    "properties": {
        "prompt": {"type": "string", "minLength": 1},
        "craziness": {"type": "integer", "minimum": 1, "maximum": 4},
        "isSexual": {"type": "boolean"},
        "madeFor": {"type": "string", "enum": ["boys", "girls"]}
    },
    "required": ["prompt", "craziness", "isSexual"],
    "additionalProperties": False
}


class ParameterizationLLM:
    """LLM client specifically for parameterization tasks."""
    
    def __init__(self):
        """Initialize the parameterization LLM client."""
        self.client = None
        self.system_prompt = None
        self._load_system_prompt()
    
    def _load_system_prompt(self) -> None:
        """Load the parameterization system prompt."""
        try:
            with open('prompts/parameterize.prompt', 'r', encoding='utf-8') as f:
                self.system_prompt = f.read()
        except FileNotFoundError:
            raise RuntimeError("parameterize.prompt file not found")
    
    def _get_client(self) -> AsyncOpenAI:
        """Get or create the OpenAI client."""
        if self.client is None:
            api_key = getXaiApiKey()
            if not api_key:
                raise RuntimeError("XAI API key not available")
            self.client = AsyncOpenAI(
                api_key=api_key,
                base_url=getXaiBaseUrl()
            )
        return self.client
    
    async def parameterize(self, prompt_text: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Call LLM to parameterize a prompt text.
        
        Args:
            prompt_text: The prompt text to parameterize
            max_retries: Maximum number of retries on failure
            
        Returns:
            Dictionary with parametric data or None if failed
        """
        client = self._get_client()
        model = "grok-4-fast-reasoning"
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                temp = 0.0 if attempt == 0 else min(0.1 + (attempt * 0.1), 0.3)
                print(f"🎲 Attempt {attempt + 1}/{max_retries + 1}, temperature={temp}")
                
                response = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": f"Input:\n{prompt_text}"}
                    ],
                    temperature=temp,
                    max_tokens=1000,
                )
                
                response_text = response.choices[0].message.content
                print(f"\n🤖 LLM RAW RESPONSE for '{prompt_text[:50]}...':")
                print(f"{'='*60}")
                print(response_text)
                print(f"{'='*60}")
                
                if not response_text:
                    print("❌ Empty response from LLM")
                    continue
                
                # Parse JSON response
                try:
                    result = json.loads(response_text.strip())
                    print(f"✅ Valid JSON parsed: {json.dumps(result, indent=2)}")
                    
                    if self._validate_json_schema(result):
                        print(f"✅ Schema validation passed")
                        return result
                    else:
                        print(f"❌ Schema validation failed for '{prompt_text[:50]}...'")
                        return None
                        
                except json.JSONDecodeError as e:
                    print(f"❌ JSON parse error for '{prompt_text[:50]}...': {e}")
                    print(f"   Raw response: {repr(response_text)}")
                    
                    recovered_json = self._try_recover_partial_json(response_text, prompt_text)
                    if recovered_json:
                        print(f"🔧 Recovered partial JSON: {json.dumps(recovered_json, indent=2)}")
                        if self._validate_json_schema(recovered_json):
                            print(f"✅ Recovered JSON passed validation")
                            return recovered_json
                        else:
                            print(f"❌ Recovered JSON failed validation")
                    
                    if attempt < max_retries:
                        print(f"🔄 Retrying with higher temperature...")
                        continue
                    else:
                        return None
                    
            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()
                
                if "429" in error_msg or "rate limit" in error_msg:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * 60
                        print(f"⏳ Rate limited. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        continue
                else:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * 10
                        print(f"⚠️  LLM error: {e}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        continue
        
        print(f"❌ Failed to parameterize '{prompt_text[:50]}...' after {max_retries} retries")
        return None
    
    def _validate_json_schema(self, data: Any) -> bool:
        """Validate JSON data against the parametrics schema."""
        try:
            if not isinstance(data, dict):
                return False
            
            required_fields = ["prompt", "craziness", "isSexual"]
            for field in required_fields:
                if field not in data:
                    return False
            
            if not isinstance(data["prompt"], str) or len(data["prompt"].strip()) == 0:
                return False
            
            if not isinstance(data["craziness"], int) or not (1 <= data["craziness"] <= 4):
                return False
            
            if not isinstance(data["isSexual"], bool):
                return False
            
            if "madeFor" in data:
                if not isinstance(data["madeFor"], str) or data["madeFor"] not in ["boys", "girls"]:
                    return False
            
            allowed_fields = {"prompt", "craziness", "isSexual", "madeFor"}
            if set(data.keys()) - allowed_fields:
                return False
            
            return True
            
        except Exception:
            return False
    
    def _try_recover_partial_json(self, response_text: str, original_prompt: str) -> Optional[Dict[str, Any]]:
        """Attempt to recover a partial/truncated JSON response."""
        try:
            response_text = response_text.strip()
            
            if response_text.startswith('{') and not response_text.endswith('}'):
                if '"craziness":' in response_text and '"isSexual"' not in response_text:
                    import re
                    craziness_match = re.search(r'"craziness":\s*(\d+)', response_text)
                    if craziness_match:
                        craziness = int(craziness_match.group(1))
                        if 1 <= craziness <= 4:
                            recovered = {
                                "prompt": original_prompt,
                                "craziness": craziness,
                                "isSexual": False
                            }
                            return recovered
                
                if response_text.count('{') > response_text.count('}'):
                    try_complete = response_text + '}'
                    try:
                        return json.loads(try_complete)
                    except Exception:
                        pass
                
                if '"prompt":' in response_text and '"craziness":' in response_text:
                    if not response_text.endswith(',') and '"isSexual"' not in response_text:
                        try_complete = response_text.rstrip(',') + ', "isSexual": false}'
                        try:
                            return json.loads(try_complete)
                        except Exception:
                            pass
            
            return None
        except Exception:
            return None


class ParameterizationWorkflow:
    """Main workflow for parameterizing database entries.
    
    Reads from DATABASE.json, filters entries that lack parametric fields,
    processes them via LLM, and updates the entries in-place in DATABASE.json.
    """
    
    def __init__(self, num_items: int):
        """Initialize the workflow."""
        self.num_items = num_items
        self.llm = ParameterizationLLM()
        self.client = None
        self.bucket_name = None
        self.database_object = None
        
    def _get_storage_client(self):
        """Get or create storage client."""
        if self.client is None:
            credentials = loadCredentialsFromAptJson(getAptJsonPath())
            self.client = getStorageClient(credentials)
            self.bucket_name = getBucketName()
            self.database_object = getDatabaseObjectName()
        return self.client
    
    async def run(self) -> Dict[str, int]:
        """Run the parameterization workflow."""
        print(f"🚀 Starting parameterization workflow for {self.num_items} items")
        
        # Load data
        print("📥 Loading database entries...")
        database_entries = await self._load_database_entries()
        
        if not database_entries:
            print("❌ No database entries found")
            return {"processed": 0, "skipped": 0, "failed": 0, "added": 0}
        
        print(f"📊 Found {len(database_entries)} database entries")
        
        # Filter entries that need parameterization (no craziness field)
        available_items = self._filter_unparameterized(database_entries)
        already_parameterized = len(database_entries) - len(available_items)
        print(f"📊 Already parameterized: {already_parameterized}")
        print(f"📊 Needing parameterization: {len(available_items)}")
        
        selected_items = self._select_random_items(available_items, self.num_items)
        
        print(f"🎯 Selected {len(selected_items)} items for processing")
        
        if not selected_items:
            print("ℹ️  No new items to process")
            return {"processed": 0, "skipped": already_parameterized, "failed": 0, "added": 0}
        
        # Process items and update in-place
        stats = await self._process_items(selected_items)
        
        print(f"✅ Workflow completed:")
        print(f"   📊 Processed: {stats['processed']}")
        print(f"   ⏭️  Skipped: {stats['skipped']}")
        print(f"   ❌ Failed: {stats['failed']}")
        print(f"   ✅ Updated: {stats['added']}")
        
        return stats
    
    async def _load_database_entries(self) -> List[Dict[str, Any]]:
        """Load entries from the global database."""
        try:
            client = self._get_storage_client()
            data, _ = downloadJson(client, self.bucket_name, self.database_object)
            return data if isinstance(data, list) else []
        except Exception as e:
            print(f"❌ Error loading database: {e}")
            return []
    
    def _filter_unparameterized(self, database_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter database entries that haven't been parameterized yet.
        
        An entry is considered unparameterized if it lacks the 'craziness' field.
        """
        available = []
        for entry in database_entries:
            prompt = entry.get("prompt", "").strip()
            if prompt and "craziness" not in entry:
                available.append(entry)
        return available
    
    def _select_random_items(self, available_items: List[Dict[str, Any]], 
                           num_items: int) -> List[Dict[str, Any]]:
        """Select random items from available items."""
        if len(available_items) <= num_items:
            return available_items
        return random.sample(available_items, num_items)
    
    async def _process_items(self, items: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process the selected items and update them in DATABASE.json."""
        stats = {"processed": 0, "skipped": 0, "failed": 0, "added": 0}
        pending_updates: List[Dict[str, Any]] = []
        
        for i, item in enumerate(items, 1):
            prompt = item.get("prompt", "").strip()
            if not prompt:
                stats["skipped"] += 1
                continue
            
            print(f"🔄 Processing {i}/{len(items)}: '{prompt[:50]}{'...' if len(prompt) > 50 else ''}'")
            
            # Call LLM for parameterization
            result = await self.llm.parameterize(prompt)
            stats["processed"] += 1
            
            if result:
                pending_updates.append({
                    "prompt": prompt,
                    "craziness": result["craziness"],
                    "isSexual": result["isSexual"],
                    "madeFor": result.get("madeFor"),
                })
                stats["added"] += 1
                print(f"   ✅ Added: craziness={result['craziness']}, sexual={result['isSexual']}")
                
                # Save incrementally every 5 items
                if len(pending_updates) % 5 == 0:
                    await self._apply_updates_to_database(pending_updates)
                    pending_updates = []
            else:
                stats["failed"] += 1
                print(f"   ❌ Failed to parameterize")
        
        # Final save for remaining updates
        if pending_updates:
            await self._apply_updates_to_database(pending_updates)
        
        return stats
    
    async def _apply_updates_to_database(self, updates: List[Dict[str, Any]], max_retries: int = 5) -> bool:
        """Apply parametric updates to entries in DATABASE.json.
        
        Downloads the database, finds matching entries by prompt,
        merges parametric fields, and uploads with optimistic concurrency.
        """
        if not updates:
            return True
            
        try:
            client = self._get_storage_client()
            
            attempt = 0
            backoff = 0.2
            while True:
                try:
                    data, generation = downloadJson(client, self.bucket_name, self.database_object)
                    
                    # Build lookup for quick matching
                    update_map = {u["prompt"]: u for u in updates}
                    
                    updated_count = 0
                    for entry in data:
                        entry_prompt = entry.get("prompt", "").strip()
                        if entry_prompt in update_map:
                            upd = update_map[entry_prompt]
                            entry["craziness"] = upd["craziness"]
                            entry["isSexual"] = upd["isSexual"]
                            if upd.get("madeFor"):
                                entry["madeFor"] = upd["madeFor"]
                            updated_count += 1
                    
                    if updated_count > 0:
                        uploadJsonWithPreconditions(
                            client=client,
                            bucketName=self.bucket_name,
                            objectName=self.database_object,
                            data=data,
                            ifGenerationMatch=generation
                        )
                        print(f"💾 Updated {updated_count} entries in DATABASE.json")
                    
                    return True
                    
                except Exception as e:
                    attempt += 1
                    if attempt > max_retries:
                        raise
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 2.0)
            
        except Exception as e:
            print(f"❌ Error updating database: {e}")
            return False


async def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python llm_parameterization.py <number_of_items>")
        print("Example: python llm_parameterization.py 10")
        sys.exit(1)
    
    try:
        num_items = int(sys.argv[1])
        if num_items <= 0:
            print("❌ Number of items must be positive")
            sys.exit(1)
    except ValueError:
        print("❌ Number of items must be a valid integer")
        sys.exit(1)
    
    # Run workflow
    workflow = ParameterizationWorkflow(num_items)
    await workflow.run()


if __name__ == "__main__":
    asyncio.run(main())
