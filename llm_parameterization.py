#!/usr/bin/env python3
"""
LLM Parameterization Workflow

Processes cleaned entries from the database and generates parametric data using LLM.
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
    getParametricsObjectName,
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
            with open('paramaterize.prompt', 'r', encoding='utf-8') as f:
                self.system_prompt = f.read()
        except FileNotFoundError:
            raise RuntimeError("paramaterize.prompt file not found")
    
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
    
    async def parameterize(self, cleaned_text: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Call LLM to parameterize a cleaned text entry.
        
        Args:
            cleaned_text: The cleaned text to parameterize
            max_retries: Maximum number of retries on failure
            
        Returns:
            Dictionary with parametric data or None if failed
        """
        client = self._get_client()
        model = "grok-4-fast-reasoning"  # Use grok-4-fast-reasoning specifically for parameterization
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                temp = 0.0 if attempt == 0 else min(0.1 + (attempt * 0.1), 0.3)
                print(f"🎲 Attempt {attempt + 1}/{max_retries + 1}, temperature={temp}")
                
                response = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": self.system_prompt},
                        {"role": "user", "content": f"Input:\n{cleaned_text}"}
                    ],
                    temperature=temp,
                    max_tokens=1000,
                )
                
                response_text = response.choices[0].message.content
                print(f"\n🤖 LLM RAW RESPONSE for '{cleaned_text[:50]}...':")
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
                    
                    # Validate against schema
                    if self._validate_json_schema(result):
                        print(f"✅ Schema validation passed")
                        return result
                    else:
                        print(f"❌ Schema validation failed for '{cleaned_text[:50]}...'")
                        print(f"   Expected: prompt(str), craziness(1-4), isSexual(bool), madeFor(optional)")
                        return None
                        
                except json.JSONDecodeError as e:
                    print(f"❌ JSON parse error for '{cleaned_text[:50]}...': {e}")
                    print(f"   Raw response: {repr(response_text)}")
                    
                    # Try to recover partial JSON
                    recovered_json = self._try_recover_partial_json(response_text, cleaned_text)
                    if recovered_json:
                        print(f"🔧 Recovered partial JSON: {json.dumps(recovered_json, indent=2)}")
                        if self._validate_json_schema(recovered_json):
                            print(f"✅ Recovered JSON passed validation")
                            return recovered_json
                        else:
                            print(f"❌ Recovered JSON failed validation")
                    
                    # If this is not the last attempt, continue to retry
                    if attempt < max_retries:
                        print(f"🔄 Retrying with higher temperature...")
                        continue
                    else:
                        return None
                    
            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()
                
                # Handle rate limits
                if "429" in error_msg or "rate limit" in error_msg:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * 60
                        print(f"⏳ Rate limited. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        continue
                else:
                    # Other errors
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * 10
                        print(f"⚠️  LLM error: {e}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        continue
        
        print(f"❌ Failed to parameterize '{cleaned_text[:50]}...' after {max_retries} retries")
        return None
    
    def _validate_json_schema(self, data: Any) -> bool:
        """Validate JSON data against the parametrics schema."""
        try:
            # Basic type checks
            if not isinstance(data, dict):
                return False
            
            # Required fields
            required_fields = ["prompt", "craziness", "isSexual"]
            for field in required_fields:
                if field not in data:
                    return False
            
            # Field type and value validation
            if not isinstance(data["prompt"], str) or len(data["prompt"].strip()) == 0:
                return False
            
            if not isinstance(data["craziness"], int) or not (1 <= data["craziness"] <= 4):
                return False
            
            if not isinstance(data["isSexual"], bool):
                return False
            
            # Optional madeFor field
            if "madeFor" in data:
                if not isinstance(data["madeFor"], str) or data["madeFor"] not in ["boys", "girls"]:
                    return False
            
            # No additional properties
            allowed_fields = {"prompt", "craziness", "isSexual", "madeFor"}
            if set(data.keys()) - allowed_fields:
                return False
            
            return True
            
        except Exception:
            return False
    
    def _try_recover_partial_json(self, response_text: str, original_prompt: str) -> Optional[Dict[str, Any]]:
        """Attempt to recover a partial/truncated JSON response."""
        try:
            # Common patterns for incomplete JSON
            response_text = response_text.strip()
            
            # If it looks like it starts with JSON but is incomplete
            if response_text.startswith('{') and not response_text.endswith('}'):
                # Try to complete common patterns
                
                # Pattern 1: Missing closing brace and isSexual field
                if '"craziness":' in response_text and '"isSexual"' not in response_text:
                    # Extract craziness value
                    import re
                    craziness_match = re.search(r'"craziness":\s*(\d+)', response_text)
                    if craziness_match:
                        craziness = int(craziness_match.group(1))
                        if 1 <= craziness <= 4:
                            # Construct complete JSON
                            recovered = {
                                "prompt": original_prompt,
                                "craziness": craziness,
                                "isSexual": False  # Default to false for safety
                            }
                            return recovered
                
                # Pattern 2: Try to add missing closing brace
                if response_text.count('{') > response_text.count('}'):
                    try_complete = response_text + '}'
                    try:
                        return json.loads(try_complete)
                    except:
                        pass
                
                # Pattern 3: Try to add missing fields and closing brace
                if '"prompt":' in response_text and '"craziness":' in response_text:
                    if not response_text.endswith(',') and not '"isSexual"' in response_text:
                        try_complete = response_text.rstrip(',') + ', "isSexual": false}'
                        try:
                            return json.loads(try_complete)
                        except:
                            pass
            
            return None
        except Exception:
            return None


class ParameterizationWorkflow:
    """Main workflow for parameterizing database entries."""
    
    def __init__(self, num_items: int):
        """Initialize the workflow."""
        self.num_items = num_items
        self.llm = ParameterizationLLM()
        self.client = None
        self.bucket_name = None
        self.database_object = None
        self.parametrics_object = None
        
    def _get_storage_client(self):
        """Get or create storage client."""
        if self.client is None:
            credentials = loadCredentialsFromAptJson(getAptJsonPath())
            self.client = getStorageClient(credentials)
            self.bucket_name = getBucketName()
            self.database_object = getDatabaseObjectName()
            self.parametrics_object = getParametricsObjectName()
        return self.client
    
    async def run(self) -> Dict[str, int]:
        """Run the parameterization workflow."""
        print(f"🚀 Starting parameterization workflow for {self.num_items} items")
        
        # Load data
        print("📥 Loading database and parametrics data...")
        database_entries = await self._load_database_entries()
        existing_parametrics = await self._load_existing_parametrics()
        
        if not database_entries:
            print("❌ No database entries found")
            return {"processed": 0, "skipped": 0, "failed": 0, "added": 0}
        
        print(f"📊 Found {len(database_entries)} database entries")
        print(f"📊 Found {len(existing_parametrics)} existing parametrics")
        
        # Filter and select items
        available_items = self._filter_available_items(database_entries, existing_parametrics)
        selected_items = self._select_random_items(available_items, self.num_items)
        
        print(f"🎯 Selected {len(selected_items)} items for processing")
        
        if not selected_items:
            print("ℹ️  No new items to process")
            return {"processed": 0, "skipped": len(database_entries) - len(available_items), "failed": 0, "added": 0}
        
        # Process items
        stats = await self._process_items(selected_items, existing_parametrics)
        
        print(f"✅ Workflow completed:")
        print(f"   📊 Processed: {stats['processed']}")
        print(f"   ⏭️  Skipped: {stats['skipped']}")
        print(f"   ❌ Failed: {stats['failed']}")
        print(f"   ✅ Added: {stats['added']}")
        
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
    
    async def _load_existing_parametrics(self) -> List[Dict[str, Any]]:
        """Load existing parametrics data."""
        try:
            client = self._get_storage_client()
            data, _ = downloadJson(client, self.bucket_name, self.parametrics_object)
            return data if isinstance(data, list) else []
        except Exception as e:
            print(f"⚠️  Error loading parametrics (may not exist yet): {e}")
            return []
    
    def _filter_available_items(self, database_entries: List[Dict[str, Any]], 
                              existing_parametrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter database entries that haven't been parameterized yet."""
        existing_prompts = {item.get("prompt", "") for item in existing_parametrics}
        
        available = []
        for entry in database_entries:
            cleaned = entry.get("cleaned", "").strip()
            if cleaned and cleaned not in existing_prompts:
                available.append(entry)
        
        return available
    
    def _select_random_items(self, available_items: List[Dict[str, Any]], 
                           num_items: int) -> List[Dict[str, Any]]:
        """Select random items from available items."""
        if len(available_items) <= num_items:
            return available_items
        
        return random.sample(available_items, num_items)
    
    async def _process_items(self, items: List[Dict[str, Any]], 
                           existing_parametrics: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process the selected items."""
        stats = {"processed": 0, "skipped": 0, "failed": 0, "added": 0}
        new_parametrics = []
        
        for i, item in enumerate(items, 1):
            cleaned = item.get("cleaned", "").strip()
            if not cleaned:
                stats["skipped"] += 1
                continue
            
            print(f"🔄 Processing {i}/{len(items)}: '{cleaned[:50]}{'...' if len(cleaned) > 50 else ''}'")
            
            # Call LLM for parameterization
            result = await self.llm.parameterize(cleaned)
            stats["processed"] += 1
            
            if result:
                new_parametrics.append(result)
                stats["added"] += 1
                print(f"   ✅ Added: craziness={result['craziness']}, sexual={result['isSexual']}")
                
                # Save incrementally every 5 items
                if len(new_parametrics) % 5 == 0:
                    await self._save_parametrics_incremental(existing_parametrics + new_parametrics)
            else:
                stats["failed"] += 1
                print(f"   ❌ Failed to parameterize")
        
        # Final save
        if new_parametrics:
            final_parametrics = existing_parametrics + new_parametrics
            await self._save_parametrics_incremental(final_parametrics)
        
        return stats
    
    async def _save_parametrics_incremental(self, parametrics_data: List[Dict[str, Any]]) -> bool:
        """Save parametrics data incrementally."""
        try:
            client = self._get_storage_client()
            
            # Get current generation for optimistic concurrency
            current_data, generation = downloadJson(client, self.bucket_name, self.parametrics_object)
            
            # Upload with precondition
            uploadJsonWithPreconditions(
                client=client,
                bucketName=self.bucket_name,
                objectName=self.parametrics_object,
                data=parametrics_data,
                ifGenerationMatch=generation
            )
            
            print(f"💾 Saved {len(parametrics_data)} parametrics to cloud storage")
            return True
            
        except Exception as e:
            print(f"❌ Error saving parametrics: {e}")
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
