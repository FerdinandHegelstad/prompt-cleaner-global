import json
import os
import asyncio
from typing import Any, Dict, List, Optional

from cloud_storage import downloadJson, getStorageClient, loadCredentialsFromAptJson
from config import getAptJsonPath, getBucketName, getDatabaseObjectName
from database import DatabaseManager
from normalizer import build_dedup_key, normalize
from llm import call_llm


class WorkflowTester:
    """Tests the entire workflow end-to-end with verbose logging using the real LLM."""
    
    def __init__(self, db_manager: DatabaseManager, write_global: bool = True):
        self.db_manager = db_manager
        self.write_global = write_global
        self.processed_items: List[Dict[str, Any]] = []
        self.added_to_user_selection: List[Dict[str, Any]] = []
        self.added_to_global_db: List[Dict[str, Any]] = []
        self.duplicates_blocked: List[Dict[str, Any]] = []
    
    async def process_single_item(self, default_text: str) -> None:
        """Process a single item through the entire workflow."""
        print(f"\n{'='*80}")
        print(f"PROCESSING ITEM: {default_text!r}")
        print(f"{'='*80}")
        
        # Step 1: LLM Cleaning (real API call)
        print(f"\n[STEP 1] LLM Cleaning (real API)")
        cleaned = await call_llm(default_text)
        if cleaned is None:
            print(f"❌ LLM returned None - skipping item")
            return
        
        # Post-process LLM output (same as real workflow)
        cleaned = cleaned.strip()
        if not cleaned:
            print(f"❌ Cleaned text is empty - skipping item")
            return
        if "\n" in cleaned:
            cleaned = cleaned.splitlines()[0].strip()
        if (cleaned.startswith('"') and cleaned.endswith('"')) or (cleaned.startswith("'") and cleaned.endswith("'")):
            cleaned = cleaned[1:-1].strip()
        
        print(f"✅ Post-processed cleaned text: {cleaned!r}")
        
        # Step 2: Normalization
        print(f"\n[STEP 2] Normalization")
        normalized = normalize(cleaned).strip()
        if not normalized:
            print(f"❌ Normalized text is empty - skipping item")
            return
        
        print(f"✅ Normalized text: {normalized!r}")
        
        # Step 3: Dedup Key Computation
        print(f"\n[STEP 3] Dedup Key Computation")
        dedup_key = build_dedup_key(normalized)
        print(f"✅ Dedup key: {dedup_key!r}")
        
        # Step 4: Database Existence Check
        print(f"\n[STEP 4] Database Existence Check")
        try:
            exists = await self.db_manager.exists_in_database(normalized)
            print(f"✅ Exists in database: {exists}")
        except Exception as e:
            print(f"❌ Database check failed: {e}")
            return
        
        # Step 5: Decision and Action
        print(f"\n[STEP 5] Decision and Action")
        if exists:
            print(f"❌ DUPLICATE DETECTED - Item will NOT be added")
            self.duplicates_blocked.append({
                "default": default_text,
                "cleaned": cleaned,
                "normalized": normalized,
                "dedup_key": dedup_key
            })
        else:
            print(f"✅ UNIQUE ITEM - Adding to user selection and global DB")
            
            # Add to user selection
            print(f"\n[STEP 5a] Adding to User Selection")
            try:
                await self.db_manager.add_to_user_selection({
                    "default": default_text,
                    "cleaned": cleaned,
                    "normalized": normalized
                })
                print(f"✅ Added to user selection")
                self.added_to_user_selection.append({
                    "default": default_text,
                    "cleaned": cleaned,
                    "normalized": normalized,
                    "dedup_key": dedup_key
                })
            except Exception as e:
                print(f"❌ Failed to add to user selection: {e}")
                return
            
            # Add to global database (optional)
            if self.write_global:
                print(f"\n[STEP 5b] Adding to Global Database")
                try:
                    await self.db_manager.add_to_global_database({
                        "default": default_text,
                        "cleaned": cleaned,
                        "normalized": normalized
                    })
                    print(f"✅ Added to global database")
                    self.added_to_global_db.append({
                        "default": default_text,
                        "cleaned": cleaned,
                        "normalized": normalized,
                        "dedup_key": dedup_key
                    })
                except Exception as e:
                    print(f"❌ Failed to add to global database: {e}")
                    print(f"⚠️  Note: Item was added to user selection but not global DB")
        
        # Record processed item
        self.processed_items.append({
            "default": default_text,
            "cleaned": cleaned,
            "normalized": normalized,
            "dedup_key": dedup_key,
            "was_duplicate": exists
        })


def load_real_cloud_db() -> List[Dict[str, Any]]:
    """Load the real Cloud DB data to use as initial database."""
    try:
        bucket = getBucketName()
        object_name = getDatabaseObjectName()
        apt = getAptJsonPath()
        print(f"Loading from Cloud DB: bucket={bucket}, object={object_name}")
        creds = loadCredentialsFromAptJson(apt)
        client = getStorageClient(creds)
        data, generation = downloadJson(client, bucket, object_name)
        print(f"Loaded {len(data)} entries from generation {generation}")
        return data
    except Exception as e:
        print(f"Failed to load Cloud DB: {e}")
        print("Falling back to static test data...")
        return [
            {
                "default": "Emilie drikk",
                "cleaned": "[PLAYER] [DRINKS]",
                "normalized": "PLAYER DRINKS",
            },
            {
                "default": "Nice Apple",
                "cleaned": "Nice Apple",
                "normalized": "Nice Apple",
            },
        ]


def print_workflow_summary(tester: WorkflowTester) -> None:
    """Print a comprehensive summary of the workflow test results."""
    print(f"\n{'='*80}")
    print(f"WORKFLOW TEST SUMMARY")
    print(f"{'='*80}")
    
    print(f"\n📊 PROCESSING STATISTICS:")
    print(f"  Total items processed: {len(tester.processed_items)}")
    print(f"  Items added to user selection: {len(tester.added_to_user_selection)}")
    print(f"  Items added to global DB: {len(tester.added_to_global_db)}")
    print(f"  Duplicates blocked: {len(tester.duplicates_blocked)}")
    
    print(f"\n✅ ITEMS ADDED TO USER SELECTION:")
    for i, item in enumerate(tester.added_to_user_selection, 1):
        print(f"  {i:2d}. {item['default']!r}")
        print(f"      → cleaned: {item['cleaned']!r}")
        print(f"      → normalized: {item['normalized']!r}")
        print(f"      → dedup_key: {item['dedup_key']!r}")
    
    print(f"\n❌ DUPLICATES BLOCKED:")
    for i, item in enumerate(tester.duplicates_blocked, 1):
        print(f"  {i:2d}. {item['default']!r}")
        print(f"      → cleaned: {item['cleaned']!r}")
        print(f"      → normalized: {item['normalized']!r}")
        print(f"      → dedup_key: {item['dedup_key']!r}")
    
    print(f"\n📋 DETAILED PROCESSING LOG:")
    for i, item in enumerate(tester.processed_items, 1):
        status = "❌ BLOCKED" if item['was_duplicate'] else "✅ ADDED"
        print(f"  {i:2d}. {status} - {item['default']!r}")
        print(f"      dedup_key: {item['dedup_key']!r}")


async def run_workflow_test() -> None:
    """Run the complete workflow test with REAL LLM and Cloud DB dedup checks."""
    print(f"{'='*80}")
    print(f"COMPLETE WORKFLOW TEST")
    print(f"Testing entire pipeline: LLM → Cleaning → Normalization → Dedup → Database")
    print(f"{'='*80}")
    
    # Initialize components
    db_manager = DatabaseManager()
    # Allow opting out of global writes via env var (default True for full E2E)
    write_global_env = os.getenv("TEST_DEDUP_DEMO_WRITE_GLOBAL", "1").strip()
    write_global = write_global_env not in ("0", "false", "False")
    
    # Test items from the prescribed examples in clean.prompt (plus some extras)
    test_items = [
        "Markus liker å danse 💃",
        "Pkelek, den som får flest pek drikker 5.",
        "Markus hvis du liker rødvin må du drikke 3",
        "Er det en i dette rommet du vil ligge med? Svar eller drikk 5",
        "hatt klamma?",
        # Extras to exercise dedup and placeholders further
        "Great Player Banana",
        "Cool Players Drink",
        "Some Unique Text",
        "Janix elsker å danse",
        "Janix må drikke 5",
        "Janix og Janix må kline",
    ]
    
    # Initialize workflow tester
    tester = WorkflowTester(db_manager, write_global=write_global)
    
    # Process each item through the complete workflow
    print(f"\n🚀 STARTING WORKFLOW PROCESSING")
    print(f"Processing {len(test_items)} test items...")
    
    for i, item in enumerate(test_items, 1):
        print(f"\n{'='*60}")
        print(f"ITEM {i}/{len(test_items)}")
        print(f"{'='*60}")
        await tester.process_single_item(item)
    
    # Print comprehensive summary
    print_workflow_summary(tester)
    
    print(f"\n{'='*80}")
    print(f"WORKFLOW TEST COMPLETE")
    print(f"{'='*80}")


if __name__ == "__main__":
    asyncio.run(run_workflow_test())


