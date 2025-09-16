#!/usr/bin/env python3
"""
One-time script to reset all occurrence counts to 1 in the database.

This script addresses the inflated occurrence counts that resulted from
the duplicate detection bug where user selection wasn't being checked.
"""

import asyncio
import sys
from typing import List, Dict, Any

# Add the project root to Python path
sys.path.insert(0, '.')

from database import DatabaseManager
from cloud_storage import downloadJson, uploadJsonWithPreconditions, loadCredentialsFromAptJson, getStorageClient
from config import getBucketName, getAptJsonPath, getDatabaseObjectName, getUserSelectionObjectName, getDiscardsObjectName


async def reset_global_database_occurrences():
    """Reset all occurrence counts to 1 in the global database."""
    print("🔄 Resetting global database occurrences...")

    try:
        # Set up GCS client
        bucket_name = getBucketName()
        object_name = getDatabaseObjectName()
        apt_path = getAptJsonPath()
        credentials = loadCredentialsFromAptJson(apt_path)
        client = getStorageClient(credentials)

        # Download current data
        data, generation = downloadJson(client, bucket_name, object_name)
        print(f"📊 Found {len(data)} items in global database")

        # Reset occurrences
        reset_count = 0
        for item in data:
            if 'occurrences' in item and item['occurrences'] != 1:
                old_count = item['occurrences']
                item['occurrences'] = 1
                reset_count += 1
                print(f"  Reset {item.get('default', 'N/A')[:30]}...: {old_count} → 1")

        if reset_count > 0:
            # Upload updated data
            uploadJsonWithPreconditions(client, bucket_name, object_name, data, generation)
            print(f"✅ Reset {reset_count} items in global database")
        else:
            print("ℹ️  No items needed resetting in global database")

    except Exception as e:
        print(f"❌ Error resetting global database: {e}")


async def reset_user_selection_occurrences():
    """Reset all occurrence counts to 1 in user selection."""
    print("\n🔄 Resetting user selection occurrences...")

    try:
        db_manager = DatabaseManager()

        # Get current user selection data
        # We'll need to access the internal data since there's no direct reset method
        user_selection_data = await db_manager.userSelection._load_json()
        print(f"📊 Found {len(user_selection_data)} items in user selection")

        # Reset occurrences
        reset_count = 0
        for item in user_selection_data:
            if 'occurrences' in item and item['occurrences'] != 1:
                old_count = item['occurrences']
                item['occurrences'] = 1
                reset_count += 1
                print(f"  Reset {item.get('default', 'N/A')[:30]}...: {old_count} → 1")

        if reset_count > 0:
            # Save updated data
            await db_manager.userSelection._save_json(user_selection_data)
            print(f"✅ Reset {reset_count} items in user selection")
        else:
            print("ℹ️  No items needed resetting in user selection")

    except Exception as e:
        print(f"❌ Error resetting user selection: {e}")


async def reset_discards_occurrences():
    """Reset all occurrence counts to 1 in discards."""
    print("\n🔄 Resetting discards occurrences...")

    try:
        db_manager = DatabaseManager()

        # Get current discards data
        # We'll need to access the internal data since there's no direct reset method
        discards_data = await db_manager.discardsStore._load_json()
        print(f"📊 Found {len(discards_data)} items in discards")

        # Reset occurrences
        reset_count = 0
        for item in discards_data:
            if 'occurrences' in item and item['occurrences'] != 1:
                old_count = item['occurrences']
                item['occurrences'] = 1
                reset_count += 1
                print(f"  Reset {item.get('default', 'N/A')[:30]}...: {old_count} → 1")

        if reset_count > 0:
            # Save updated data
            await db_manager.discardsStore._save_json(discards_data)
            print(f"✅ Reset {reset_count} items in discards")
        else:
            print("ℹ️  No items needed resetting in discards")

    except Exception as e:
        print(f"❌ Error resetting discards: {e}")


async def main():
    """Main function to reset all occurrence counts."""
    print("🚀 Starting occurrence count reset...")
    print("This will set all occurrence counts to 1 in:")
    print("  - Global database")
    print("  - User selection")
    print("  - Discards")
    print()

    try:
        await reset_global_database_occurrences()
        await reset_user_selection_occurrences()
        await reset_discards_occurrences()

        print("\n🎉 Occurrence count reset complete!")
        print("All items now have occurrence count of 1.")
        print("Future duplicates will be counted accurately.")

    except Exception as e:
        print(f"\n❌ Error during reset: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
