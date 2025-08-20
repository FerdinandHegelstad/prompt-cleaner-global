#!/usr/bin/env python3
"""
Debug script to identify why workflow is not adding items to user selection.
"""
import sys
import os
sys.path.insert(0, '.')

def test_config():
    """Test configuration loading"""
    print("🧪 Testing Configuration...")
    try:
        from config import getBucketName, getRawStrippedObjectName, getAptJsonPath, getXaiApiKey

        bucket_name = getBucketName()
        raw_object = getRawStrippedObjectName()
        apt_path = getAptJsonPath()
        api_key = getXaiApiKey()

        print(f"✅ Bucket: {bucket_name}")
        print(f"✅ Raw object: {raw_object}")
        print(f"✅ APT path: {apt_path}")
        print(f"✅ API key available: {'YES' if api_key else 'NO'}")

        return bucket_name, raw_object, apt_path
    except Exception as e:
        print(f"❌ Config error: {e}")
        return None, None, None

def test_gcs_connection(bucket_name, raw_object, apt_path):
    """Test GCS connectivity"""
    print("\n🧪 Testing GCS Connection...")
    try:
        from cloud_storage import downloadTextFile, getStorageClient, loadCredentialsFromAptJson

        credentials = loadCredentialsFromAptJson(apt_path)
        client = getStorageClient(credentials)

        content, generation = downloadTextFile(client, bucket_name, raw_object)

        if content:
            lines = content.strip().split('\n')
            non_empty = [line for line in lines if line.strip()]
            print(f"✅ Raw file has {len(lines)} total lines")
            print(f"✅ Raw file has {len(non_empty)} non-empty lines")
            if non_empty:
                print(f"✅ Sample: {non_empty[0][:100]}...")
            return True
        else:
            print("❌ Raw file is empty or missing")
            return False

    except Exception as e:
        print(f"❌ GCS error: {e}")
        return False

def test_database():
    """Test database connectivity"""
    print("\n🧪 Testing Database...")
    try:
        from database import DatabaseManager
        import asyncio

        async def check_db():
            db = DatabaseManager()
            count = await db.get_user_selection_count()
            print(f"✅ User selection count: {count}")
            return count

        count = asyncio.run(check_db())
        return count

    except Exception as e:
        print(f"❌ Database error: {e}")
        return None

def test_workflow():
    """Test workflow execution"""
    print("\n🧪 Testing Workflow...")
    try:
        from workflow import Workflow
        import asyncio
        import traceback

        async def run_test():
            # Test with just 1 item to see if workflow works
            workflow = Workflow("raw_stripped.txt", 1)
            await workflow.run()
            print("✅ Workflow completed")
            return True

        result = asyncio.run(run_test())
        return result

    except Exception as e:
        print(f"❌ Workflow error: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        print(f"❌ Full traceback:")
        traceback.print_exc()
        return False

def main():
    print("🔍 DEBUGGING WORKFLOW ISSUES\n")

    # Test configuration
    bucket_name, raw_object, apt_path = test_config()
    if not bucket_name:
        return

    # Test GCS
    gcs_ok = test_gcs_connection(bucket_name, raw_object, apt_path)
    if not gcs_ok:
        return

    # Test database
    user_count = test_database()
    if user_count is None:
        return

    # Test workflow with more detailed error info
    print("\n🧪 Testing Workflow (Detailed)...")
    try:
        from workflow import Workflow
        import asyncio
        import traceback

        async def run_test_detailed():
            # Test with just 1 item to see if workflow works
            workflow = Workflow("raw_stripped.txt", 1)
            print("   ✅ Workflow object created")

            # Get the items to process
            items = await workflow._select_and_remove_items()
            print(f"   ✅ Got {len(items)} items to process")

            if items:
                print(f"   ✅ First item preview: {items[0][:100]}...")

                # Create Item objects and try to process the first one
                from workflow import Item
                item_obj = Item(default=items[0])
                print("   🔄 Processing first item...")
                await workflow._process_single_item(item_obj)
                print("   ✅ Single item processed successfully")

            return True

        result = asyncio.run(run_test_detailed())
        print(f"   ✅ Workflow test completed")
        workflow_ok = True

    except Exception as e:
        print(f"   ❌ Workflow error: {e}")
        print(f"   ❌ Error type: {type(e).__name__}")
        print("   ❌ Full traceback:")
        traceback.print_exc()
        workflow_ok = False

    print(f"\n📊 SUMMARY:")
    print(f"   User selection items: {user_count}")
    print(f"   GCS connection: {'✅ OK' if gcs_ok else '❌ FAIL'}")
    print(f"   Workflow execution: {'✅ OK' if workflow_ok else '❌ FAIL'}")

if __name__ == "__main__":
    main()
