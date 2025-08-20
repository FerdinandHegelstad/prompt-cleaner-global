import argparse
import json
import os
import sys

from cloud_storage import downloadJson, getStorageClient, loadCredentialsFromAptJson
from config import getAptJsonPath, getBucketName, getDatabaseObjectName


def main() -> None:
    parser = argparse.ArgumentParser(description="Print entries from Cloud DB (GCS).")
    parser.add_argument("--limit", type=int, default=10, help="How many entries to print")
    parser.add_argument("--raw-bytes", type=int, default=0, help="Print first N raw bytes of the JSON object (0 to skip)")
    args = parser.parse_args()

    print("ENV:")
    print("  GCS_BUCKET=", os.getenv("GCS_BUCKET"))
    print("  GCS_DATABASE_OBJECT=", os.getenv("GCS_DATABASE_OBJECT"))
    print("  APT_JSON_PATH=", os.getenv("APT_JSON_PATH"))

    try:
        bucket = getBucketName()
        object_name = getDatabaseObjectName()
        apt = getAptJsonPath()
    except Exception as e:
        print("\nCONFIG ERROR:", repr(e))
        sys.exit(1)

    print("\nResolved Config:")
    print("  Bucket:", bucket)
    print("  Object:", object_name)
    print("  APT.json:", apt)

    try:
        creds = loadCredentialsFromAptJson(apt)
        client = getStorageClient(creds)
    except Exception as e:
        print("\nAUTH/CLIENT ERROR:", repr(e))
        sys.exit(1)

    try:
        data, generation = downloadJson(client, bucket, object_name)
    except Exception as e:
        print("\nDOWNLOAD ERROR:", repr(e))
        sys.exit(1)

    print("\nRemote Object:")
    print("  Generation:", generation)

    if args.raw_bytes and args.raw_bytes > 0:
        try:
            # Re-download raw text for a raw preview
            bucket_ref = client.bucket(bucket)
            blob = bucket_ref.get_blob(object_name)
            if blob is None:
                print("Object not found when fetching raw preview.")
            else:
                raw = blob.download_as_text(encoding="utf-8")
                preview = raw[: args.raw_bytes]
                print(f"\n=== RAW JSON (first {args.raw_bytes} bytes) ===")
                print(preview)
                if len(raw) > len(preview):
                    print("\n... [TRUNCATED] ...")
                print("=== END RAW PREVIEW ===")
        except Exception as e:
            print("\nRAW PREVIEW ERROR:", repr(e))

    if not isinstance(data, list):
        print("\nNote: top-level JSON is not a list; printing parsed object:")
        print(json.dumps(data, ensure_ascii=False, indent=4))
        return

    print("\nEntries count:", len(data))
    to_show = data[: max(0, args.limit)]
    print(f"\nFirst {len(to_show)} entries (pretty JSON):")
    for i, item in enumerate(to_show, start=1):
        print(f"\n--- Entry {i} ---")
        print(json.dumps(item, ensure_ascii=False, indent=4))


if __name__ == "__main__":
    main()


