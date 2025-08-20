#!prefetcher.py
import asyncio
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict

from database import DatabaseManager
from cloud_storage import downloadTextFile, getStorageClient, loadCredentialsFromAptJson
from config import getAptJsonPath, getBucketName, getRawStrippedObjectName


USER_SELECTION_FILE = "USER_SELECTION.json"
RAW_STRIPPED_FILE = "raw_stripped.txt"
WORKFLOW_FILE = "workflow.py"
LOCK_FILE = ".prefetcher.lock"


def getPaths() -> Dict[str, Path]:
    base = Path(__file__).resolve().parent
    return {
        "base": base,
        "userSelection": base / USER_SELECTION_FILE,
        "rawStripped": base / RAW_STRIPPED_FILE,
        "workflow": base / WORKFLOW_FILE,
        "lock": base / LOCK_FILE,
    }


def countUserSelection() -> int:
    try:
        db = DatabaseManager()
        count = asyncio.run(db.get_user_selection_count())
        return count
    except Exception:
        return 0


def main() -> None:
    # Args: targetCapacity [optional, default 10]
    targetCapacity = 10
    if len(sys.argv) >= 2:
        try:
            targetCapacity = int(sys.argv[1])
        except Exception:
            targetCapacity = 10

    paths = getPaths()

    # Create lock atomically; if exists, exit
    try:
        with open(paths["lock"], "x", encoding="utf-8") as f:
            f.write("running")
    except FileExistsError:
        return
    except Exception:
        return

    multiplier = 3.0  # start conservative
    try:
        while True:
            currentCount = countUserSelection()
            if currentCount >= targetCapacity:
                break

            deficit = targetCapacity - currentCount
            # Overfetch to compensate for duplicates/invalids
            x = max(deficit, int(math.ceil(deficit * multiplier)))

            # Check if workflow file exists locally
            if not paths["workflow"].exists():
                break

            # Check if raw_stripped.txt exists in GCS
            try:
                bucket_name = getBucketName()
                object_name = getRawStrippedObjectName()
                apt_path = getAptJsonPath()
                credentials = loadCredentialsFromAptJson(apt_path)
                client = getStorageClient(credentials)
                content, _ = downloadTextFile(client, bucket_name, object_name)
                if not content.strip():
                    break
            except Exception:
                break

            beforeCount = currentCount
            try:
                # Pass GCS object name instead of local file path
                subprocess.run(
                    [sys.executable, str(paths["workflow"]), object_name, str(x)],
                    cwd=str(paths["base"]),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            except Exception:
                # transient issue; slow down and retry
                time.sleep(2.0)
                continue

            # Measure yield
            time.sleep(0.5)  # allow filesystem to settle
            afterCount = countUserSelection()
            added = max(0, afterCount - beforeCount)

            # Adjust multiplier based on yield ratio
            ratio = (added / float(x)) if x > 0 else 0.0
            if ratio < 0.25:
                multiplier = min(multiplier * 2.0, 10.0)
            elif ratio > 0.75 and multiplier > 2.0:
                multiplier = max(2.0, multiplier * 0.8)

            # Brief pause to avoid tight loop
            time.sleep(1.0)
    finally:
        try:
            if paths["lock"].exists():
                os.remove(paths["lock"])
        except Exception:
            pass


if __name__ == "__main__":
    main()


