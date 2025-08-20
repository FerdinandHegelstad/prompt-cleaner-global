#!config.py
import os
from typing import Optional


def getBucketName() -> str:
    """Returns the Google Cloud Storage bucket name for the global database.

    The value is read from the environment variable GCS_BUCKET or Streamlit secrets.
    Raises a RuntimeError if not set to avoid ambiguous defaults.
    """
    bucketName: Optional[str] = os.getenv("GCS_BUCKET")
    print(f"DEBUG: GCS_BUCKET environment variable: {bucketName}")

    # Try to get from Streamlit secrets
    if not bucketName:
        try:
            import streamlit as st
            print(f"DEBUG: Streamlit secrets available: {hasattr(st, 'secrets')}")
            if hasattr(st, 'secrets'):
                print(f"DEBUG: Available secrets sections: {list(st.secrets.keys()) if st.secrets else 'No secrets'}")
                if 'gcs' in st.secrets:
                    bucketName = st.secrets['gcs']['bucket_name']
                    print(f"DEBUG: Bucket name from secrets: {bucketName}")
        except Exception as e:
            print(f"DEBUG: Error loading from Streamlit secrets: {e}")

    if not bucketName:
        raise RuntimeError(
            "GCS_BUCKET environment variable is not set and bucket_name not found in Streamlit secrets. Set it to the name of your GCS bucket."
        )
    print(f"DEBUG: Final bucket name: {bucketName}")
    return bucketName


def getDatabaseObjectName() -> str:
    """Returns the object name for the global database JSON within the bucket.

    Defaults to 'DATABASE.json' at the bucket root. Can be overridden via
    the environment variable GCS_DATABASE_OBJECT.
    """
    objectName: Optional[str] = os.getenv("GCS_DATABASE_OBJECT")
    return objectName or "DATABASE.json"


def getAptJsonPath() -> str:
    """Returns the file path to the service account JSON (APT.json).

    Defaults to 'APT.json' in the project root. Can be overridden via
    the environment variable APT_JSON_PATH.

    For Streamlit Cloud, returns empty string when using secrets.
    """
    aptPath: Optional[str] = os.getenv("APT_JSON_PATH")
    return aptPath or "APT.json"


def getRawStrippedObjectName() -> str:
    """Returns the object name for the raw_stripped.txt file within the bucket.

    Defaults to 'raw_stripped.txt' at the bucket root. Can be overridden via
    the environment variable GCS_RAW_STRIPPED_OBJECT.
    """
    objectName: Optional[str] = os.getenv("GCS_RAW_STRIPPED_OBJECT")
    return objectName or "raw_stripped.txt"


def getUserSelectionObjectName() -> str:
    """Returns the object name for the USER_SELECTION.json file within the bucket.

    Defaults to 'USER_SELECTION.json' at the bucket root. Can be overridden via
    the environment variable GCS_USER_SELECTION_OBJECT.
    """
    objectName: Optional[str] = os.getenv("GCS_USER_SELECTION_OBJECT")
    return objectName or "USER_SELECTION.json"


def getXaiApiKey() -> Optional[str]:
    """Returns the xAI API key for Grok API.

    Reads from environment variable XAI_API_KEY or Streamlit secrets.
    Returns None if not found (app will work in fallback mode).
    """
    apiKey: Optional[str] = os.getenv("XAI_API_KEY")

    # Try to get from Streamlit secrets
    if not apiKey:
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'xai' in st.secrets:
                apiKey = st.secrets['xai']['api_key']
        except Exception:
            pass

    return apiKey


