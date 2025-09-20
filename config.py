#!config.py
import os
from typing import Optional


def getBucketName() -> str:
    """Returns the Google Cloud Storage bucket name for the global database.

    The value is read from the environment variable GCS_BUCKET or Streamlit secrets.
    Raises a RuntimeError if not set to avoid ambiguous defaults.
    """
    bucketName: Optional[str] = os.getenv("GCS_BUCKET")

    # Try to get from Streamlit secrets
    if not bucketName:
        try:
            # Import streamlit with proper error handling
            try:
                import streamlit as st  # type: ignore
            except ImportError:
                st = None

            if st and hasattr(st, 'secrets'):
                # Try environment section (professional format)
                if 'environment' in st.secrets and 'GCS_BUCKET' in st.secrets['environment']:
                    bucketName = st.secrets['environment']['GCS_BUCKET']
                # Try gcs section as fallback
                elif 'gcs' in st.secrets and 'bucket_name' in st.secrets['gcs']:
                    bucketName = st.secrets['gcs']['bucket_name']
        except Exception:
            pass

    if not bucketName:
        raise RuntimeError(
            "GCS_BUCKET environment variable is not set and bucket_name not found in Streamlit secrets. Set it to the name of your GCS bucket."
        )
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


def getDiscardsObjectName() -> str:
    """Returns the object name for the DISCARDS.json file within the bucket.

    Defaults to 'DISCARDS.json' at the bucket root. Can be overridden via
    the environment variable GCS_DISCARDS_OBJECT.
    """
    objectName: Optional[str] = os.getenv("GCS_DISCARDS_OBJECT")
    return objectName or "DISCARDS.json"


def getParametricsObjectName() -> str:
    """Returns the object name for the PARAMETRICS.json file within the bucket.

    Defaults to 'PARAMETRICS.json' at the bucket root. Can be overridden via
    the environment variable GCS_PARAMETRICS_OBJECT.
    """
    objectName: Optional[str] = os.getenv("GCS_PARAMETRICS_OBJECT")
    return objectName or "PARAMETRICS.json"


def _get_st_secrets():
    """Helper to get Streamlit secrets safely."""
    try:
        import streamlit as st  # type: ignore
        return st.secrets
    except Exception:
        return {}

def _get_nested(*path, default=None):
    """Helper to safely get nested secrets values."""
    s = _get_st_secrets()
    try:
        for p in path:
            s = s[p]
        return s
    except Exception:
        return default

def _first(*vals):
    """Return the first non-empty value from the list."""
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return None

def getXaiApiKey() -> Optional[str]:
    """Returns the xAI API key for Grok API.

    Reads from environment variable XAI_API_KEY or Streamlit secrets.
    Supports both nested [xai] and flat keys.
    Returns None if not found (app will work in fallback mode).
    """
    return _first(
        _get_nested("xai", "XAI_API_KEY"),
        _get_nested("xai", "api_key"),  # Legacy format
        _get_nested("XAI_API_KEY"),
        os.getenv("XAI_API_KEY")
    )

def getXaiBaseUrl() -> str:
    """Returns the xAI base URL.

    Defaults to 'https://api.x.ai/v1' if not found.
    """
    return _first(
        _get_nested("xai", "BASE_URL"),
        _get_nested("XAI_BASE_URL"),
        os.getenv("XAI_BASE_URL"),
        "https://api.x.ai/v1"
    )

def getXaiModel() -> str:
    """Returns the xAI model name.

    Defaults to 'grok-4-fast-reasoning' if not found.
    """
    return _first(
        _get_nested("xai", "MODEL"),
        _get_nested("XAI_MODEL"),
        os.getenv("XAI_MODEL"),
        "grok-4-fast-reasoning"
    )


