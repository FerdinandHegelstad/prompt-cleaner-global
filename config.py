#!config.py
"""Configuration helpers for secrets.

This module supports two secret sources:

1. Local development via environment variables loaded from a `.env` file.
2. Streamlit deployment via `st.secrets`.

All other secret-loading paths have been removed to keep behaviour explicit.
"""

import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _st_secrets() -> dict:
    """Safely return Streamlit secrets or an empty dict."""
    try:
        import streamlit as st  # type: ignore

        return st.secrets
    except Exception:
        return {}


def getBucketName() -> str:
    """Get the Google Cloud Storage bucket name.

    The value is read from the environment variable ``GCS_BUCKET`` or from
    ``[environment].GCS_BUCKET`` in Streamlit secrets. If neither are provided
    a ``RuntimeError`` is raised.
    """

    bucketName: Optional[str] = os.getenv("GCS_BUCKET")
    if not bucketName:
        bucketName = _st_secrets().get("environment", {}).get("GCS_BUCKET")
    if not bucketName:
        raise RuntimeError("GCS_BUCKET not set in environment or Streamlit secrets")
    return bucketName


def getDatabaseObjectName() -> str:
    """Return the GCS object name for the global database JSON."""
    objectName: Optional[str] = os.getenv("GCS_DATABASE_OBJECT")
    return objectName or "DATABASE.json"


def getAptJsonPath() -> str:
    """Return the file path to the service account JSON (APT.json).

    ``APT_JSON_PATH`` is used verbatim if set. If omitted, ``APT.json`` in the
    project root is used. Setting ``APT_JSON_PATH`` to an empty string signals
    that credentials should be loaded exclusively from Streamlit secrets.
    """

    aptPath = os.getenv("APT_JSON_PATH")
    if aptPath is None:
        aptPath = _st_secrets().get("environment", {}).get("APT_JSON_PATH")
    if aptPath is None:
        return "APT.json"
    return aptPath


def getRawStrippedObjectName() -> str:
    """Return the object name for ``raw_stripped.txt`` within the bucket."""
    objectName: Optional[str] = os.getenv("GCS_RAW_STRIPPED_OBJECT")
    return objectName or "raw_stripped.txt"


def getUserSelectionObjectName() -> str:
    """Return the object name for ``USER_SELECTION.json`` within the bucket."""
    objectName: Optional[str] = os.getenv("GCS_USER_SELECTION_OBJECT")
    return objectName or "USER_SELECTION.json"


def getXaiApiKey() -> Optional[str]:
    """Return the xAI API key for Grok API."""
    return os.getenv("XAI_API_KEY") or _st_secrets().get("xai", {}).get("XAI_API_KEY")


def getXaiBaseUrl() -> str:
    """Return the xAI base URL."""
    return (
        os.getenv("XAI_BASE_URL")
        or _st_secrets().get("xai", {}).get("XAI_BASE_URL")
        or "https://api.x.ai/v1"
    )


def getXaiModel() -> str:
    """Return the xAI model name."""
    return (
        os.getenv("XAI_MODEL")
        or _st_secrets().get("xai", {}).get("XAI_MODEL")
        or "grok-3-mini"
    )

