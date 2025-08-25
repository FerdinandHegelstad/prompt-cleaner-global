#!config.py
"""
Configuration helpers for local dev (.env) and Streamlit deployment (st.secrets).

This module:
- Reads env vars from a local `.env` (for dev) via python-dotenv.
- Reads production secrets from Streamlit's Secrets tab (st.secrets).
- Provides small helpers for GCS + xAI config.
- Optionally builds a google.cloud.storage.Client from either a JSON file on disk
  or the `[connections.gcs]` block in st.secrets when `APT_JSON_PATH=""`.

Conventions expected in Streamlit Secrets (TOML):

[environment]
GCS_BUCKET = "your_bucket"
APT_JSON_PATH = ""  # empty string => use [connections.gcs] from st.secrets

[xai]
XAI_API_KEY = "..."
# optional:
# XAI_BASE_URL = "https://api.x.ai/v1"
# XAI_MODEL    = "grok-3-mini"

[connections.gcs]
type = "service_account"
project_id = "..."
private_key_id = "..."
private_key = "\""-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----"\""
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "..."
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()


# ---------- internals ----------

def _st_secrets() -> Dict[str, Any]:
    """
    Safely return Streamlit secrets or an empty dict if Streamlit is unavailable.
    """
    try:
        import streamlit as st  # type: ignore
    except ImportError:
        return {}
    # getattr() so we don't explode if st.secrets doesn't exist in some envs
    return dict(getattr(st, "secrets", {}))


def _get_from_nested(mapping: Dict[str, Any], *keys: str) -> Optional[Any]:
    """
    Walk nested dictionaries with keys like ("environment", "GCS_BUCKET").
    Returns None if any level is missing.
    """
    cur: Any = mapping
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


# ---------- public getters ----------

def getBucketName() -> str:
    """
    Get the Google Cloud Storage bucket name.
    Reads env GCS_BUCKET, then st.secrets["environment"]["GCS_BUCKET"].
    """
    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        bucket = _get_from_nested(_st_secrets(), "environment", "GCS_BUCKET")
    if not bucket or not isinstance(bucket, str):
        raise RuntimeError("GCS_BUCKET not set in environment or Streamlit secrets")
    return bucket


def getDatabaseObjectName() -> str:
    """
    Return the GCS object name for the global database JSON.
    Env var: GCS_DATABASE_OBJECT  (default: 'DATABASE.json')
    """
    return os.getenv("GCS_DATABASE_OBJECT") or "DATABASE.json"


def getRawStrippedObjectName() -> str:
    """
    Return the object name for 'raw_stripped.txt' within the bucket.
    Env var: GCS_RAW_STRIPPED_OBJECT  (default: 'raw_stripped.txt')
    """
    return os.getenv("GCS_RAW_STRIPPED_OBJECT") or "raw_stripped.txt"


def getUserSelectionObjectName() -> str:
    """
    Return the object name for 'USER_SELECTION.json' within the bucket.
    Env var: GCS_USER_SELECTION_OBJECT  (default: 'USER_SELECTION.json')
    """
    return os.getenv("GCS_USER_SELECTION_OBJECT") or "USER_SELECTION.json"


def getAptJsonPath() -> str:
    """
    Return the file path to the service account JSON (APT.json).

    Priority:
      1) env APT_JSON_PATH
      2) st.secrets["environment"]["APT_JSON_PATH"]
      3) default "APT.json"

    Special case: empty string ("") indicates "use Streamlit [connections.gcs] only".
    """
    apt = os.getenv("APT_JSON_PATH")
    if apt is None:
        apt = _get_from_nested(_st_secrets(), "environment", "APT_JSON_PATH")
    if apt is None:
        return "APT.json"
    # allow explicit "" sentinel
    return apt


def getXaiApiKey() -> Optional[str]:
    """
    Return the xAI API key for Grok API.
    Env var: XAI_API_KEY
    Secrets: st.secrets["xai"]["XAI_API_KEY"]
    """
    return os.getenv("XAI_API_KEY") or _get_from_nested(_st_secrets(), "xai", "XAI_API_KEY")


def getXaiBaseUrl() -> str:
    """
    Return the xAI base URL.
    Env var: XAI_BASE_URL
    Secrets: st.secrets["xai"]["XAI_BASE_URL"]
    Default: "https://api.x.ai/v1"
    """
    return (
        os.getenv("XAI_BASE_URL")
        or _get_from_nested(_st_secrets(), "xai", "XAI_BASE_URL")
        or "https://api.x.ai/v1"
    )


def getXaiModel() -> str:
    """
    Return the xAI model name.
    Env var: XAI_MODEL
    Secrets: st.secrets["xai"]["XAI_MODEL"]
    Default: "grok-3-mini"
    """
    return (
        os.getenv("XAI_MODEL")
        or _get_from_nested(_st_secrets(), "xai", "XAI_MODEL")
        or "grok-3-mini"
    )


# ---------- optional helpers for GCS ----------

def build_gcs_client():
    """
    Create and return a google.cloud.storage.Client using either:
      - A JSON credentials file (when APT_JSON_PATH is a non-empty path), or
      - Streamlit secrets [connections.gcs] (when APT_JSON_PATH == "").

    Raises informative RuntimeError if misconfigured.
    """
    try:
        from google.cloud import storage  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "google-cloud-storage is not installed. "
            "Install with: pip install google-cloud-storage"
        ) from e

    apt_path = getAptJsonPath()

    if apt_path == "":
        secrets = _st_secrets()
        conn = _get_from_nested(secrets, "connections", "gcs")
        if not isinstance(conn, dict) or not conn:
            raise RuntimeError(
                "APT_JSON_PATH is empty (use secrets), but [connections.gcs] "
                "is missing in Streamlit secrets."
            )
        # Expect a full service-account dict
        try:
            return storage.Client.from_service_account_info(conn)  # type: ignore[arg-type]
        except Exception as e:
            raise RuntimeError(
                "Failed to build GCS client from st.secrets['connections']['gcs']. "
                "Verify the service account block and private_key formatting."
            ) from e

    # Otherwise use file path on disk
    if not os.path.exists(apt_path):
        raise RuntimeError(
            f"Service account file '{apt_path}' not found. "
            "Set APT_JSON_PATH to '' to use st.secrets connections, "
            "or provide a valid path."
        )
    try:
        return storage.Client.from_service_account_json(apt_path)
    except Exception as e:
        raise RuntimeError(
            f"Failed to build GCS client from file '{apt_path}'. "
            "Is the JSON valid service account credentials?"
        ) from e


__all__ = [
    "getBucketName",
    "getDatabaseObjectName",
    "getRawStrippedObjectName",
    "getUserSelectionObjectName",
    "getAptJsonPath",
    "getXaiApiKey",
    "getXaiBaseUrl",
    "getXaiModel",
    "build_gcs_client",
]