#!cloud_storage.py
import json
from typing import Any, Dict, List, Optional, Tuple

from google.auth.credentials import Credentials  # type: ignore
from google.oauth2 import service_account  # type: ignore
from google.cloud import storage  # type: ignore


def loadCredentialsFromAptJson(aptJsonPath: str) -> Credentials:
    """Load Google service account credentials from a local JSON file or Streamlit secrets.

    Args:
        aptJsonPath: Absolute or project-relative path to APT.json.
                 If empty string, tries to load from Streamlit secrets.

    Returns:
        Google Auth credentials object.
    """
    # Try to load from Streamlit secrets first (for cloud deployment)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'google_cloud' in st.secrets:
            import json
            credentials_dict = st.secrets['google_cloud']['credentials']
            if isinstance(credentials_dict, str):
                credentials_dict = json.loads(credentials_dict)
            print("DEBUG: Loading credentials from Streamlit secrets")
            return service_account.Credentials.from_service_account_info(credentials_dict)
        else:
            print("DEBUG: Streamlit secrets not available or missing google_cloud section")
    except Exception as e:
        print(f"DEBUG: Error loading from Streamlit secrets: {e}")

    # Fall back to file-based loading
    if not aptJsonPath:
        aptJsonPath = "APT.json"

    print(f"DEBUG: Falling back to file-based loading from: {aptJsonPath}")
    return service_account.Credentials.from_service_account_file(aptJsonPath)


def getStorageClient(credentials: Credentials) -> storage.Client:
    """Create a Google Cloud Storage client using the provided credentials."""
    return storage.Client(credentials=credentials, project=credentials.project_id)


def downloadJson(
    client: storage.Client, bucketName: str, objectName: str
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Download a JSON array from GCS, returning data and object generation.

    If the object does not exist, returns ([], None).
    """
    bucket = client.bucket(bucketName)
    blob = bucket.get_blob(objectName)
    if blob is None:
        return [], None
    content = blob.download_as_text(encoding="utf-8")
    try:
        data = json.loads(content)
        if not isinstance(data, list):
            # Defensive: if the object exists but is not a JSON array, treat as empty
            data = []
    except Exception:
        data = []
    # Generation is an int if present
    generation: Optional[int] = None
    try:
        generation = int(blob.generation) if blob.generation is not None else None
    except Exception:
        generation = None
    return data, generation


def uploadJsonWithPreconditions(
    client: storage.Client,
    bucketName: str,
    objectName: str,
    data: List[Dict[str, Any]],
    ifGenerationMatch: Optional[int],
) -> None:
    """Upload a JSON array to GCS, enforcing an optimistic concurrency precondition.

    If ifGenerationMatch is None and the object exists, the upload will fail.
    If ifGenerationMatch is set but mismatched, the upload will fail.
    """
    bucket = client.bucket(bucketName)
    blob = bucket.blob(objectName)
    jsonText = json.dumps(data, ensure_ascii=False, indent=4)
    if ifGenerationMatch is None:
        # Create only if object does not exist
        blob.upload_from_string(
            jsonText,
            content_type="application/json",
            if_generation_match=0,
        )
    else:
        # Use precondition to avoid lost updates
        blob.upload_from_string(
            jsonText,
            content_type="application/json",
            if_generation_match=ifGenerationMatch,
        )


def downloadTextFile(
    client: storage.Client, bucketName: str, objectName: str
) -> Tuple[str, Optional[int]]:
    """Download a text file from GCS, returning content and object generation.

    If the object does not exist, returns ("", None).
    """
    bucket = client.bucket(bucketName)
    blob = bucket.get_blob(objectName)
    if blob is None:
        return "", None
    content = blob.download_as_text(encoding="utf-8")
    # Generation is an int if present
    generation: Optional[int] = None
    try:
        generation = int(blob.generation) if blob.generation is not None else None
    except Exception:
        generation = None
    return content, generation


def uploadTextFile(
    client: storage.Client,
    bucketName: str,
    objectName: str,
    content: str,
    ifGenerationMatch: Optional[int] = None,
) -> None:
    """Upload a text file to GCS.

    If ifGenerationMatch is None and the object exists, the upload will fail.
    If ifGenerationMatch is set but mismatched, the upload will fail.
    """
    bucket = client.bucket(bucketName)
    blob = bucket.blob(objectName)
    if ifGenerationMatch is None:
        # Create only if object does not exist
        blob.upload_from_string(
            content,
            content_type="text/plain",
            if_generation_match=0,
        )
    else:
        # Use precondition to avoid lost updates
        blob.upload_from_string(
            content,
            content_type="text/plain",
            if_generation_match=ifGenerationMatch,
        )


