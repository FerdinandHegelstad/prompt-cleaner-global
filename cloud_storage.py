#!cloud_storage.py
import json
from typing import Any, Dict, List, Optional, Tuple

from google.auth.credentials import Credentials  # type: ignore
from google.oauth2 import service_account  # type: ignore
from google.cloud import storage  # type: ignore


def loadCredentialsFromAptJson(aptJsonPath: str) -> Credentials:
    """Load Google service account credentials from local file or Streamlit GCS connection.

    Args:
        aptJsonPath: Absolute or project-relative path to APT.json.
                 If empty string, tries to use Streamlit GCS connection.

    Returns:
        Google Auth credentials object.
    """
    # Try to use Streamlit GCS connection first (for cloud deployment)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'connections' in st.secrets and 'gcs' in st.secrets['connections']:
            print("DEBUG: Using Streamlit GCS connection")
            # Create credentials from the connection parameters
            gcs_config = st.secrets['connections']['gcs']
            credentials_dict = {
                'type': gcs_config['type'],
                'project_id': gcs_config['project_id'],
                'private_key_id': gcs_config['private_key_id'],
                'private_key': gcs_config['private_key'],
                'client_email': gcs_config['client_email'],
                'client_id': gcs_config['client_id'],
                'auth_uri': gcs_config['auth_uri'],
                'token_uri': gcs_config['token_uri'],
                'auth_provider_x509_cert_url': gcs_config['auth_provider_x509_cert_url'],
                'client_x509_cert_url': gcs_config['client_x509_cert_url']
            }
            return service_account.Credentials.from_service_account_info(credentials_dict)
        else:
            print("DEBUG: Streamlit GCS connection not available")
    except Exception as e:
        print(f"DEBUG: Error loading from Streamlit GCS connection: {e}")
        import traceback
        print(f"DEBUG: Full traceback: {traceback.format_exc()}")

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


