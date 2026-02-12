"""Service for managing parametric field operations on DATABASE.json."""

from typing import Any, Dict, List, Optional
from cloud_storage import (
    downloadJson,
    getStorageClient,
    loadCredentialsFromAptJson,
    uploadJsonWithPreconditions,
)
from config import (
    getAptJsonPath,
    getBucketName,
    getDatabaseObjectName,
)


class ParametricsService:
    """Service for parametric field operations (craziness, isSexual, madeFor) on DATABASE.json."""
    
    def __init__(self):
        """Initialize the parametrics service."""
        self._client = None
        self._bucket_name = None
        self._object_name = None
    
    def _get_client(self):
        """Get or create the storage client."""
        if self._client is None:
            credentials = loadCredentialsFromAptJson(getAptJsonPath())
            self._client = getStorageClient(credentials)
            self._bucket_name = getBucketName()
            self._object_name = getDatabaseObjectName()
        return self._client
    
    def load_all_database_entries(self) -> List[Dict[str, Any]]:
        """Load all database entries from cloud storage."""
        try:
            client = self._get_client()
            data, _generation = downloadJson(client, self._bucket_name, self._object_name)
            if not isinstance(data, list):
                return []
            return data
        except Exception:
            return []
    
    def save_database(self, data: List[Dict[str, Any]], generation: Optional[int] = None) -> bool:
        """Save the full database back to cloud storage."""
        try:
            client = self._get_client()
            uploadJsonWithPreconditions(
                client=client,
                bucketName=self._bucket_name,
                objectName=self._object_name,
                data=data,
                ifGenerationMatch=generation
            )
            return True
        except Exception:
            return False
    
    def get_parametrics_stats(self) -> Dict[str, Any]:
        """Get statistics about the parametrics data."""
        try:
            data = self.load_all_database_entries()
            if not data:
                return {}
            
            # Only count parameterized items for stats
            parameterized = [r for r in data if "craziness" in r]
            
            stats = {
                "total_count": len(data),
                "parameterized_count": len(parameterized),
                "boys_count": len([r for r in parameterized if r.get("madeFor") == "boys"]),
                "girls_count": len([r for r in parameterized if r.get("madeFor") == "girls"]),
                "sexual_count": len([r for r in parameterized if r.get("isSexual", False)]),
                "avg_craziness": sum(r.get("craziness", 0) for r in parameterized) / len(parameterized) if parameterized else 0,
                "craziness_distribution": {}
            }
            
            craziness_counts = {}
            for item in parameterized:
                level = item.get("craziness", 0)
                craziness_counts[level] = craziness_counts.get(level, 0) + 1
            stats["craziness_distribution"] = craziness_counts
            
            return stats
        except Exception:
            return {}
    
    def _validate_parametric_item(self, item: Dict[str, Any]) -> bool:
        """Validate that a parametric item has the correct structure."""
        required_fields = ["prompt", "craziness", "isSexual"]
        
        for field in required_fields:
            if field not in item:
                return False
        
        if not isinstance(item["prompt"], str) or not item["prompt"].strip():
            return False
        
        if not isinstance(item["craziness"], int) or not (1 <= item["craziness"] <= 10):
            return False
        
        if not isinstance(item["isSexual"], bool):
            return False
        
        if "madeFor" in item:
            if not isinstance(item["madeFor"], str) or item["madeFor"] not in ["boys", "girls", "both"]:
                return False
        
        return True
    
    def clear_all_parametric_fields(self) -> int:
        """Clear parametric fields (craziness, isSexual, madeFor) from all entries.
        
        Does NOT delete entries -- only removes the parametric classification data.
        Returns the number of entries that had parametric fields cleared.
        """
        try:
            client = self._get_client()
            current_data, generation = downloadJson(client, self._bucket_name, self._object_name)
            
            if not isinstance(current_data, list) or not current_data:
                return 0
            
            cleared_count = 0
            for item in current_data:
                had_parametrics = False
                for field in ("craziness", "isSexual", "madeFor"):
                    if field in item:
                        del item[field]
                        had_parametrics = True
                if had_parametrics:
                    cleared_count += 1
            
            if cleared_count > 0:
                self.save_database(current_data, generation)
            
            return cleared_count
        except Exception as e:
            print(f"Clear parametrics error: {e}")
            return 0
