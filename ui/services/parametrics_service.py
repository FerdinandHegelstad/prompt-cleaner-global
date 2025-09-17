"""Service for managing parametrics data operations."""

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
    getParametricsObjectName,
)


class ParametricsService:
    """Service for parametrics data management and operations."""
    
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
            self._object_name = getParametricsObjectName()
        return self._client
    
    def load_parametrics(self) -> List[Dict[str, Any]]:
        """Load parametrics data from cloud storage."""
        try:
            client = self._get_client()
            data, _generation = downloadJson(client, self._bucket_name, self._object_name)
            if not isinstance(data, list):
                return []
            return data
        except Exception:
            return []
    
    def save_parametrics(self, data: List[Dict[str, Any]], generation: Optional[int] = None) -> bool:
        """Save parametrics data to cloud storage."""
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
    
    def add_parametric_item(self, item: Dict[str, Any]) -> bool:
        """Add a new parametric item to the cloud storage."""
        try:
            # Load current data
            current_data = self.load_parametrics()
            
            # Validate item structure
            if not self._validate_parametric_item(item):
                return False
            
            # Add the new item
            current_data.append(item)
            
            # Save back to cloud storage
            return self.save_parametrics(current_data)
        except Exception:
            return False
    
    def update_parametric_item(self, index: int, updated_item: Dict[str, Any]) -> bool:
        """Update a specific parametric item by index."""
        try:
            # Load current data
            current_data = self.load_parametrics()
            
            # Check bounds
            if index < 0 or index >= len(current_data):
                return False
            
            # Validate item structure
            if not self._validate_parametric_item(updated_item):
                return False
            
            # Update the item
            current_data[index] = updated_item
            
            # Save back to cloud storage
            return self.save_parametrics(current_data)
        except Exception:
            return False
    
    def delete_parametric_item(self, index: int) -> bool:
        """Delete a specific parametric item by index."""
        try:
            # Load current data
            current_data = self.load_parametrics()
            
            # Check bounds
            if index < 0 or index >= len(current_data):
                return False
            
            # Remove the item
            current_data.pop(index)
            
            # Save back to cloud storage
            return self.save_parametrics(current_data)
        except Exception:
            return False
    
    def get_parametrics_stats(self) -> Dict[str, Any]:
        """Get statistics about the parametrics data."""
        try:
            data = self.load_parametrics()
            if not data:
                return {}
            
            stats = {
                "total_count": len(data),
                "boys_count": len([r for r in data if r.get("madeFor") == "boys"]),
                "girls_count": len([r for r in data if r.get("madeFor") == "girls"]),
                "sexual_count": len([r for r in data if r.get("isSexual", False)]),
                "avg_craziness": sum(r.get("craziness", 0) for r in data) / len(data) if data else 0,
                "craziness_distribution": {}
            }
            
            # Calculate craziness distribution
            craziness_counts = {}
            for item in data:
                level = item.get("craziness", 0)
                craziness_counts[level] = craziness_counts.get(level, 0) + 1
            stats["craziness_distribution"] = craziness_counts
            
            return stats
        except Exception:
            return {}
    
    def _validate_parametric_item(self, item: Dict[str, Any]) -> bool:
        """Validate that a parametric item has the correct structure."""
        required_fields = ["prompt", "craziness", "isSexual", "madeFor"]
        
        # Check all required fields exist
        for field in required_fields:
            if field not in item:
                return False
        
        # Validate field types and values
        if not isinstance(item["prompt"], str) or not item["prompt"].strip():
            return False
        
        if not isinstance(item["craziness"], int) or not (1 <= item["craziness"] <= 10):
            return False
        
        if not isinstance(item["isSexual"], bool):
            return False
        
        if not isinstance(item["madeFor"], str) or item["madeFor"] not in ["boys", "girls", "both"]:
            return False
        
        return True
    
    def search_parametrics(self, query: str, field: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search parametrics data by query string."""
        try:
            data = self.load_parametrics()
            if not data or not query:
                return data
            
            query_lower = query.lower()
            results = []
            
            for item in data:
                if field and field in item:
                    # Search specific field
                    field_value = str(item[field]).lower()
                    if query_lower in field_value:
                        results.append(item)
                else:
                    # Search all text fields
                    prompt = str(item.get("prompt", "")).lower()
                    made_for = str(item.get("madeFor", "")).lower()
                    
                    if query_lower in prompt or query_lower in made_for:
                        results.append(item)
            
            return results
        except Exception:
            return []
    
    def filter_parametrics(self, **filters) -> List[Dict[str, Any]]:
        """Filter parametrics data by various criteria."""
        try:
            data = self.load_parametrics()
            if not data:
                return []
            
            results = data
            
            # Apply filters
            if "madeFor" in filters and filters["madeFor"]:
                results = [r for r in results if r.get("madeFor") == filters["madeFor"]]
            
            if "isSexual" in filters and filters["isSexual"] is not None:
                results = [r for r in results if r.get("isSexual") == filters["isSexual"]]
            
            if "min_craziness" in filters and filters["min_craziness"] is not None:
                results = [r for r in results if r.get("craziness", 0) >= filters["min_craziness"]]
            
            if "max_craziness" in filters and filters["max_craziness"] is not None:
                results = [r for r in results if r.get("craziness", 0) <= filters["max_craziness"]]
            
            return results
        except Exception:
            return []
    
    def delete_parametrics_by_prompts(self, prompts_to_delete: List[str]) -> int:
        """Delete parametrics items by their prompt text."""
        try:
            # Load current data with generation for optimistic concurrency
            client = self._get_client()
            current_data, generation = downloadJson(client, self._bucket_name, self._object_name)
            
            if not isinstance(current_data, list) or not current_data:
                return 0
            
            # Filter out items to delete
            prompts_set = set(prompts_to_delete)
            filtered_data = [item for item in current_data if item.get("prompt", "") not in prompts_set]
            
            # Calculate how many were removed
            removed_count = len(current_data) - len(filtered_data)
            
            if removed_count > 0:
                # Save the filtered data back with generation
                if self.save_parametrics(filtered_data, generation):
                    return removed_count
            
            return 0
        except Exception as e:
            print(f"Delete error: {e}")
            return 0
