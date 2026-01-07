"""
Schema Registry for Kafka Messages
Manages schema evolution and validation for Yelp data streams
"""
import json
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import hashlib

logger = logging.getLogger(__name__)


class SchemaType(Enum):
    """Supported schema types"""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class CompatibilityMode(Enum):
    """Schema compatibility modes"""
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    NONE = "none"


@dataclass
class SchemaVersion:
    """Schema version metadata"""
    version: int
    schema_id: str
    schema_content: Dict[str, Any]
    schema_type: SchemaType
    created_at: datetime
    checksum: str
    compatibility: CompatibilityMode = CompatibilityMode.BACKWARD
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'version': self.version,
            'schema_id': self.schema_id,
            'schema_content': self.schema_content,
            'schema_type': self.schema_type.value,
            'created_at': self.created_at.isoformat(),
            'checksum': self.checksum,
            'compatibility': self.compatibility.value
        }


class SchemaRegistry:
    """Simple file-based schema registry"""
    
    def __init__(self, registry_path: str = "schemas/"):
        self.registry_path = registry_path
        self.schemas: Dict[str, List[SchemaVersion]] = {}
        
        # Ensure directory exists
        import os
        os.makedirs(self.registry_path, exist_ok=True)
        
        # Load existing schemas
        self._load_schemas()
    
    def _calculate_checksum(self, schema_content: Dict[str, Any]) -> str:
        """Calculate checksum for schema content"""
        schema_str = json.dumps(schema_content, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]
    
    def _load_schemas(self):
        """Load schemas from filesystem"""
        import os
        import glob
        
        schema_files = glob.glob(os.path.join(self.registry_path, "*.json"))
        
        for schema_file in schema_files:
            try:
                with open(schema_file, 'r') as f:
                    schema_data = json.load(f)
                
                schema_version = SchemaVersion(
                    version=schema_data['version'],
                    schema_id=schema_data['schema_id'],
                    schema_content=schema_data['schema_content'],
                    schema_type=SchemaType(schema_data['schema_type']),
                    created_at=datetime.fromisoformat(schema_data['created_at']),
                    checksum=schema_data['checksum'],
                    compatibility=CompatibilityMode(schema_data.get('compatibility', 'backward'))
                )
                
                if schema_version.schema_id not in self.schemas:
                    self.schemas[schema_version.schema_id] = []
                
                self.schemas[schema_version.schema_id].append(schema_version)
                
            except Exception as e:
                logger.error(f"Error loading schema from {schema_file}: {e}")
        
        # Sort schemas by version
        for schema_id in self.schemas:
            self.schemas[schema_id].sort(key=lambda x: x.version)
    
    def _save_schema(self, schema_version: SchemaVersion):
        """Save schema to filesystem"""
        import os
        
        filename = f"{schema_version.schema_id}_v{schema_version.version}.json"
        filepath = os.path.join(self.registry_path, filename)
        
        with open(filepath, 'w') as f:
            json.dump(schema_version.to_dict(), f, indent=2)
        
        logger.info(f"Schema saved: {filepath}")
    
    def register_schema(self, schema_id: str, schema_content: Dict[str, Any], 
                       schema_type: SchemaType = SchemaType.JSON,
                       compatibility: CompatibilityMode = CompatibilityMode.BACKWARD) -> SchemaVersion:
        """Register a new schema version"""
        
        checksum = self._calculate_checksum(schema_content)
        
        # Check if schema already exists
        if schema_id in self.schemas:
            # Check if content is the same as latest version
            latest_version = self.schemas[schema_id][-1]
            if latest_version.checksum == checksum:
                logger.info(f"Schema {schema_id} already exists with same content")
                return latest_version
            
            # Create new version
            new_version = latest_version.version + 1
        else:
            new_version = 1
            self.schemas[schema_id] = []
        
        schema_version = SchemaVersion(
            version=new_version,
            schema_id=schema_id,
            schema_content=schema_content,
            schema_type=schema_type,
            created_at=datetime.now(),
            checksum=checksum,
            compatibility=compatibility
        )
        
        # Validate compatibility if not first version
        if new_version > 1:
            if not self._check_compatibility(schema_id, schema_content, compatibility):
                raise ValueError(f"Schema compatibility check failed for {schema_id}")
        
        # Add to registry
        self.schemas[schema_id].append(schema_version)
        
        # Save to filesystem
        self._save_schema(schema_version)
        
        logger.info(f"Registered schema {schema_id} version {new_version}")
        return schema_version
    
    def get_schema(self, schema_id: str, version: Optional[int] = None) -> Optional[SchemaVersion]:
        """Get schema by ID and version"""
        if schema_id not in self.schemas:
            return None
        
        if version is None:
            # Return latest version
            return self.schemas[schema_id][-1]
        
        # Find specific version
        for schema_version in self.schemas[schema_id]:
            if schema_version.version == version:
                return schema_version
        
        return None
    
    def list_schemas(self) -> List[str]:
        """List all schema IDs"""
        return list(self.schemas.keys())
    
    def list_versions(self, schema_id: str) -> List[int]:
        """List all versions for a schema"""
        if schema_id not in self.schemas:
            return []
        
        return [sv.version for sv in self.schemas[schema_id]]
    
    def _check_compatibility(self, schema_id: str, new_schema: Dict[str, Any], 
                           compatibility: CompatibilityMode) -> bool:
        """Check schema compatibility (simplified implementation)"""
        if schema_id not in self.schemas:
            return True
        
        latest_schema = self.schemas[schema_id][-1].schema_content
        
        if compatibility == CompatibilityMode.NONE:
            return True
        
        # Simplified compatibility check
        # In production, use proper Avro/JSON Schema compatibility libraries
        
        if compatibility == CompatibilityMode.BACKWARD:
            # New schema should be able to read data written with old schema
            return self._is_backward_compatible(latest_schema, new_schema)
        
        elif compatibility == CompatibilityMode.FORWARD:
            # Old schema should be able to read data written with new schema
            return self._is_forward_compatible(latest_schema, new_schema)
        
        elif compatibility == CompatibilityMode.FULL:
            # Both backward and forward compatible
            return (self._is_backward_compatible(latest_schema, new_schema) and 
                   self._is_forward_compatible(latest_schema, new_schema))
        
        return True
    
    def _is_backward_compatible(self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> bool:
        """Check backward compatibility (simplified)"""
        # For JSON schema, check if required fields are preserved
        old_required = set(old_schema.get('required', []))
        new_required = set(new_schema.get('required', []))
        
        # All old required fields should still be required
        return old_required.issubset(new_required)
    
    def _is_forward_compatible(self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> bool:
        """Check forward compatibility (simplified)"""
        # For JSON schema, check if new required fields have defaults
        old_required = set(old_schema.get('required', []))
        new_required = set(new_schema.get('required', []))
        
        # New required fields should have been optional before
        new_fields = new_required - old_required
        return len(new_fields) == 0


# Predefined schemas for Yelp data
YELP_SCHEMAS = {
    'yelp.business.v1': {
        "type": "object",
        "required": ["id", "name", "location"],
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "image_url": {"type": ["string", "null"]},
            "is_closed": {"type": "boolean"},
            "url": {"type": "string"},
            "review_count": {"type": "integer", "minimum": 0},
            "rating": {"type": "number", "minimum": 0, "maximum": 5},
            "phone": {"type": ["string", "null"]},
            "display_phone": {"type": ["string", "null"]},
            "price": {"type": ["string", "null"], "enum": ["$", "$$", "$$$", "$$$$", None]},
            "distance": {"type": ["number", "null"]},
            "transactions": {"type": "array", "items": {"type": "string"}},
            "categories": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "alias": {"type": "string"},
                        "title": {"type": "string"}
                    }
                }
            },
            "coordinates": {
                "type": "object",
                "properties": {
                    "latitude": {"type": "number"},
                    "longitude": {"type": "number"}
                }
            },
            "location": {
                "type": "object",
                "required": ["city", "state"],
                "properties": {
                    "address1": {"type": ["string", "null"]},
                    "address2": {"type": ["string", "null"]},
                    "address3": {"type": ["string", "null"]},
                    "city": {"type": "string"},
                    "zip_code": {"type": ["string", "null"]},
                    "country": {"type": "string"},
                    "state": {"type": "string"},
                    "display_address": {"type": "array", "items": {"type": "string"}}
                }
            },
            "_metadata": {
                "type": "object",
                "properties": {
                    "ingested_at": {"type": "string", "format": "date-time"},
                    "source": {"type": "string"},
                    "search_location": {"type": "string"},
                    "producer_id": {"type": "string"},
                    "batch_id": {"type": "string"}
                }
            }
        }
    },
    
    'yelp.review.v1': {
        "type": "object",
        "required": ["review_id", "user_id", "business_id", "stars", "text", "date"],
        "properties": {
            "review_id": {"type": "string"},
            "user_id": {"type": "string"},
            "business_id": {"type": "string"},
            "stars": {"type": "number", "minimum": 1, "maximum": 5},
            "useful": {"type": "integer", "minimum": 0},
            "funny": {"type": "integer", "minimum": 0},
            "cool": {"type": "integer", "minimum": 0},
            "text": {"type": "string", "minLength": 1},
            "date": {"type": "string", "format": "date-time"}
        }
    }
}


def initialize_schema_registry(registry_path: str = "schemas/") -> SchemaRegistry:
    """Initialize schema registry with predefined schemas"""
    registry = SchemaRegistry(registry_path)
    
    # Register predefined schemas
    for schema_id, schema_content in YELP_SCHEMAS.items():
        try:
            registry.register_schema(schema_id, schema_content, SchemaType.JSON)
        except ValueError as e:
            logger.warning(f"Schema {schema_id} already exists: {e}")
    
    return registry


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    registry = initialize_schema_registry()
    
    # List all schemas
    print("Available schemas:")
    for schema_id in registry.list_schemas():
        versions = registry.list_versions(schema_id)
        print(f"  {schema_id}: versions {versions}")
    
    # Get latest business schema
    business_schema = registry.get_schema('yelp.business.v1')
    if business_schema:
        print(f"\nLatest business schema (v{business_schema.version}):")
        print(json.dumps(business_schema.schema_content, indent=2)[:200] + "...")
