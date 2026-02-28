"""Data models package.

Public surface area — import from here rather than from sub-modules directly.
"""

from app.models.schema import (
    CURRENT_SCHEMA_VERSION,
    Column,
    ColumnLineageMap,
    DataObject,
    DataObjectType,
    DataSource,
    Lineage,
    LineageBaseModel,
    LineageType,
    Platform,
)
from app.models.validators import (
    OBJECT_TYPE_METADATA_SCHEMAS,
    PLATFORM_METADATA_SCHEMAS,
    get_object_type_schema,
    get_platform_schema,
    validate_metadata,
)

__all__ = [
    # Schema version
    "CURRENT_SCHEMA_VERSION",
    # Base
    "LineageBaseModel",
    # Enums
    "Platform",
    "DataObjectType",
    "LineageType",
    # Entities
    "DataSource",
    "DataObject",
    "Column",
    "ColumnLineageMap",
    "Lineage",
    # Validation
    "PLATFORM_METADATA_SCHEMAS",
    "OBJECT_TYPE_METADATA_SCHEMAS",
    "validate_metadata",
    "get_platform_schema",
    "get_object_type_schema",
]
