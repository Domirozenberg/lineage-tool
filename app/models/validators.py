"""JSON Schema definitions and validation helpers for platform-specific metadata.

Each entry in PLATFORM_METADATA_SCHEMAS / OBJECT_TYPE_METADATA_SCHEMAS defines
the *optional* additional properties that the extra_metadata dict may carry for a
given platform or object type.  All schemas use ``additionalProperties: true`` so
that unknown keys are accepted (forward-compatibility).

Usage::

    from app.models.validators import validate_metadata, PLATFORM_METADATA_SCHEMAS
    errors = validate_metadata(data_source.extra_metadata,
                               PLATFORM_METADATA_SCHEMAS["postgresql"])
    if errors:
        raise ValueError(errors)
"""

from typing import Any

import jsonschema
from jsonschema import ValidationError

# ---------------------------------------------------------------------------
# Platform-level metadata schemas
# ---------------------------------------------------------------------------

PLATFORM_METADATA_SCHEMAS: dict[str, dict[str, Any]] = {
    "postgresql": {
        "type": "object",
        "properties": {
            "encoding": {"type": "string"},
            "locale": {"type": "string"},
            "tablespace": {"type": "string"},
            "max_connections": {"type": "integer", "minimum": 1},
            "ssl_mode": {
                "type": "string",
                "enum": ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"],
            },
        },
        "additionalProperties": True,
    },
    "snowflake": {
        "type": "object",
        "properties": {
            "account": {"type": "string"},
            "warehouse": {"type": "string"},
            "role": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "bigquery": {
        "type": "object",
        "properties": {
            "project_id": {"type": "string"},
            "location": {"type": "string"},
            "credentials_path": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "tableau": {
        "type": "object",
        "properties": {
            "site_id": {"type": "string"},
            "server_url": {"type": "string"},
            "api_version": {"type": "string"},
            "content_url": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "powerbi": {
        "type": "object",
        "properties": {
            "tenant_id": {"type": "string"},
            "workspace_id": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "dbt": {
        "type": "object",
        "properties": {
            "project_name": {"type": "string"},
            "profiles_dir": {"type": "string"},
            "target": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "airflow": {
        "type": "object",
        "properties": {
            "base_url": {"type": "string"},
            "api_version": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "cube": {
        "type": "object",
        "properties": {
            "api_url": {"type": "string"},
            "cube_cloud_url": {"type": "string"},
            "deployment_id": {"type": "string"},
        },
        "additionalProperties": True,
    },
}

# ---------------------------------------------------------------------------
# Object-type-level metadata schemas
# ---------------------------------------------------------------------------

OBJECT_TYPE_METADATA_SCHEMAS: dict[str, dict[str, Any]] = {
    "table": {
        "type": "object",
        "properties": {
            "row_count": {"type": "integer", "minimum": 0},
            "size_bytes": {"type": "integer", "minimum": 0},
            "partition_key": {"type": "string"},
            "is_partitioned": {"type": "boolean"},
            "storage_format": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "view": {
        "type": "object",
        "properties": {
            "is_updatable": {"type": "boolean"},
            "check_option": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "dashboard": {
        "type": "object",
        "properties": {
            "owner": {"type": "string"},
            "project": {"type": "string"},
            "url": {"type": "string"},
            "thumbnail_url": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "model": {
        "type": "object",
        "properties": {
            "materialization": {
                "type": "string",
                "enum": ["table", "view", "incremental", "ephemeral"],
            },
            "tags": {"type": "array", "items": {"type": "string"}},
            "depends_on": {"type": "array", "items": {"type": "string"}},
        },
        "additionalProperties": True,
    },
    "semantic_model": {
        "type": "object",
        "properties": {
            # dbt Semantic Layer / MetricFlow
            "model": {"type": "string"},
            "entities": {"type": "array", "items": {"type": "string"}},
            "dimensions": {"type": "array", "items": {"type": "string"}},
            "measures": {"type": "array", "items": {"type": "string"}},
            # LookML
            "explore": {"type": "string"},
            "lookml_view": {"type": "string"},
            # Cube.js
            "cube_name": {"type": "string"},
            "data_source": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "metric": {
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": ["simple", "ratio", "cumulative", "derived", "conversion"],
            },
            "expression": {"type": "string"},
            "filter": {"type": "string"},
            "time_granularity": {
                "type": "string",
                "enum": ["day", "week", "month", "quarter", "year"],
            },
            "tags": {"type": "array", "items": {"type": "string"}},
        },
        "additionalProperties": True,
    },
}


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------


def validate_metadata(metadata: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    """Validate *metadata* against *schema* using JSON Schema Draft 7.

    Returns a (possibly empty) list of human-readable error messages.
    An empty list means the data is valid.
    """
    errors: list[str] = []
    try:
        jsonschema.validate(instance=metadata, schema=schema)
    except ValidationError as exc:
        errors.append(exc.message)
    except jsonschema.SchemaError as exc:
        errors.append(f"Invalid schema definition: {exc.message}")
    return errors


def get_platform_schema(platform: str) -> dict[str, Any] | None:
    """Return the JSON schema for *platform*, or None if not defined."""
    return PLATFORM_METADATA_SCHEMAS.get(platform.lower())


def get_object_type_schema(object_type: str) -> dict[str, Any] | None:
    """Return the JSON schema for *object_type*, or None if not defined."""
    return OBJECT_TYPE_METADATA_SCHEMAS.get(object_type.lower())
