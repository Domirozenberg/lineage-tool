"""Universal metadata schema for the lineage tool.

Core entity types:
  DataSource   — a connected platform (e.g. PostgreSQL instance, Tableau server)
  DataObject   — a data entity within a source (table, view, dashboard, …)
  Column       — a column/field within a DataObject
  Lineage      — a directional relationship between two DataObjects

Each entity carries:
  - schema_version  tracks the model version used at creation time
  - created_at / updated_at  audit timestamps (UTC)
  - extra_metadata  free-form dict for platform-specific fields
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, computed_field, field_validator

CURRENT_SCHEMA_VERSION = "1.1.0"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class Platform(str, Enum):
    """Supported source platforms."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    MSSQL = "mssql"
    ORACLE = "oracle"
    SQLITE = "sqlite"
    TABLEAU = "tableau"
    POWERBI = "powerbi"
    LOOKER = "looker"
    QLIK = "qlik"
    METABASE = "metabase"
    DBT = "dbt"
    AIRFLOW = "airflow"
    SPARK = "spark"
    KAFKA = "kafka"
    CUBE = "cube"
    UNKNOWN = "unknown"


class DataObjectType(str, Enum):
    """Supported data object kinds."""

    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    PROCEDURE = "procedure"
    FUNCTION = "function"
    DASHBOARD = "dashboard"
    WORKSHEET = "worksheet"
    CHART = "chart"
    DATASET = "dataset"
    REPORT = "report"
    MODEL = "model"
    SEMANTIC_MODEL = "semantic_model"
    METRIC = "metric"
    TASK = "task"
    DAG = "dag"
    TOPIC = "topic"
    UNKNOWN = "unknown"


class LineageType(str, Enum):
    """How a target object is derived from its source."""

    DIRECT = "direct"
    DERIVED = "derived"
    AGGREGATED = "aggregated"
    TRANSFORMED = "transformed"
    REFERENCE = "reference"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Base model
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class LineageBaseModel(BaseModel):
    """Shared audit fields present on every entity."""

    schema_version: str = Field(
        default=CURRENT_SCHEMA_VERSION,
        description="Schema version used when this record was created.",
    )
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Core entities
# ---------------------------------------------------------------------------


class DataSource(LineageBaseModel):
    """A connected platform or system."""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., min_length=1, max_length=255)
    platform: Platform
    description: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    database: Optional[str] = None
    extra_metadata: dict[str, Any] = Field(default_factory=dict)


class DataObject(LineageBaseModel):
    """A data entity (table, view, dashboard, …) that lives inside a DataSource."""

    id: UUID = Field(default_factory=uuid4)
    source_id: UUID
    object_type: DataObjectType
    name: str = Field(..., min_length=1, max_length=255)
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    description: Optional[str] = None
    sql_definition: Optional[str] = None
    extra_metadata: dict[str, Any] = Field(default_factory=dict)

    @computed_field  # type: ignore[misc]
    @property
    def qualified_name(self) -> str:
        """Dot-separated fully-qualified name, omitting empty segments."""
        parts = [p for p in [self.database_name, self.schema_name, self.name] if p]
        return ".".join(parts)


class Column(LineageBaseModel):
    """A column or field within a DataObject."""

    id: UUID = Field(default_factory=uuid4)
    object_id: UUID
    name: str = Field(..., min_length=1, max_length=255)
    data_type: Optional[str] = None
    ordinal_position: Optional[int] = Field(default=None, ge=0)
    is_nullable: bool = True
    is_primary_key: bool = False
    description: Optional[str] = None
    extra_metadata: dict[str, Any] = Field(default_factory=dict)


class ColumnLineageMap(BaseModel):
    """Maps one source column to one target column within a Lineage record."""

    source_column_id: UUID
    target_column_id: UUID
    transformation: Optional[str] = None


class Lineage(LineageBaseModel):
    """A directional data-flow relationship between two DataObjects."""

    id: UUID = Field(default_factory=uuid4)
    source_object_id: UUID
    target_object_id: UUID
    lineage_type: LineageType = LineageType.DIRECT
    column_mappings: list[ColumnLineageMap] = Field(default_factory=list)
    sql: Optional[str] = None
    description: Optional[str] = None
    extra_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("target_object_id")
    @classmethod
    def source_and_target_must_differ(cls, v: UUID, info: Any) -> UUID:
        if "source_object_id" in info.data and v == info.data["source_object_id"]:
            raise ValueError("source_object_id and target_object_id must be different")
        return v
