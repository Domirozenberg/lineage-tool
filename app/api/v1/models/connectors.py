"""Request/response Pydantic models for the connector API endpoints."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# PostgreSQL connector request models
# ---------------------------------------------------------------------------


class PostgreSQLExtractRequest(BaseModel):
    source_name: str = Field(..., min_length=1, max_length=255)
    host: str = "localhost"
    port: int = Field(default=5433, ge=1, le=65535)
    dbname: str = "lineage_sample"
    user: str = "lineage"
    password: str = "lineage"
    schemas: Optional[list[str]] = Field(
        default=None,
        description="Optional list of schema names to extract. Defaults to all non-system schemas.",
    )
    include_column_lineage: bool = True


class PostgreSQLTestRequest(BaseModel):
    host: str = "localhost"
    port: int = Field(default=5433, ge=1, le=65535)
    dbname: str = "lineage_sample"
    user: str = "lineage"
    password: str = "lineage"


# ---------------------------------------------------------------------------
# Offline mode request model
# ---------------------------------------------------------------------------


class OfflineValidateRequest(BaseModel):
    folder_path: str = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class PostgreSQLExtractResponse(BaseModel):
    source_id: str
    objects: int
    columns: int
    lineage_edges: int
    duration_seconds: float
    slow_warning: Optional[str] = None


class PostgreSQLTestResponse(BaseModel):
    connected: bool
    version: Optional[str] = None
    schemas: list[str] = Field(default_factory=list)
    error: Optional[str] = None


class OfflineValidateResponse(BaseModel):
    valid: bool
    errors: list[str] = Field(default_factory=list)
    files: dict[str, Any] = Field(default_factory=dict)


class ConnectorStatusResponse(BaseModel):
    connector: str = "postgresql"
    status: str = "available"
    auth_modes: list[str] = Field(default_factory=list)
    version: str = "1.0.0"
