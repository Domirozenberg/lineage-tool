"""API request/response models for DataSource."""

from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from app.models.schema import DataSource, Platform


class DataSourceCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    platform: Platform
    description: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    database: Optional[str] = None
    extra_metadata: dict[str, Any] = Field(default_factory=dict)

    def to_domain(self) -> DataSource:
        return DataSource(**self.model_dump())


class DataSourceUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    platform: Optional[Platform] = None
    description: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    database: Optional[str] = None
    extra_metadata: Optional[dict[str, Any]] = None

    def apply_to(self, source: DataSource) -> DataSource:
        """Return a new DataSource with non-None update fields applied."""
        updated = source.model_copy(
            update={k: v for k, v in self.model_dump().items() if v is not None}
        )
        return updated


# Response model — the full domain entity is the response
DataSourceResponse = DataSource


class DataSourceListResponse(BaseModel):
    items: list[DataSource]
    count: int
