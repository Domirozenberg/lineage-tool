"""API request/response models for Lineage."""

from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.models.schema import ColumnLineageMap, Lineage, LineageType


class LineageCreate(BaseModel):
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

    def to_domain(self) -> Lineage:
        return Lineage(**self.model_dump())


class LineageUpdate(BaseModel):
    lineage_type: Optional[LineageType] = None
    column_mappings: Optional[list[ColumnLineageMap]] = None
    sql: Optional[str] = None
    description: Optional[str] = None
    extra_metadata: Optional[dict[str, Any]] = None

    def apply_to(self, lin: Lineage) -> Lineage:
        updates = {k: v for k, v in self.model_dump().items() if v is not None}
        return lin.model_copy(update=updates)


LineageResponse = Lineage


class LineageListResponse(BaseModel):
    items: list[Lineage]
    count: int


class ImpactNode(BaseModel):
    """A node in a downstream/upstream impact traversal result."""

    id: UUID
    name: str
    object_type: str
    source_id: UUID
    depth: int
    lineage_id: str


class ImpactResponse(BaseModel):
    object_id: UUID
    direction: str
    nodes: list[ImpactNode]
