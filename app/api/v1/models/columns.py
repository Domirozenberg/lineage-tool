"""API request/response models for Column."""

from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from app.models.schema import Column


class ColumnCreate(BaseModel):
    object_id: UUID
    name: str = Field(..., min_length=1, max_length=255)
    data_type: Optional[str] = None
    ordinal_position: Optional[int] = Field(default=None, ge=0)
    is_nullable: bool = True
    is_primary_key: bool = False
    description: Optional[str] = None
    extra_metadata: dict[str, Any] = Field(default_factory=dict)

    def to_domain(self) -> Column:
        return Column(**self.model_dump())


class ColumnUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    data_type: Optional[str] = None
    ordinal_position: Optional[int] = Field(default=None, ge=0)
    is_nullable: Optional[bool] = None
    is_primary_key: Optional[bool] = None
    description: Optional[str] = None
    extra_metadata: Optional[dict[str, Any]] = None

    def apply_to(self, col: Column) -> Column:
        return col.model_copy(
            update={k: v for k, v in self.model_dump().items() if v is not None}
        )


ColumnResponse = Column


class ColumnListResponse(BaseModel):
    items: list[Column]
    count: int
