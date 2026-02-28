"""API request/response models for DataObject."""

from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from app.models.schema import DataObject, DataObjectType


class DataObjectCreate(BaseModel):
    source_id: UUID
    object_type: DataObjectType
    name: str = Field(..., min_length=1, max_length=255)
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    description: Optional[str] = None
    sql_definition: Optional[str] = None
    extra_metadata: dict[str, Any] = Field(default_factory=dict)

    def to_domain(self) -> DataObject:
        return DataObject(**self.model_dump())


class DataObjectUpdate(BaseModel):
    object_type: Optional[DataObjectType] = None
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    description: Optional[str] = None
    sql_definition: Optional[str] = None
    extra_metadata: Optional[dict[str, Any]] = None

    def apply_to(self, obj: DataObject) -> DataObject:
        return obj.model_copy(
            update={k: v for k, v in self.model_dump().items() if v is not None}
        )


DataObjectResponse = DataObject


class DataObjectListResponse(BaseModel):
    items: list[DataObject]
    count: int
