"""REST endpoints for DataObject — /api/v1/objects."""

from typing import Annotated, Optional
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.v1.dependencies import CurrentUser, DbSession, PaginationDep, WriterUser
from app.api.v1.models.objects import (
    DataObjectCreate,
    DataObjectListResponse,
    DataObjectResponse,
    DataObjectUpdate,
)
from app.core.errors import NotFoundError
from app.db.repositories.data_object import DataObjectRepository
from app.models.schema import DataObjectType

router = APIRouter(prefix="/objects", tags=["objects"])


@router.post("/", response_model=DataObjectResponse, status_code=status.HTTP_201_CREATED)
def create_object(body: DataObjectCreate, session: DbSession, _: WriterUser) -> DataObjectResponse:
    repo = DataObjectRepository(session)
    obj = body.to_domain()
    return repo.create(obj)


@router.get("/", response_model=DataObjectListResponse)
def list_objects(
    session: DbSession,
    pagination: PaginationDep,
    _: CurrentUser,
    source_id: Annotated[Optional[UUID], Query(description="Filter by DataSource ID")] = None,
    object_type: Annotated[
        Optional[DataObjectType], Query(description="Filter by object type")
    ] = None,
) -> DataObjectListResponse:
    repo = DataObjectRepository(session)
    if source_id:
        items = repo.list_by_source(source_id)
    elif object_type:
        items = repo.list_by_type(object_type)
    else:
        items = repo.list_all()
    paged = items[pagination.skip : pagination.skip + pagination.limit]
    return DataObjectListResponse(items=paged, count=len(items))


@router.get("/{object_id}", response_model=DataObjectResponse)
def get_object(object_id: UUID, session: DbSession, _: CurrentUser) -> DataObjectResponse:
    repo = DataObjectRepository(session)
    obj = repo.get_by_id(object_id)
    if obj is None:
        raise NotFoundError("DataObject", object_id)
    return obj


@router.put("/{object_id}", response_model=DataObjectResponse)
def update_object(
    object_id: UUID, body: DataObjectUpdate, session: DbSession, _: WriterUser
) -> DataObjectResponse:
    repo = DataObjectRepository(session)
    obj = repo.get_by_id(object_id)
    if obj is None:
        raise NotFoundError("DataObject", object_id)
    updated = body.apply_to(obj)
    return repo.update(updated)


@router.delete("/{object_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_object(object_id: UUID, session: DbSession, _: WriterUser) -> None:
    repo = DataObjectRepository(session)
    if not repo.delete(object_id):
        raise NotFoundError("DataObject", object_id)
