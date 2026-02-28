"""REST endpoints for Column — /api/v1/columns."""

from typing import Annotated, Optional
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.v1.dependencies import DbSession, PaginationDep
from app.api.v1.models.columns import (
    ColumnCreate,
    ColumnListResponse,
    ColumnResponse,
    ColumnUpdate,
)
from app.core.errors import NotFoundError
from app.db.repositories.column import ColumnRepository

router = APIRouter(prefix="/columns", tags=["columns"])


@router.post("/", response_model=ColumnResponse, status_code=status.HTTP_201_CREATED)
def create_column(body: ColumnCreate, session: DbSession) -> ColumnResponse:
    repo = ColumnRepository(session)
    col = body.to_domain()
    return repo.create(col)


@router.get("/", response_model=ColumnListResponse)
def list_columns(
    session: DbSession,
    pagination: PaginationDep,
    object_id: Annotated[Optional[UUID], Query(description="Filter by DataObject ID")] = None,
) -> ColumnListResponse:
    repo = ColumnRepository(session)
    items = repo.list_by_object(object_id) if object_id else repo.list_all()
    paged = items[pagination.skip : pagination.skip + pagination.limit]
    return ColumnListResponse(items=paged, count=len(items))


@router.get("/{column_id}", response_model=ColumnResponse)
def get_column(column_id: UUID, session: DbSession) -> ColumnResponse:
    repo = ColumnRepository(session)
    col = repo.get_by_id(column_id)
    if col is None:
        raise NotFoundError("Column", column_id)
    return col


@router.put("/{column_id}", response_model=ColumnResponse)
def update_column(
    column_id: UUID, body: ColumnUpdate, session: DbSession
) -> ColumnResponse:
    repo = ColumnRepository(session)
    col = repo.get_by_id(column_id)
    if col is None:
        raise NotFoundError("Column", column_id)
    updated = body.apply_to(col)
    return repo.update(updated)


@router.delete("/{column_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_column(column_id: UUID, session: DbSession) -> None:
    repo = ColumnRepository(session)
    if not repo.delete(column_id):
        raise NotFoundError("Column", column_id)
