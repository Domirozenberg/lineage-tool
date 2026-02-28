"""REST endpoints for DataSource — /api/v1/sources."""

from typing import Annotated, Optional
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.v1.dependencies import DbSession, PaginationDep
from app.api.v1.models.sources import (
    DataSourceCreate,
    DataSourceListResponse,
    DataSourceResponse,
    DataSourceUpdate,
)
from app.core.errors import NotFoundError
from app.db.repositories.data_source import DataSourceRepository
from app.models.schema import Platform

router = APIRouter(prefix="/sources", tags=["sources"])


@router.post("/", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
def create_source(body: DataSourceCreate, session: DbSession) -> DataSourceResponse:
    repo = DataSourceRepository(session)
    source = body.to_domain()
    return repo.create(source)


@router.get("/", response_model=DataSourceListResponse)
def list_sources(
    session: DbSession,
    pagination: PaginationDep,
    platform: Annotated[Optional[Platform], Query(description="Filter by platform")] = None,
) -> DataSourceListResponse:
    repo = DataSourceRepository(session)
    items = repo.list_by_platform(platform) if platform else repo.list_all()
    paged = items[pagination.skip : pagination.skip + pagination.limit]
    return DataSourceListResponse(items=paged, count=len(items))


@router.get("/{source_id}", response_model=DataSourceResponse)
def get_source(source_id: UUID, session: DbSession) -> DataSourceResponse:
    repo = DataSourceRepository(session)
    source = repo.get_by_id(source_id)
    if source is None:
        raise NotFoundError("DataSource", source_id)
    return source


@router.put("/{source_id}", response_model=DataSourceResponse)
def update_source(
    source_id: UUID, body: DataSourceUpdate, session: DbSession
) -> DataSourceResponse:
    repo = DataSourceRepository(session)
    source = repo.get_by_id(source_id)
    if source is None:
        raise NotFoundError("DataSource", source_id)
    updated = body.apply_to(source)
    return repo.update(updated)


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_source(source_id: UUID, session: DbSession) -> None:
    repo = DataSourceRepository(session)
    if not repo.delete(source_id):
        raise NotFoundError("DataSource", source_id)
