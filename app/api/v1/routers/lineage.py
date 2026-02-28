"""REST endpoints for Lineage — /api/v1/lineage."""

from typing import Annotated, Optional
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.v1.dependencies import DbSession
from app.api.v1.models.lineage import (
    ImpactNode,
    ImpactResponse,
    LineageCreate,
    LineageListResponse,
    LineageResponse,
    LineageUpdate,
)
from app.core.errors import NotFoundError
from app.db.repositories.lineage import LineageRepository

router = APIRouter(prefix="/lineage", tags=["lineage"])


@router.post("/", response_model=LineageResponse, status_code=status.HTTP_201_CREATED)
def create_lineage(body: LineageCreate, session: DbSession) -> LineageResponse:
    repo = LineageRepository(session)
    lin = body.to_domain()
    return repo.create(lin)


@router.get("/", response_model=LineageListResponse)
def list_lineage(
    session: DbSession,
    source_object_id: Annotated[
        Optional[UUID], Query(description="Filter by source DataObject ID")
    ] = None,
    target_object_id: Annotated[
        Optional[UUID], Query(description="Filter by target DataObject ID")
    ] = None,
) -> LineageListResponse:
    repo = LineageRepository(session)
    if source_object_id:
        items = repo.list_by_source(source_object_id)
    elif target_object_id:
        items = repo.list_by_target(target_object_id)
    else:
        items = repo.list_all()
    return LineageListResponse(items=items, count=len(items))


@router.get("/{lineage_id}", response_model=LineageResponse)
def get_lineage(lineage_id: UUID, session: DbSession) -> LineageResponse:
    repo = LineageRepository(session)
    lin = repo.get_by_id(lineage_id)
    if lin is None:
        raise NotFoundError("Lineage", lineage_id)
    return lin


@router.put("/{lineage_id}", response_model=LineageResponse)
def update_lineage(
    lineage_id: UUID, body: LineageUpdate, session: DbSession
) -> LineageResponse:
    repo = LineageRepository(session)
    lin = repo.get_by_id(lineage_id)
    if lin is None:
        raise NotFoundError("Lineage", lineage_id)
    updated = body.apply_to(lin)
    return repo.update(updated)


@router.delete("/{lineage_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_lineage(lineage_id: UUID, session: DbSession) -> None:
    repo = LineageRepository(session)
    if not repo.delete(lineage_id):
        raise NotFoundError("Lineage", lineage_id)


@router.get("/impact/{object_id}/downstream", response_model=ImpactResponse)
def get_downstream(
    object_id: UUID,
    session: DbSession,
    max_depth: Annotated[int, Query(ge=1, le=20, description="Max traversal depth")] = 10,
) -> ImpactResponse:
    repo = LineageRepository(session)
    raw = repo.get_downstream(object_id, max_depth=max_depth)
    nodes = [
        ImpactNode(
            id=UUID(r["props"]["id"]),
            name=r["props"]["name"],
            object_type=r["props"]["object_type"],
            source_id=UUID(r["props"]["source_id"]),
            depth=r["depth"],
            lineage_id=r["lineage_id"],
        )
        for r in raw
    ]
    return ImpactResponse(object_id=object_id, direction="downstream", nodes=nodes)


@router.get("/impact/{object_id}/upstream", response_model=ImpactResponse)
def get_upstream(
    object_id: UUID,
    session: DbSession,
    max_depth: Annotated[int, Query(ge=1, le=20, description="Max traversal depth")] = 10,
) -> ImpactResponse:
    repo = LineageRepository(session)
    raw = repo.get_upstream(object_id, max_depth=max_depth)
    nodes = [
        ImpactNode(
            id=UUID(r["props"]["id"]),
            name=r["props"]["name"],
            object_type=r["props"]["object_type"],
            source_id=UUID(r["props"]["source_id"]),
            depth=r["depth"],
            lineage_id=r["lineage_id"],
        )
        for r in raw
    ]
    return ImpactResponse(object_id=object_id, direction="upstream", nodes=nodes)
