"""REST endpoints for connector operations — /api/v1/connectors."""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, HTTPException, status

from app.api.v1.dependencies import DbSession, WriterUser
from app.api.v1.models.connectors import (
    ConnectorStatusResponse,
    OfflineValidateRequest,
    OfflineValidateResponse,
    PostgreSQLExtractRequest,
    PostgreSQLExtractResponse,
    PostgreSQLTestRequest,
    PostgreSQLTestResponse,
)
from app.connectors.base import AuthMode
from app.connectors.postgresql.connector import PostgreSQLConnector
from app.connectors.postgresql.extractor import get_pg_version, get_schemas
from app.db.repositories.data_object import DataObjectRepository
from app.db.repositories.data_source import DataSourceRepository
from app.db.repositories.lineage import LineageRepository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/connectors", tags=["connectors"])

_OFFLINE_REQUIRED_FILES = {
    "tables.json",
    "columns.json",
    "foreign_keys.json",
    "view_definitions.json",
    "functions.json",
}


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------


@router.get("/postgresql/status", response_model=ConnectorStatusResponse)
def connector_status(_: WriterUser) -> ConnectorStatusResponse:
    return ConnectorStatusResponse(
        connector="postgresql",
        status="available",
        auth_modes=["username_password", "offline"],
    )


# ---------------------------------------------------------------------------
# Test connection
# ---------------------------------------------------------------------------


@router.post("/postgresql/test-connection", response_model=PostgreSQLTestResponse)
def test_pg_connection(
    body: PostgreSQLTestRequest,
    _: WriterUser,
) -> PostgreSQLTestResponse:
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=body.host,
            port=body.port,
            dbname=body.dbname,
            user=body.user,
            password=body.password,
            connect_timeout=10,
        )
    except Exception as exc:
        return PostgreSQLTestResponse(connected=False, error=str(exc))

    try:
        version = get_pg_version(conn)
        schemas = get_schemas(conn)
    finally:
        conn.close()

    return PostgreSQLTestResponse(connected=True, version=version, schemas=schemas)


# ---------------------------------------------------------------------------
# Extract metadata and lineage
# ---------------------------------------------------------------------------


@router.post(
    "/postgresql/extract",
    response_model=PostgreSQLExtractResponse,
    status_code=status.HTTP_200_OK,
)
def extract_postgresql(
    body: PostgreSQLExtractRequest,
    session: DbSession,
    _: WriterUser,
) -> PostgreSQLExtractResponse:
    cfg: dict[str, Any] = {
        "source_name": body.source_name,
        "host": body.host,
        "port": body.port,
        "dbname": body.dbname,
        "user": body.user,
        "password": body.password,
        "schemas": body.schemas,
        "include_column_lineage": body.include_column_lineage,
    }

    connector = PostgreSQLConnector(cfg, auth_mode=AuthMode.USERNAME_PASSWORD)

    if not connector.test_connection():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot connect to PostgreSQL with the provided credentials.",
        )

    try:
        meta = connector.extract_metadata()
        lineage_result = connector.extract_lineage()
    except Exception as exc:
        logger.exception("Extraction failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Extraction failed: {exc}",
        ) from exc
    finally:
        connector._close_pool()

    datasource = meta["datasource"]
    objects = meta["objects"]
    columns = meta["columns"]
    lineage_list = lineage_result["lineage"]
    duration = meta["duration_s"]

    # Persist to Neo4j using MERGE (upsert)
    src_repo = DataSourceRepository(session)
    obj_repo = DataObjectRepository(session)
    lin_repo = LineageRepository(session)

    src_repo.create(datasource)
    for obj in objects:
        obj_repo.create(obj)

    # Columns don't have a dedicated repo — store via DataObject extra_metadata is
    # too limiting. We use a lightweight Column node repo if it exists, else skip.
    try:
        from app.db.repositories.column import ColumnRepository

        col_repo = ColumnRepository(session)
        for col in columns:
            col_repo.create(col)
    except (ImportError, Exception) as exc:
        logger.debug("Column persistence skipped: %s", exc)

    for lin in lineage_list:
        try:
            lin_repo.create(lin)
        except Exception as exc:
            logger.warning("Skipping lineage edge %s: %s", lin.id, exc)

    slow_warning = None
    if duration > 5:
        slow_warning = f"Extraction took {duration:.1f}s (> 5s threshold). Consider async processing."

    return PostgreSQLExtractResponse(
        source_id=str(datasource.id),
        objects=len(objects),
        columns=len(columns),
        lineage_edges=len(lineage_list),
        duration_seconds=round(duration, 3),
        slow_warning=slow_warning,
    )


# ---------------------------------------------------------------------------
# Offline validation
# ---------------------------------------------------------------------------


@router.post("/offline/validate", response_model=OfflineValidateResponse)
def validate_offline_folder(
    body: OfflineValidateRequest,
    _: WriterUser,
) -> OfflineValidateResponse:
    folder = body.folder_path
    errors: list[str] = []
    files: dict[str, Any] = {}

    if not os.path.isdir(folder):
        return OfflineValidateResponse(
            valid=False,
            errors=[f"Folder does not exist: {folder}"],
        )

    for fname in _OFFLINE_REQUIRED_FILES:
        fpath = os.path.join(folder, fname)
        if os.path.exists(fpath):
            files[fname] = {"path": fpath, "size_bytes": os.path.getsize(fpath), "present": True}
        else:
            files[fname] = {"path": fpath, "present": False}
            errors.append(f"Missing required file: {fname}")

    return OfflineValidateResponse(valid=len(errors) == 0, errors=errors, files=files)
