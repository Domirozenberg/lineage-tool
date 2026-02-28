"""PostgreSQL metadata and lineage connector.

Supports:
  AuthMode.USERNAME_PASSWORD — live connection via psycopg2 connection pool
  AuthMode.OFFLINE           — read previously exported JSON files from a folder
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid5, NAMESPACE_URL

import psycopg2
import psycopg2.pool

from app.connectors.base import AuthMode, BaseConnector
from app.connectors.postgresql.extractor import (
    get_columns,
    get_foreign_keys,
    get_functions,
    get_indexes,
    get_pg_version,
    get_schemas,
    get_tables,
    get_view_definitions,
)
from app.connectors.postgresql.lineage_parser import (
    SqlLineageParser,
    detect_circular_refs,
)
from app.models.schema import (
    Column,
    ColumnLineageMap,
    DataObject,
    DataObjectType,
    DataSource,
    Lineage,
    LineageType,
    Platform,
)

logger = logging.getLogger(__name__)

# Stable namespace for deterministic UUIDs — ensures the same DB object always
# gets the same UUID regardless of how many times the connector is invoked.
_LINEAGE_NS = uuid5(NAMESPACE_URL, "urn:lineage-tool:postgresql")


def _stable_source_id(platform: str, host: str, dbname: str, source_name: str) -> UUID:
    return uuid5(_LINEAGE_NS, f"{platform}:{host}:{dbname}:{source_name}")


def _stable_object_id(source_id: UUID, schema: str, name: str) -> UUID:
    return uuid5(_LINEAGE_NS, f"{source_id}:{schema}:{name}")


def _stable_column_id(object_id: UUID, name: str) -> UUID:
    return uuid5(_LINEAGE_NS, f"{object_id}:{name}")

# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------

_OBJECT_TYPE_MAP: dict[str, DataObjectType] = {
    "TABLE": DataObjectType.TABLE,
    "VIEW": DataObjectType.VIEW,
    "MATERIALIZED_VIEW": DataObjectType.MATERIALIZED_VIEW,
    "FUNCTION": DataObjectType.FUNCTION,
    "PROCEDURE": DataObjectType.PROCEDURE,
}


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class PostgreSQLConnector(BaseConnector):
    """Extract metadata and lineage from a PostgreSQL database.

    Config keys (USERNAME_PASSWORD mode):
      host, port, dbname, user, password
      schemas        — optional list[str] to filter schemas
      min_conn       — min pool connections (default 1)
      max_conn       — max pool connections (default 10)
      include_column_lineage — bool (default True)

    Config keys (OFFLINE mode):
      folder_path    — directory containing JSON export files
    """

    def __init__(self, config: dict[str, Any], auth_mode: AuthMode = AuthMode.USERNAME_PASSWORD):
        super().__init__(config, auth_mode)
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_pool(self) -> psycopg2.pool.ThreadedConnectionPool:
        if self._pool is None:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.config.get("min_conn", 1),
                maxconn=self.config.get("max_conn", 10),
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 5432),
                dbname=self.config.get("dbname", "postgres"),
                user=self.config.get("user", "postgres"),
                password=self.config.get("password", ""),
            )
        return self._pool

    def _conn(self):
        """Context manager: borrow a connection from the pool."""
        return _PooledConnection(self._get_pool())

    def _close_pool(self) -> None:
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def test_connection(self) -> bool:
        if self.auth_mode == AuthMode.OFFLINE:
            folder = self.config.get("folder_path", "")
            return os.path.isdir(folder)

        try:
            with self._conn() as conn:
                get_pg_version(conn)
            return True
        except Exception as exc:
            logger.error("PostgreSQL connection test failed: %s", exc)
            return False

    def extract_metadata(self) -> dict[str, Any]:
        """Return (datasource, objects, columns) extracted from PostgreSQL.

        Returns:
            dict with keys:
              datasource  — DataSource
              objects     — list[DataObject]
              columns     — list[Column]
              duration_s  — float
        """
        t0 = time.monotonic()

        if self.auth_mode == AuthMode.OFFLINE:
            return self._extract_offline_metadata()

        return self._extract_online_metadata(t0)

    def extract_lineage(self) -> dict[str, Any]:
        """Return lineage relationships.

        Returns:
            dict with keys:
              lineage — list[Lineage]
        """
        if self.auth_mode == AuthMode.OFFLINE:
            return self._extract_offline_lineage()

        return self._extract_online_lineage()

    # ------------------------------------------------------------------
    # Online extraction
    # ------------------------------------------------------------------

    def _extract_online_metadata(self, t0: float) -> dict[str, Any]:
        source_name = self.config.get("source_name", "postgresql")
        schema_filter: Optional[list[str]] = self.config.get("schemas")

        with self._conn() as conn:
            version = get_pg_version(conn)
            all_schemas = get_schemas(conn)

        if schema_filter:
            schemas = [s for s in all_schemas if s in schema_filter]
        else:
            schemas = all_schemas

        datasource = DataSource(
            id=_stable_source_id(
                "postgresql",
                self.config.get("host", ""),
                self.config.get("dbname", ""),
                source_name,
            ),
            name=source_name,
            platform=Platform.POSTGRESQL,
            host=self.config.get("host"),
            port=self.config.get("port"),
            database=self.config.get("dbname"),
            extra_metadata={
                "postgres_version": version,
                "schemas_extracted": schemas,
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

        objects: list[DataObject] = []
        columns: list[Column] = []

        with self._conn() as conn:
            for schema in schemas:
                tables = get_tables(conn, schema)
                for t in tables:
                    obj = self._make_data_object(datasource.id, schema, t)
                    objects.append(obj)

                    if obj.object_type in (
                        DataObjectType.TABLE,
                        DataObjectType.VIEW,
                        DataObjectType.MATERIALIZED_VIEW,
                    ):
                        raw_cols = get_columns(conn, schema, t["name"])
                        for rc in raw_cols:
                            columns.append(self._make_column(obj.id, rc))

                # Functions
                for fn in get_functions(conn, schema):
                    obj = self._make_function_object(datasource.id, schema, fn)
                    objects.append(obj)

        duration = time.monotonic() - t0
        return {
            "datasource": datasource,
            "objects": objects,
            "columns": columns,
            "duration_s": duration,
        }

    def _extract_online_lineage(self) -> dict[str, Any]:
        schema_filter: Optional[list[str]] = self.config.get("schemas")
        include_col_lineage = self.config.get("include_column_lineage", True)

        with self._conn() as conn:
            all_schemas = get_schemas(conn)

        if schema_filter:
            schemas = [s for s in all_schemas if s in schema_filter]
        else:
            schemas = all_schemas

        lineage_list: list[Lineage] = []

        # We need object_id lookup: (schema, name) → UUID
        # Run metadata extraction to build the map
        meta = self._extract_online_metadata(time.monotonic())
        object_map: dict[tuple[str, str], UUID] = {
            (obj.schema_name or "", obj.name): obj.id
            for obj in meta["objects"]
        }
        column_map: dict[tuple[UUID, str], UUID] = {
            (col.object_id, col.name): col.id
            for col in meta["columns"]
        }

        parser = SqlLineageParser(dialect="postgres")
        processing_set: set[str] = set()

        with self._conn() as conn:
            for schema in schemas:
                # FK lineage
                fks = get_foreign_keys(conn, schema)
                for fk in fks:
                    src_key = (schema, fk["source_table"])
                    tgt_key = (schema, fk["target_table"])
                    src_id = object_map.get(src_key)
                    tgt_id = object_map.get(tgt_key)
                    if src_id and tgt_id and src_id != tgt_id:
                        lin = Lineage(
                            source_object_id=tgt_id,
                            target_object_id=src_id,
                            lineage_type=LineageType.REFERENCE,
                            description=f"FK: {fk['constraint_name']}",
                        )
                        lineage_list.append(lin)

                # View lineage
                if include_col_lineage:
                    view_defs = get_view_definitions(conn, schema)
                    for vd in view_defs:
                        target_key = (schema, vd["name"])
                        target_id = object_map.get(target_key)
                        if target_id is None:
                            continue

                        processing_key = f"{schema}.{vd['name']}".lower()
                        processing_set.add(processing_key)

                        try:
                            parsed = parser.parse_view(
                                vd["view_definition"] or "",
                                target_schema=schema,
                                target_name=vd["name"],
                            )
                        except Exception as exc:
                            logger.warning(
                                "Failed to parse view %s.%s: %s",
                                schema,
                                vd["name"],
                                exc,
                            )
                            processing_set.discard(processing_key)
                            continue

                        safe_sources = detect_circular_refs(
                            vd["name"],
                            parsed.source_tables,
                            schema,
                            processing_set,
                        )

                        for src_schema, src_table in safe_sources:
                            effective_schema = src_schema or schema
                            src_key = (effective_schema, src_table)
                            src_id = object_map.get(src_key)
                            if src_id is None:
                                continue

                            col_mappings: list[ColumnLineageMap] = []
                            if not parsed.parse_error:
                                for entry in parsed.column_entries:
                                    eff_entry_schema = entry.source_schema or effective_schema
                                    if (
                                        entry.source_table.lower() == src_table.lower()
                                        and eff_entry_schema.lower() == effective_schema.lower()
                                    ):
                                        src_col_id = column_map.get((src_id, entry.source_column))
                                        tgt_col_id = column_map.get((target_id, entry.target_column))
                                        if src_col_id and tgt_col_id:
                                            col_mappings.append(
                                                ColumnLineageMap(
                                                    source_column_id=src_col_id,
                                                    target_column_id=tgt_col_id,
                                                    transformation=entry.transformation,
                                                )
                                            )

                            lineage_type = (
                                LineageType.DERIVED
                                if vd.get("is_materialized")
                                else LineageType.DIRECT
                            )

                            lin = Lineage(
                                source_object_id=src_id,
                                target_object_id=target_id,
                                lineage_type=lineage_type,
                                column_mappings=col_mappings,
                                sql=vd["view_definition"],
                            )
                            lineage_list.append(lin)

                        processing_set.discard(processing_key)

        return {"lineage": lineage_list}

    # ------------------------------------------------------------------
    # Offline extraction
    # ------------------------------------------------------------------

    def _extract_offline_metadata(self) -> dict[str, Any]:
        folder = self.config.get("folder_path", "")
        source_name = self.config.get("source_name", "postgresql_offline")

        datasource = DataSource(
            name=source_name,
            platform=Platform.POSTGRESQL,
            extra_metadata={
                "offline_folder": folder,
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

        objects: list[DataObject] = []
        columns: list[Column] = []

        tables_file = os.path.join(folder, "tables.json")
        columns_file = os.path.join(folder, "columns.json")

        if os.path.exists(tables_file):
            with open(tables_file) as f:
                tables_data = json.load(f)
            for schema, table_list in tables_data.items():
                for t in table_list:
                    obj = self._make_data_object(datasource.id, schema, t)
                    objects.append(obj)

        if os.path.exists(columns_file):
            with open(columns_file) as f:
                columns_data = json.load(f)
            obj_map = {(o.schema_name, o.name): o.id for o in objects}
            for (schema, table), col_list in (
                (k.split("|"), v) for k, v in columns_data.items()
            ):
                obj_id = obj_map.get((schema, table))
                if obj_id is None:
                    continue
                for rc in col_list:
                    columns.append(self._make_column(obj_id, rc))

        return {"datasource": datasource, "objects": objects, "columns": columns, "duration_s": 0.0}

    def _extract_offline_lineage(self) -> dict[str, Any]:
        folder = self.config.get("folder_path", "")
        lineage_list: list[Lineage] = []

        fk_file = os.path.join(folder, "foreign_keys.json")
        view_file = os.path.join(folder, "view_definitions.json")

        meta = self._extract_offline_metadata()
        object_map: dict[tuple[str, str], UUID] = {
            (obj.schema_name or "", obj.name): obj.id
            for obj in meta["objects"]
        }

        if os.path.exists(fk_file):
            with open(fk_file) as f:
                fk_data = json.load(f)
            for schema, fks in fk_data.items():
                for fk in fks:
                    src_key = (schema, fk["source_table"])
                    tgt_key = (schema, fk["target_table"])
                    src_id = object_map.get(src_key)
                    tgt_id = object_map.get(tgt_key)
                    if src_id and tgt_id and src_id != tgt_id:
                        lineage_list.append(
                            Lineage(
                                source_object_id=tgt_id,
                                target_object_id=src_id,
                                lineage_type=LineageType.REFERENCE,
                                description=f"FK: {fk.get('constraint_name', '')}",
                            )
                        )

        if os.path.exists(view_file):
            parser = SqlLineageParser(dialect="postgres")
            with open(view_file) as f:
                view_data = json.load(f)
            for schema, view_list in view_data.items():
                for vd in view_list:
                    target_key = (schema, vd["name"])
                    target_id = object_map.get(target_key)
                    if target_id is None:
                        continue
                    try:
                        parsed = parser.parse_view(
                            vd.get("view_definition", ""),
                            target_schema=schema,
                            target_name=vd["name"],
                        )
                    except Exception:
                        continue
                    for src_schema, src_table in parsed.source_tables:
                        src_key = (src_schema or schema, src_table)
                        src_id = object_map.get(src_key)
                        if src_id and src_id != target_id:
                            lineage_list.append(
                                Lineage(
                                    source_object_id=src_id,
                                    target_object_id=target_id,
                                    lineage_type=LineageType.DIRECT,
                                )
                            )

        return {"lineage": lineage_list}

    # ------------------------------------------------------------------
    # Model factories
    # ------------------------------------------------------------------

    @staticmethod
    def _make_data_object(
        source_id: UUID,
        schema: str,
        row: dict[str, Any],
    ) -> DataObject:
        obj_type = _OBJECT_TYPE_MAP.get(row.get("object_type", ""), DataObjectType.UNKNOWN)

        extra: dict[str, Any] = {}
        if obj_type == DataObjectType.TABLE:
            extra = {
                "row_count_estimate": row.get("row_count_estimate"),
                "has_indexes": True,
                "tablespace": row.get("tablespace"),
            }
        elif obj_type in (DataObjectType.VIEW, DataObjectType.MATERIALIZED_VIEW):
            extra = {
                "is_materialized": obj_type == DataObjectType.MATERIALIZED_VIEW,
                "view_sql": row.get("view_definition"),
                "referenced_tables": [],
            }

        return DataObject(
            id=_stable_object_id(source_id, schema, row["name"]),
            source_id=source_id,
            object_type=obj_type,
            name=row["name"],
            schema_name=schema,
            description=row.get("description"),
            sql_definition=row.get("view_definition"),
            extra_metadata=extra,
        )

    @staticmethod
    def _make_function_object(
        source_id: UUID,
        schema: str,
        row: dict[str, Any],
    ) -> DataObject:
        obj_type_str = row.get("object_type", "FUNCTION")
        obj_type = _OBJECT_TYPE_MAP.get(obj_type_str, DataObjectType.FUNCTION)
        return DataObject(
            id=_stable_object_id(source_id, schema, row["name"]),
            source_id=source_id,
            object_type=obj_type,
            name=row["name"],
            schema_name=schema,
            description=row.get("description"),
            sql_definition=row.get("source"),
            extra_metadata={
                "return_type": row.get("return_type"),
                "argument_types": row.get("argument_types"),
                "language": row.get("language"),
            },
        )

    @staticmethod
    def _make_column(object_id: UUID, row: dict[str, Any]) -> Column:
        return Column(
            id=_stable_column_id(object_id, row["name"]),
            object_id=object_id,
            name=row["name"],
            data_type=row.get("data_type"),
            ordinal_position=row.get("ordinal_position"),
            is_nullable=row.get("is_nullable", True),
            is_primary_key=row.get("is_primary_key", False),
            description=row.get("description"),
            extra_metadata={
                "pg_type": row.get("pg_type"),
                "position": row.get("ordinal_position"),
                "column_default": row.get("column_default"),
            },
        )


# ---------------------------------------------------------------------------
# Context manager for connection pool
# ---------------------------------------------------------------------------


class _PooledConnection:
    def __init__(self, pool: psycopg2.pool.ThreadedConnectionPool):
        self._pool = pool
        self._conn = None

    def __enter__(self):
        self._conn = self._pool.getconn()
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            self._pool.putconn(self._conn)
            self._conn = None
        return False
