"""Integration tests for the PostgreSQL connector.

Requires the lineage-postgres container to be running with the seeded schema.
Tests are automatically skipped when the database is unreachable.
"""

from __future__ import annotations

import time

import pytest

from app.connectors.base import AuthMode

# Guard: skip all tests in this module if psycopg2 not installed
try:
    import psycopg2
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False

pytestmark = pytest.mark.skipif(not _HAS_PSYCOPG2, reason="psycopg2 not installed")


# ---------------------------------------------------------------------------
# Connection fixture and skip guard
# ---------------------------------------------------------------------------

_PG_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "lineage_sample",
    "user": "lineage",
    "password": "lineage",
    "source_name": "test_postgres",
    "include_column_lineage": True,
}


def _pg_available() -> bool:
    try:
        conn = psycopg2.connect(
            host=_PG_CONFIG["host"],
            port=_PG_CONFIG["port"],
            dbname=_PG_CONFIG["dbname"],
            user=_PG_CONFIG["user"],
            password=_PG_CONFIG["password"],
            connect_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def pg_connector():
    if not _pg_available():
        pytest.skip("lineage-postgres is not reachable on localhost:5433")
    from app.connectors.postgresql.connector import PostgreSQLConnector

    connector = PostgreSQLConnector(_PG_CONFIG, auth_mode=AuthMode.USERNAME_PASSWORD)
    yield connector
    connector._close_pool()


@pytest.fixture(scope="module")
def pg_metadata(pg_connector):
    """Extract metadata once and share across all tests in this module."""
    return pg_connector.extract_metadata()


@pytest.fixture(scope="module")
def pg_lineage(pg_connector):
    """Extract lineage once and share across all tests in this module."""
    return pg_connector.extract_lineage()


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------


class TestConnection:
    def test_connection_returns_true(self, pg_connector):
        assert pg_connector.test_connection() is True

    def test_bad_password_returns_false(self):
        from app.connectors.postgresql.connector import PostgreSQLConnector

        bad_cfg = dict(_PG_CONFIG, password="wrong_password")
        connector = PostgreSQLConnector(bad_cfg, auth_mode=AuthMode.USERNAME_PASSWORD)
        try:
            result = connector.test_connection()
            assert result is False
        finally:
            connector._close_pool()


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------


class TestMetadataExtraction:
    def test_datasource_created(self, pg_metadata):
        from app.models.schema import DataSource

        assert isinstance(pg_metadata["datasource"], DataSource)
        assert pg_metadata["datasource"].name == "test_postgres"

    def test_tables_extracted(self, pg_metadata):
        from app.models.schema import DataObjectType

        tables = [
            o for o in pg_metadata["objects"]
            if o.object_type == DataObjectType.TABLE
        ]
        assert len(tables) >= 35, f"Expected >= 35 tables, got {len(tables)}"

    def test_views_extracted(self, pg_metadata):
        from app.models.schema import DataObjectType

        views = [
            o for o in pg_metadata["objects"]
            if o.object_type in (DataObjectType.VIEW, DataObjectType.MATERIALIZED_VIEW)
        ]
        assert len(views) >= 15, f"Expected >= 15 views, got {len(views)}"

    def test_columns_extracted(self, pg_metadata):
        assert len(pg_metadata["columns"]) >= 200, (
            f"Expected >= 200 columns, got {len(pg_metadata['columns'])}"
        )

    def test_functions_extracted(self, pg_metadata):
        from app.models.schema import DataObjectType

        fns = [
            o for o in pg_metadata["objects"]
            if o.object_type in (DataObjectType.FUNCTION, DataObjectType.PROCEDURE)
        ]
        assert len(fns) >= 2

    def test_schema_names_present(self, pg_metadata):
        schema_names = {o.schema_name for o in pg_metadata["objects"]}
        assert "raw" in schema_names
        assert "dw" in schema_names
        assert "rpt" in schema_names

    def test_column_data_types_mapped(self, pg_metadata):
        canonical_types = {
            "integer", "decimal", "string", "boolean",
            "date", "timestamp", "json", "uuid", "array",
        }
        col_types = {c.data_type for c in pg_metadata["columns"] if c.data_type}
        # At least some columns should have canonical types
        assert col_types & canonical_types, f"No canonical types found, got: {col_types}"

    def test_primary_key_flagged(self, pg_metadata):
        pks = [c for c in pg_metadata["columns"] if c.is_primary_key]
        assert len(pks) > 0, "No primary key columns detected"

    def test_datasource_extra_metadata(self, pg_metadata):
        extra = pg_metadata["datasource"].extra_metadata
        assert "postgres_version" in extra
        assert "schemas_extracted" in extra
        assert "extraction_timestamp" in extra

    def test_object_extra_metadata(self, pg_metadata):
        from app.models.schema import DataObjectType

        tables = [
            o for o in pg_metadata["objects"]
            if o.object_type == DataObjectType.TABLE
        ]
        assert len(tables) > 0
        tbl = tables[0]
        assert "row_count_estimate" in tbl.extra_metadata


# ---------------------------------------------------------------------------
# Lineage extraction
# ---------------------------------------------------------------------------


class TestLineageExtraction:
    def test_lineage_list_not_empty(self, pg_lineage):
        assert len(pg_lineage["lineage"]) > 0

    def test_fk_lineage_present(self, pg_lineage):
        from app.models.schema import LineageType

        fk_edges = [
            lin for lin in pg_lineage["lineage"]
            if lin.lineage_type == LineageType.REFERENCE
        ]
        assert len(fk_edges) > 0, "Expected FK lineage edges"

    def test_view_lineage_present(self, pg_lineage):
        from app.models.schema import LineageType

        view_edges = [
            lin for lin in pg_lineage["lineage"]
            if lin.lineage_type in (LineageType.DIRECT, LineageType.DERIVED)
        ]
        assert len(view_edges) > 0, "Expected view lineage edges"

    def test_no_self_referential_lineage(self, pg_lineage):
        for lin in pg_lineage["lineage"]:
            assert lin.source_object_id != lin.target_object_id


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


class TestPerformance:
    def test_full_extract_under_30_seconds(self, pg_connector):
        t0 = time.monotonic()
        meta = pg_connector.extract_metadata()
        duration = time.monotonic() - t0
        assert duration < 30, f"Extraction took {duration:.1f}s (> 30s limit)"
        assert len(meta["objects"]) > 0


# ---------------------------------------------------------------------------
# Idempotent extraction (upsert behaviour)
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_double_extract_same_count(self, pg_connector):
        """Running extract twice should produce the same object counts."""
        meta1 = pg_connector.extract_metadata()
        meta2 = pg_connector.extract_metadata()
        assert len(meta1["objects"]) == len(meta2["objects"])
        assert len(meta1["columns"]) == len(meta2["columns"])


# ---------------------------------------------------------------------------
# Circular dependency handling
# ---------------------------------------------------------------------------


class TestCircularDependency:
    def test_lineage_extraction_does_not_crash(self, pg_connector):
        """Even if there were circular views, extraction must not raise."""
        try:
            result = pg_connector.extract_lineage()
            assert "lineage" in result
        except Exception as exc:
            pytest.fail(f"extract_lineage raised unexpectedly: {exc}")


# ---------------------------------------------------------------------------
# Offline mode
# ---------------------------------------------------------------------------


class TestOfflineMode:
    def test_offline_returns_empty_without_files(self, tmp_path):
        from app.connectors.postgresql.connector import PostgreSQLConnector

        cfg = {"folder_path": str(tmp_path), "source_name": "offline_test"}
        connector = PostgreSQLConnector(cfg, auth_mode=AuthMode.OFFLINE)
        assert connector.test_connection() is True

        meta = connector.extract_metadata()
        assert meta["objects"] == []
        assert meta["columns"] == []

    def test_offline_invalid_folder(self):
        from app.connectors.postgresql.connector import PostgreSQLConnector

        cfg = {"folder_path": "/nonexistent/path_12345"}
        connector = PostgreSQLConnector(cfg, auth_mode=AuthMode.OFFLINE)
        assert connector.test_connection() is False


# ---------------------------------------------------------------------------
# Neo4j persistence (requires Neo4j)
# ---------------------------------------------------------------------------


class TestNeo4jPersistence:
    def test_objects_stored_and_retrievable(self, pg_metadata):
        """Verify that extracted objects can be stored and retrieved via Neo4j."""
        from app.db.neo4j import verify_connectivity

        if not verify_connectivity():
            pytest.skip("Neo4j not available")

        from app.db.neo4j import get_session
        from app.db.repositories.data_object import DataObjectRepository
        from app.db.repositories.data_source import DataSourceRepository

        datasource = pg_metadata["datasource"]
        objects = pg_metadata["objects"][:3]  # Store just 3 to keep test fast

        with get_session() as session:
            src_repo = DataSourceRepository(session)
            obj_repo = DataObjectRepository(session)

            src_repo.create(datasource)
            for obj in objects:
                obj_repo.create(obj)

            retrieved = src_repo.get_by_id(datasource.id)
            assert retrieved is not None
            assert retrieved.name == datasource.name

            for obj in objects:
                fetched = obj_repo.get_by_id(obj.id)
                assert fetched is not None
                assert fetched.name == obj.name
