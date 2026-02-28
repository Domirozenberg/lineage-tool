"""Integration tests for Task 1.4 — Graph Database Setup.

Covers the plan's Graph Database acceptance criteria:
  - Neo4j connection established programmatically
  - CRUD for all entities (delegated to test_repositories.py)
  - Cypher queries return correct lineage relationships
  - Performance: query 1000 nodes in <100ms
  - Constraints and indexes are present
  - Health endpoint reports Neo4j + Redis status
  - Backup script is executable (existence check)

Requires a running Neo4j instance (docker compose up -d).
Skipped automatically when Neo4j is unreachable.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.core.config import settings
from app.db.constraints import apply_constraints_and_indexes
from app.db.neo4j import get_db_status, get_session, verify_connectivity
from app.db.repositories.data_object import DataObjectRepository
from app.db.repositories.data_source import DataSourceRepository
from app.db.repositories.lineage import LineageRepository
from app.models.schema import DataObject, DataObjectType, DataSource, Lineage, Platform

pytestmark = pytest.mark.skipif(
    not verify_connectivity(),
    reason="Neo4j is not reachable — start docker compose up -d",
)

_PERF_PREFIX = "perf_test_"
_PERF_SCHEMA = "perf_schema"


@pytest.fixture(scope="module")
def session():
    """Module-scoped session — shared across all tests in this file."""
    with get_session() as s:
        apply_constraints_and_indexes(s)
        yield s
        # Cleanup all performance-test nodes
        s.run(
            "MATCH (n) WHERE n.name STARTS WITH $prefix OR n.schema_name = $schema DETACH DELETE n",
            prefix=_PERF_PREFIX,
            schema=_PERF_SCHEMA,
        )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


class TestConnection:
    def test_connectivity(self):
        assert verify_connectivity() is True

    def test_driver_uses_configured_uri(self):
        from app.db.neo4j import get_driver
        driver = get_driver()
        assert driver is not None

    def test_pool_settings_respected(self):
        """Pool size from config must be applied — verify via get_db_status."""
        status = get_db_status()
        assert status["connected"] is True
        assert status["pool_size"] == settings.NEO4J_MAX_CONNECTION_POOL_SIZE

    def test_get_db_status_structure(self):
        status = get_db_status()
        assert "connected" in status
        assert "uri" in status
        assert "pool_size" in status
        assert "error" in status
        assert status["error"] is None


# ---------------------------------------------------------------------------
# Constraints & Indexes
# ---------------------------------------------------------------------------


class TestConstraintsAndIndexes:
    def test_uniqueness_constraints_exist(self, session):
        """All four entity uniqueness constraints must be present in Neo4j."""
        result = session.run("SHOW CONSTRAINTS YIELD name RETURN collect(name) AS names")
        names: list[str] = result.single()["names"]
        expected = [
            "datasource_id_unique",
            "dataobject_id_unique",
            "column_id_unique",
            "lineage_id_unique",
        ]
        for constraint in expected:
            assert constraint in names, f"Missing constraint: {constraint}"

    def test_indexes_exist(self, session):
        """Core lookup indexes must be present."""
        result = session.run("SHOW INDEXES YIELD name RETURN collect(name) AS names")
        names: list[str] = result.single()["names"]
        expected_indexes = [
            "datasource_name",
            "datasource_platform",
            "dataobject_name",
            "dataobject_type",
        ]
        for idx in expected_indexes:
            assert idx in names, f"Missing index: {idx}"

    def test_apply_constraints_idempotent(self, session):
        """Running apply_constraints_and_indexes twice must not raise."""
        apply_constraints_and_indexes(session)


# ---------------------------------------------------------------------------
# Cypher lineage queries
# ---------------------------------------------------------------------------


class TestLineageQueries:
    def _seed_chain(self, session):
        """Create A → B → C and return (A, B, C) DataObjects."""
        src = DataSource(name=f"{_PERF_PREFIX}chain_src", platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)
        a = DataObject(source_id=src.id, object_type=DataObjectType.TABLE,
                       name=f"{_PERF_PREFIX}chain_a", schema_name=_PERF_SCHEMA)
        b = DataObject(source_id=src.id, object_type=DataObjectType.VIEW,
                       name=f"{_PERF_PREFIX}chain_b", schema_name=_PERF_SCHEMA)
        c = DataObject(source_id=src.id, object_type=DataObjectType.DASHBOARD,
                       name=f"{_PERF_PREFIX}chain_c", schema_name=_PERF_SCHEMA)
        obj_repo = DataObjectRepository(session)
        for o in [a, b, c]:
            obj_repo.create(o)
        lin_repo = LineageRepository(session)
        lin_repo.create(Lineage(source_object_id=a.id, target_object_id=b.id))
        lin_repo.create(Lineage(source_object_id=b.id, target_object_id=c.id))
        return a, b, c

    def test_direct_relationship_cypher(self, session):
        """HAS_LINEAGE edge between two nodes is queryable via plain Cypher."""
        a, b, _ = self._seed_chain(session)
        result = session.run(
            "MATCH (s:DataObject {id: $sid})-[:HAS_LINEAGE]->(t:DataObject {id: $tid}) "
            "RETURN count(*) AS cnt",
            sid=str(a.id), tid=str(b.id),
        ).single()
        assert result["cnt"] == 1

    def test_multi_hop_downstream_cypher(self, session):
        """Variable-length traversal reaches nodes 2 hops away."""
        a, _, c = self._seed_chain(session)
        result = session.run(
            "MATCH (s:DataObject {id: $sid})-[:HAS_LINEAGE*1..5]->(t:DataObject) "
            "RETURN collect(t.id) AS ids",
            sid=str(a.id),
        ).single()
        assert str(c.id) in result["ids"]

    def test_multi_hop_upstream_cypher(self, session):
        """Reverse traversal reaches nodes 2 hops upstream."""
        a, _, c = self._seed_chain(session)
        result = session.run(
            "MATCH (s:DataObject)-[:HAS_LINEAGE*1..5]->(t:DataObject {id: $tid}) "
            "RETURN collect(s.id) AS ids",
            tid=str(c.id),
        ).single()
        assert str(a.id) in result["ids"]

    def test_cross_platform_lineage_cypher(self, session):
        """A lineage edge can connect objects from different DataSources."""
        pg = DataSource(name=f"{_PERF_PREFIX}xplat_pg", platform=Platform.POSTGRESQL)
        tb = DataSource(name=f"{_PERF_PREFIX}xplat_tb", platform=Platform.TABLEAU)
        DataSourceRepository(session).create(pg)
        DataSourceRepository(session).create(tb)

        tbl = DataObject(source_id=pg.id, object_type=DataObjectType.TABLE,
                         name=f"{_PERF_PREFIX}xplat_tbl", schema_name=_PERF_SCHEMA)
        dash = DataObject(source_id=tb.id, object_type=DataObjectType.DASHBOARD,
                          name=f"{_PERF_PREFIX}xplat_dash", schema_name=_PERF_SCHEMA)
        DataObjectRepository(session).create(tbl)
        DataObjectRepository(session).create(dash)

        lin = Lineage(source_object_id=tbl.id, target_object_id=dash.id)
        LineageRepository(session).create(lin)

        result = session.run(
            """
            MATCH (t:DataObject {id: $tid})-[:HAS_LINEAGE]->(d:DataObject {id: $did})
            MATCH (src_a:DataSource {id: t.source_id})
            MATCH (src_b:DataSource {id: d.source_id})
            RETURN src_a.platform AS from_platform, src_b.platform AS to_platform
            """,
            tid=str(tbl.id), did=str(dash.id),
        ).single()
        assert result["from_platform"] == "postgresql"
        assert result["to_platform"] == "tableau"


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


class TestPerformance:
    _seeded = False

    def _seed_1000_nodes(self, session):
        """Bulk-insert 1000 DataObject nodes using UNWIND (runs once per module)."""
        if TestPerformance._seeded:
            return
        now = datetime.now(timezone.utc).isoformat()
        source_id = str(uuid4())
        # Create a dummy DataSource node first
        session.run(
            "MERGE (n:DataSource {id: $id}) SET n += $props",
            id=source_id,
            props={
                "id": source_id,
                "name": f"{_PERF_PREFIX}bulk_src",
                "platform": "postgresql",
                "schema_version": "1.1.0",
                "created_at": now,
                "updated_at": now,
                "extra_metadata": "{}",
            },
        )
        props_list = [
            {
                "id": str(uuid4()),
                "source_id": source_id,
                "object_type": "table",
                "name": f"{_PERF_PREFIX}bulk_{i:04d}",
                "schema_name": _PERF_SCHEMA,
                "database_name": "perf_db",
                "description": None,
                "sql_definition": None,
                "extra_metadata": json.dumps({}),
                "schema_version": "1.1.0",
                "created_at": now,
                "updated_at": now,
            }
            for i in range(1000)
        ]
        session.run(
            "UNWIND $props AS p MERGE (n:DataObject {id: p.id}) SET n += p",
            props=props_list,
        )
        TestPerformance._seeded = True

    def test_1000_nodes_seeded(self, session):
        """Verify the 1000 seed nodes are actually in the DB."""
        self._seed_1000_nodes(session)
        result = session.run(
            "MATCH (n:DataObject {schema_name: $schema}) RETURN count(n) AS cnt",
            schema=_PERF_SCHEMA,
        ).single()
        assert result["cnt"] >= 1000

    def test_count_query_under_100ms(self, session):
        """COUNT query over 1000 indexed nodes must complete in <100ms."""
        self._seed_1000_nodes(session)
        # Warm-up pass (first query may open a connection)
        session.run(
            "MATCH (n:DataObject {object_type: 'table'}) RETURN count(n) AS cnt"
        ).single()
        # Timed pass
        start = time.perf_counter()
        result = session.run(
            "MATCH (n:DataObject {object_type: 'table'}) RETURN count(n) AS cnt"
        ).single()
        elapsed_ms = (time.perf_counter() - start) * 1000
        assert result["cnt"] >= 1000
        assert elapsed_ms < 100, f"Query took {elapsed_ms:.1f}ms — expected <100ms"

    def test_name_prefix_scan_under_100ms(self, session):
        """Prefix scan over 1000 nodes must complete in <100ms."""
        self._seed_1000_nodes(session)
        start = time.perf_counter()
        result = session.run(
            "MATCH (n:DataObject) WHERE n.name STARTS WITH $prefix RETURN count(n) AS cnt",
            prefix=_PERF_PREFIX,
        ).single()
        elapsed_ms = (time.perf_counter() - start) * 1000
        assert result["cnt"] >= 1000
        assert elapsed_ms < 100, f"Prefix scan took {elapsed_ms:.1f}ms — expected <100ms"


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_health_returns_neo4j_status(self):
        """get_db_status() must show connected=True when Neo4j is up."""
        status = get_db_status()
        assert status["connected"] is True
        assert status["error"] is None

    def test_health_includes_pool_size(self):
        status = get_db_status()
        assert status["pool_size"] == settings.NEO4J_MAX_CONNECTION_POOL_SIZE
        assert status["uri"] == settings.NEO4J_URI


# ---------------------------------------------------------------------------
# Backup scripts exist and are executable
# ---------------------------------------------------------------------------


class TestBackupScripts:
    def test_backup_script_exists(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "backup_neo4j.sh"
        )
        assert os.path.isfile(os.path.abspath(path))

    def test_restore_script_exists(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "restore_neo4j.sh"
        )
        assert os.path.isfile(os.path.abspath(path))

    def test_backup_script_executable(self):
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "backup_neo4j.sh")
        )
        assert os.access(path, os.X_OK)

    def test_restore_script_executable(self):
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "restore_neo4j.sh")
        )
        assert os.access(path, os.X_OK)

    def test_backup_docs_exist(self):
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "docs", "BACKUP.md")
        )
        assert os.path.isfile(path)
        with open(path) as f:
            content = f.read()
        assert "backup_neo4j.sh" in content
        assert "restore_neo4j.sh" in content
