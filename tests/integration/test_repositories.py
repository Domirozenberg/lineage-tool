"""Integration tests for Task 1.3 — Core Data Models / Persistence Layer.

These tests require a running Neo4j instance (docker compose up -d).
They are skipped automatically when Neo4j is unreachable.

Each test class uses a dedicated neo4j session that is rolled back / cleaned
up in the fixture teardown so tests are isolated from one another.
"""

import pytest

from app.db.constraints import apply_constraints_and_indexes
from app.db.neo4j import get_session, verify_connectivity
from app.db.repositories.column import ColumnRepository
from app.db.repositories.data_object import DataObjectRepository
from app.db.repositories.data_source import DataSourceRepository
from app.db.repositories.lineage import LineageRepository
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

# ---------------------------------------------------------------------------
# Skip entire module when Neo4j is not available
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
    not verify_connectivity(),
    reason="Neo4j is not reachable — start docker compose up -d",
)


# ---------------------------------------------------------------------------
# Session fixture — cleans up test data after each test
# ---------------------------------------------------------------------------

_TEST_PREFIX = "test_repo_"


@pytest.fixture()
def session():
    """Yield a live Neo4j session; delete all test nodes after the test."""
    with get_session() as s:
        apply_constraints_and_indexes(s)
        yield s
        # Tear down: remove nodes whose name starts with the test prefix
        s.run(
            "MATCH (n) WHERE n.name STARTS WITH $prefix DETACH DELETE n",
            prefix=_TEST_PREFIX,
        )


def _src_name(suffix: str) -> str:
    return f"{_TEST_PREFIX}{suffix}"


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------


class TestDataSourceRepository:
    def test_create_and_get(self, session):
        repo = DataSourceRepository(session)
        src = DataSource(name=_src_name("pg"), platform=Platform.POSTGRESQL, host="localhost")
        repo.create(src)

        fetched = repo.get_by_id(src.id)
        assert fetched is not None
        assert fetched.id == src.id
        assert fetched.name == src.name
        assert fetched.platform == Platform.POSTGRESQL

    def test_get_missing_returns_none(self, session):
        from uuid import uuid4
        repo = DataSourceRepository(session)
        assert repo.get_by_id(uuid4()) is None

    def test_list_all(self, session):
        repo = DataSourceRepository(session)
        s1 = DataSource(name=_src_name("list_a"), platform=Platform.POSTGRESQL)
        s2 = DataSource(name=_src_name("list_b"), platform=Platform.TABLEAU)
        repo.create(s1)
        repo.create(s2)

        all_sources = repo.list_all()
        ids = {s.id for s in all_sources}
        assert s1.id in ids
        assert s2.id in ids

    def test_list_by_platform(self, session):
        repo = DataSourceRepository(session)
        pg = DataSource(name=_src_name("plat_pg"), platform=Platform.POSTGRESQL)
        tb = DataSource(name=_src_name("plat_tb"), platform=Platform.TABLEAU)
        repo.create(pg)
        repo.create(tb)

        pg_sources = repo.list_by_platform(Platform.POSTGRESQL)
        pg_ids = {s.id for s in pg_sources}
        assert pg.id in pg_ids
        assert tb.id not in pg_ids

    def test_update(self, session):
        repo = DataSourceRepository(session)
        src = DataSource(name=_src_name("upd"), platform=Platform.MYSQL)
        repo.create(src)

        src.description = "updated description"
        repo.update(src)

        fetched = repo.get_by_id(src.id)
        assert fetched is not None
        assert fetched.description == "updated description"

    def test_delete(self, session):
        repo = DataSourceRepository(session)
        src = DataSource(name=_src_name("del"), platform=Platform.SNOWFLAKE)
        repo.create(src)
        assert repo.get_by_id(src.id) is not None

        deleted = repo.delete(src.id)
        assert deleted is True
        assert repo.get_by_id(src.id) is None

    def test_delete_missing_returns_false(self, session):
        from uuid import uuid4
        repo = DataSourceRepository(session)
        assert repo.delete(uuid4()) is False

    def test_extra_metadata_round_trip(self, session):
        repo = DataSourceRepository(session)
        src = DataSource(
            name=_src_name("meta"),
            platform=Platform.POSTGRESQL,
            extra_metadata={"ssl_mode": "require", "max_connections": 50},
        )
        repo.create(src)
        fetched = repo.get_by_id(src.id)
        assert fetched is not None
        assert fetched.extra_metadata["ssl_mode"] == "require"
        assert fetched.extra_metadata["max_connections"] == 50


# ---------------------------------------------------------------------------
# DataObject
# ---------------------------------------------------------------------------


class TestDataObjectRepository:
    def test_create_and_get(self, session):
        src = DataSource(name=_src_name("obj_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)

        repo = DataObjectRepository(session)
        obj = DataObject(
            source_id=src.id,
            object_type=DataObjectType.TABLE,
            name=_src_name("orders"),
            schema_name="public",
        )
        repo.create(obj)

        fetched = repo.get_by_id(obj.id)
        assert fetched is not None
        assert fetched.id == obj.id
        assert fetched.object_type == DataObjectType.TABLE
        assert fetched.schema_name == "public"

    def test_qualified_name_preserved(self, session):
        src = DataSource(name=_src_name("obj_qn_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)

        repo = DataObjectRepository(session)
        obj = DataObject(
            source_id=src.id,
            object_type=DataObjectType.VIEW,
            name=_src_name("v_sales"),
            schema_name="reporting",
            database_name="analytics",
        )
        repo.create(obj)
        fetched = repo.get_by_id(obj.id)
        assert fetched is not None
        assert fetched.qualified_name == f"analytics.reporting.{_src_name('v_sales')}"

    def test_list_by_source(self, session):
        src = DataSource(name=_src_name("obj_lbs"), platform=Platform.POSTGRESQL)
        other_src = DataSource(name=_src_name("obj_lbs_other"), platform=Platform.TABLEAU)
        DataSourceRepository(session).create(src)
        DataSourceRepository(session).create(other_src)

        repo = DataObjectRepository(session)
        t1 = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("t1"))
        t2 = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("t2"))
        t3 = DataObject(source_id=other_src.id, object_type=DataObjectType.TABLE, name=_src_name("t3"))
        for obj in [t1, t2, t3]:
            repo.create(obj)

        result = repo.list_by_source(src.id)
        ids = {o.id for o in result}
        assert t1.id in ids
        assert t2.id in ids
        assert t3.id not in ids

    def test_list_by_type(self, session):
        src = DataSource(name=_src_name("obj_lbt"), platform=Platform.TABLEAU)
        DataSourceRepository(session).create(src)

        repo = DataObjectRepository(session)
        dash = DataObject(source_id=src.id, object_type=DataObjectType.DASHBOARD, name=_src_name("dash"))
        tbl = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("tbl_lbt"))
        repo.create(dash)
        repo.create(tbl)

        dashboards = repo.list_by_type(DataObjectType.DASHBOARD)
        ids = {o.id for o in dashboards}
        assert dash.id in ids
        assert tbl.id not in ids

    def test_update(self, session):
        src = DataSource(name=_src_name("obj_upd_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)

        repo = DataObjectRepository(session)
        obj = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("obj_upd"))
        repo.create(obj)

        obj.description = "new description"
        repo.update(obj)

        fetched = repo.get_by_id(obj.id)
        assert fetched is not None
        assert fetched.description == "new description"

    def test_delete(self, session):
        src = DataSource(name=_src_name("obj_del_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)

        repo = DataObjectRepository(session)
        obj = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("obj_del"))
        repo.create(obj)

        assert repo.delete(obj.id) is True
        assert repo.get_by_id(obj.id) is None


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------


class TestColumnRepository:
    def _make_object(self, session) -> DataObject:
        src = DataSource(name=_src_name("col_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)
        obj = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("col_tbl"))
        DataObjectRepository(session).create(obj)
        return obj

    def test_create_and_get(self, session):
        obj = self._make_object(session)
        repo = ColumnRepository(session)

        col = Column(object_id=obj.id, name=_src_name("order_id"), data_type="integer",
                     ordinal_position=0, is_nullable=False, is_primary_key=True)
        repo.create(col)

        fetched = repo.get_by_id(col.id)
        assert fetched is not None
        assert fetched.id == col.id
        assert fetched.is_primary_key is True
        assert fetched.is_nullable is False

    def test_list_by_object_ordered(self, session):
        obj = self._make_object(session)
        repo = ColumnRepository(session)

        c0 = Column(object_id=obj.id, name=_src_name("col_c0"), ordinal_position=0)
        c1 = Column(object_id=obj.id, name=_src_name("col_c1"), ordinal_position=1)
        c2 = Column(object_id=obj.id, name=_src_name("col_c2"), ordinal_position=2)
        for c in [c2, c0, c1]:  # create out of order
            repo.create(c)

        cols = repo.list_by_object(obj.id)
        ids = [c.id for c in cols]
        assert ids.index(c0.id) < ids.index(c1.id) < ids.index(c2.id)

    def test_update(self, session):
        obj = self._make_object(session)
        repo = ColumnRepository(session)
        col = Column(object_id=obj.id, name=_src_name("col_upd"))
        repo.create(col)

        col.data_type = "varchar"
        repo.update(col)

        fetched = repo.get_by_id(col.id)
        assert fetched is not None
        assert fetched.data_type == "varchar"

    def test_delete(self, session):
        obj = self._make_object(session)
        repo = ColumnRepository(session)
        col = Column(object_id=obj.id, name=_src_name("col_del"))
        repo.create(col)

        assert repo.delete(col.id) is True
        assert repo.get_by_id(col.id) is None


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


class TestLineageRepository:
    def _make_two_objects(self, session) -> tuple[DataObject, DataObject]:
        src = DataSource(name=_src_name("lin_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)
        a = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("lin_a"))
        b = DataObject(source_id=src.id, object_type=DataObjectType.VIEW, name=_src_name("lin_b"))
        repo = DataObjectRepository(session)
        repo.create(a)
        repo.create(b)
        return a, b

    def test_create_and_get(self, session):
        a, b = self._make_two_objects(session)
        repo = LineageRepository(session)

        lin = Lineage(source_object_id=a.id, target_object_id=b.id, lineage_type=LineageType.DERIVED)
        repo.create(lin)

        fetched = repo.get_by_id(lin.id)
        assert fetched is not None
        assert fetched.source_object_id == a.id
        assert fetched.target_object_id == b.id
        assert fetched.lineage_type == LineageType.DERIVED

    def test_has_lineage_relationship_created(self, session):
        """The HAS_LINEAGE graph edge must exist between the two DataObject nodes."""
        a, b = self._make_two_objects(session)
        lin = Lineage(source_object_id=a.id, target_object_id=b.id)
        LineageRepository(session).create(lin)

        result = session.run(
            "MATCH (s:DataObject {id: $sid})-[r:HAS_LINEAGE]->(t:DataObject {id: $tid}) "
            "RETURN count(r) AS cnt",
            sid=str(a.id), tid=str(b.id),
        ).single()
        assert result["cnt"] == 1

    def test_list_by_source(self, session):
        a, b = self._make_two_objects(session)
        src2 = DataSource(name=_src_name("lin_src2"), platform=Platform.TABLEAU)
        DataSourceRepository(session).create(src2)
        c = DataObject(source_id=src2.id, object_type=DataObjectType.DASHBOARD, name=_src_name("lin_c"))
        DataObjectRepository(session).create(c)

        repo = LineageRepository(session)
        l1 = Lineage(source_object_id=a.id, target_object_id=b.id)
        l2 = Lineage(source_object_id=a.id, target_object_id=c.id)
        l3 = Lineage(source_object_id=b.id, target_object_id=c.id)
        for lin in [l1, l2, l3]:
            repo.create(lin)

        from_a = repo.list_by_source(a.id)
        ids = {l.id for l in from_a}
        assert l1.id in ids
        assert l2.id in ids
        assert l3.id not in ids

    def test_list_by_target(self, session):
        a, b = self._make_two_objects(session)
        src2 = DataSource(name=_src_name("lin_tgt_src"), platform=Platform.TABLEAU)
        DataSourceRepository(session).create(src2)
        c = DataObject(source_id=src2.id, object_type=DataObjectType.DASHBOARD, name=_src_name("lin_tgt_c"))
        DataObjectRepository(session).create(c)

        repo = LineageRepository(session)
        l1 = Lineage(source_object_id=a.id, target_object_id=c.id)
        l2 = Lineage(source_object_id=b.id, target_object_id=c.id)
        for lin in [l1, l2]:
            repo.create(lin)

        to_c = repo.list_by_target(c.id)
        ids = {l.id for l in to_c}
        assert l1.id in ids
        assert l2.id in ids

    def test_downstream_traversal(self, session):
        """A → B → C: get_downstream(A) must return both B and C."""
        src = DataSource(name=_src_name("trav_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)
        a = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("trav_a"))
        b = DataObject(source_id=src.id, object_type=DataObjectType.VIEW, name=_src_name("trav_b"))
        c = DataObject(source_id=src.id, object_type=DataObjectType.DASHBOARD, name=_src_name("trav_c"))
        obj_repo = DataObjectRepository(session)
        for o in [a, b, c]:
            obj_repo.create(o)

        lin_repo = LineageRepository(session)
        lin_repo.create(Lineage(source_object_id=a.id, target_object_id=b.id))
        lin_repo.create(Lineage(source_object_id=b.id, target_object_id=c.id))

        downstream = lin_repo.get_downstream(a.id)
        downstream_ids = {d["props"]["id"] for d in downstream}
        assert str(b.id) in downstream_ids
        assert str(c.id) in downstream_ids

    def test_upstream_traversal(self, session):
        """A → B → C: get_upstream(C) must return both A and B."""
        src = DataSource(name=_src_name("up_src"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)
        a = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name=_src_name("up_a"))
        b = DataObject(source_id=src.id, object_type=DataObjectType.VIEW, name=_src_name("up_b"))
        c = DataObject(source_id=src.id, object_type=DataObjectType.DASHBOARD, name=_src_name("up_c"))
        obj_repo = DataObjectRepository(session)
        for o in [a, b, c]:
            obj_repo.create(o)

        lin_repo = LineageRepository(session)
        lin_repo.create(Lineage(source_object_id=a.id, target_object_id=b.id))
        lin_repo.create(Lineage(source_object_id=b.id, target_object_id=c.id))

        upstream = lin_repo.get_upstream(c.id)
        upstream_ids = {u["props"]["id"] for u in upstream}
        assert str(a.id) in upstream_ids
        assert str(b.id) in upstream_ids

    def test_column_mappings_round_trip(self, session):
        a, b = self._make_two_objects(session)
        col_repo = ColumnRepository(session)
        src_col = Column(object_id=a.id, name=_src_name("src_col"))
        tgt_col = Column(object_id=b.id, name=_src_name("tgt_col"))
        col_repo.create(src_col)
        col_repo.create(tgt_col)

        mapping = ColumnLineageMap(
            source_column_id=src_col.id,
            target_column_id=tgt_col.id,
            transformation="UPPER(src_col)",
        )
        lin = Lineage(
            source_object_id=a.id,
            target_object_id=b.id,
            column_mappings=[mapping],
        )
        LineageRepository(session).create(lin)

        fetched = LineageRepository(session).get_by_id(lin.id)
        assert fetched is not None
        assert len(fetched.column_mappings) == 1
        assert fetched.column_mappings[0].transformation == "UPPER(src_col)"

    def test_delete_removes_relationship(self, session):
        a, b = self._make_two_objects(session)
        lin = Lineage(source_object_id=a.id, target_object_id=b.id)
        repo = LineageRepository(session)
        repo.create(lin)

        assert repo.delete(lin.id) is True
        assert repo.get_by_id(lin.id) is None

        result = session.run(
            "MATCH (s:DataObject {id: $sid})-[r:HAS_LINEAGE]->(t:DataObject {id: $tid}) "
            "RETURN count(r) AS cnt",
            sid=str(a.id), tid=str(b.id),
        ).single()
        assert result["cnt"] == 0


# ---------------------------------------------------------------------------
# Constraints & Indexes
# ---------------------------------------------------------------------------


class TestConstraintsAndIndexes:
    def test_constraints_applied_idempotently(self, session):
        """Calling apply_constraints_and_indexes twice must not raise."""
        apply_constraints_and_indexes(session)
        apply_constraints_and_indexes(session)

    def test_duplicate_id_rejected(self, session):
        """Creating two DataSource nodes with the same id must fail."""
        from neo4j.exceptions import ClientError

        src = DataSource(name=_src_name("dup"), platform=Platform.POSTGRESQL)
        DataSourceRepository(session).create(src)

        with pytest.raises(ClientError):
            session.run(
                "CREATE (n:DataSource {id: $id, name: 'other', platform: 'postgresql'})",
                id=str(src.id),
            )
