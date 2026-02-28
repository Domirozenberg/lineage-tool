"""Unit tests for API request/response models (Task 1.5).

No DB or running server required — these test only Pydantic validation.
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.api.v1.models.columns import ColumnCreate, ColumnUpdate
from app.api.v1.models.lineage import LineageCreate, LineageUpdate
from app.api.v1.models.objects import DataObjectCreate, DataObjectUpdate
from app.api.v1.models.sources import DataSourceCreate, DataSourceUpdate
from app.models.schema import DataObjectType, LineageType, Platform


class TestDataSourceCreate:
    def test_valid(self):
        body = DataSourceCreate(name="prod-pg", platform=Platform.POSTGRESQL)
        src = body.to_domain()
        assert src.name == "prod-pg"
        assert src.platform == Platform.POSTGRESQL
        assert src.id is not None

    def test_missing_name_raises(self):
        with pytest.raises(ValidationError):
            DataSourceCreate(platform=Platform.POSTGRESQL)  # type: ignore[call-arg]

    def test_missing_platform_raises(self):
        with pytest.raises(ValidationError):
            DataSourceCreate(name="x")  # type: ignore[call-arg]

    def test_port_bounds(self):
        with pytest.raises(ValidationError):
            DataSourceCreate(name="x", platform=Platform.POSTGRESQL, port=0)

    def test_extra_metadata_default_empty(self):
        body = DataSourceCreate(name="x", platform=Platform.POSTGRESQL)
        assert body.extra_metadata == {}


class TestDataSourceUpdate:
    def test_all_fields_optional(self):
        upd = DataSourceUpdate()
        assert upd.name is None

    def test_apply_to_changes_only_provided_fields(self):
        body = DataSourceCreate(name="original", platform=Platform.POSTGRESQL)
        src = body.to_domain()
        upd = DataSourceUpdate(description="new desc")
        updated = upd.apply_to(src)
        assert updated.name == "original"
        assert updated.description == "new desc"

    def test_apply_to_does_not_mutate_original(self):
        src = DataSourceCreate(name="orig", platform=Platform.POSTGRESQL).to_domain()
        upd = DataSourceUpdate(name="changed")
        _ = upd.apply_to(src)
        assert src.name == "orig"


class TestDataObjectCreate:
    def test_valid(self):
        sid = uuid4()
        body = DataObjectCreate(
            source_id=sid,
            object_type=DataObjectType.TABLE,
            name="orders",
        )
        obj = body.to_domain()
        assert obj.source_id == sid
        assert obj.object_type == DataObjectType.TABLE

    def test_missing_source_id_raises(self):
        with pytest.raises(ValidationError):
            DataObjectCreate(object_type=DataObjectType.TABLE, name="x")  # type: ignore[call-arg]

    def test_semantic_model_type_accepted(self):
        body = DataObjectCreate(
            source_id=uuid4(),
            object_type=DataObjectType.SEMANTIC_MODEL,
            name="orders_semantic",
        )
        assert body.object_type == DataObjectType.SEMANTIC_MODEL

    def test_metric_type_accepted(self):
        body = DataObjectCreate(
            source_id=uuid4(),
            object_type=DataObjectType.METRIC,
            name="revenue",
        )
        assert body.object_type == DataObjectType.METRIC


class TestDataObjectUpdate:
    def test_all_optional(self):
        upd = DataObjectUpdate()
        assert upd.name is None
        assert upd.description is None

    def test_apply_to_partial_update(self):
        src = DataObjectCreate(
            source_id=uuid4(), object_type=DataObjectType.TABLE, name="raw"
        ).to_domain()
        upd = DataObjectUpdate(description="updated description")
        updated = upd.apply_to(src)
        assert updated.name == "raw"
        assert updated.description == "updated description"


class TestColumnCreate:
    def test_valid(self):
        body = ColumnCreate(object_id=uuid4(), name="order_id", data_type="integer")
        col = body.to_domain()
        assert col.name == "order_id"
        assert col.is_nullable is True

    def test_empty_name_raises(self):
        with pytest.raises(ValidationError):
            ColumnCreate(object_id=uuid4(), name="")

    def test_negative_ordinal_raises(self):
        with pytest.raises(ValidationError):
            ColumnCreate(object_id=uuid4(), name="x", ordinal_position=-1)


class TestLineageCreate:
    def test_valid(self):
        src_id, tgt_id = uuid4(), uuid4()
        body = LineageCreate(source_object_id=src_id, target_object_id=tgt_id)
        lin = body.to_domain()
        assert lin.lineage_type == LineageType.DIRECT
        assert lin.column_mappings == []

    def test_self_reference_raises(self):
        uid = uuid4()
        with pytest.raises(ValidationError, match="must be different"):
            LineageCreate(source_object_id=uid, target_object_id=uid)

    def test_all_lineage_types_accepted(self):
        s, t = uuid4(), uuid4()
        for lt in LineageType:
            body = LineageCreate(source_object_id=s, target_object_id=t, lineage_type=lt)
            assert body.lineage_type == lt


class TestQualifiedNameInResponse:
    def test_qualified_name_serialised(self):
        """qualified_name must appear in JSON output (computed_field)."""
        body = DataObjectCreate(
            source_id=uuid4(),
            object_type=DataObjectType.TABLE,
            name="orders",
            schema_name="public",
            database_name="analytics",
        )
        obj = body.to_domain()
        data = obj.model_dump()
        assert "qualified_name" in data
        assert data["qualified_name"] == "analytics.public.orders"

    def test_qualified_name_in_json(self):
        body = DataObjectCreate(
            source_id=uuid4(),
            object_type=DataObjectType.VIEW,
            name="v_sales",
            schema_name="dw",
        )
        obj = body.to_domain()
        json_str = obj.model_dump_json()
        assert "qualified_name" in json_str
        assert "dw.v_sales" in json_str
