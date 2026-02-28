"""Tests for Task 1.2 — Universal Metadata Schema.

Covers:
  - All four core entity types can be created
  - Required fields are enforced
  - Optional / nullable fields default correctly
  - qualified_name computation on DataObject
  - Lineage self-reference guard (source != target)
  - JSON schema validation helpers (valid and invalid metadata)
  - Schema version constant is accessible and correctly formatted
  - Round-trip serialisation (model → dict → model)
"""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from app.models import (
    CURRENT_SCHEMA_VERSION,
    Column,
    ColumnLineageMap,
    DataObject,
    DataObjectType,
    DataSource,
    Lineage,
    LineageType,
    Platform,
    get_object_type_schema,
    get_platform_schema,
    validate_metadata,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_source() -> DataSource:
    return DataSource(
        name="prod-postgres",
        platform=Platform.POSTGRESQL,
        host="db.example.com",
        port=5432,
        database="analytics",
    )


@pytest.fixture()
def sample_object(sample_source: DataSource) -> DataObject:
    return DataObject(
        source_id=sample_source.id,
        object_type=DataObjectType.TABLE,
        name="orders",
        schema_name="public",
        database_name="analytics",
    )


@pytest.fixture()
def sample_column(sample_object: DataObject) -> Column:
    return Column(
        object_id=sample_object.id,
        name="order_id",
        data_type="integer",
        ordinal_position=0,
        is_nullable=False,
        is_primary_key=True,
    )


@pytest.fixture()
def sample_target_object(sample_source: DataSource) -> DataObject:
    return DataObject(
        source_id=sample_source.id,
        object_type=DataObjectType.VIEW,
        name="recent_orders",
        schema_name="reporting",
    )


# ---------------------------------------------------------------------------
# Schema version
# ---------------------------------------------------------------------------


class TestSchemaVersion:
    def test_current_version_is_semver(self):
        parts = CURRENT_SCHEMA_VERSION.split(".")
        assert len(parts) == 3, "Expected MAJOR.MINOR.PATCH format"
        assert all(p.isdigit() for p in parts)

    def test_entities_carry_current_version(self, sample_source):
        assert sample_source.schema_version == CURRENT_SCHEMA_VERSION

    def test_version_can_be_overridden(self):
        src = DataSource(name="old", platform=Platform.UNKNOWN, schema_version="0.9.0")
        assert src.schema_version == "0.9.0"


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------


class TestDataSource:
    def test_create_minimal(self):
        src = DataSource(name="my-db", platform=Platform.POSTGRESQL)
        assert isinstance(src.id, UUID)
        assert src.platform == Platform.POSTGRESQL
        assert src.host is None
        assert src.port is None
        assert src.extra_metadata == {}

    def test_create_full(self, sample_source):
        assert sample_source.name == "prod-postgres"
        assert sample_source.port == 5432
        assert sample_source.database == "analytics"

    def test_name_is_required(self):
        with pytest.raises(ValidationError):
            DataSource(platform=Platform.POSTGRESQL)  # type: ignore[call-arg]

    def test_platform_is_required(self):
        with pytest.raises(ValidationError):
            DataSource(name="x")  # type: ignore[call-arg]

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            DataSource(name="", platform=Platform.POSTGRESQL)

    def test_port_bounds(self):
        with pytest.raises(ValidationError):
            DataSource(name="x", platform=Platform.POSTGRESQL, port=0)
        with pytest.raises(ValidationError):
            DataSource(name="x", platform=Platform.POSTGRESQL, port=70000)

    def test_all_platforms_accepted(self):
        for platform in Platform:
            src = DataSource(name="test", platform=platform)
            assert src.platform == platform

    def test_extra_metadata_stored(self):
        src = DataSource(
            name="x",
            platform=Platform.POSTGRESQL,
            extra_metadata={"ssl_mode": "require", "max_connections": 100},
        )
        assert src.extra_metadata["ssl_mode"] == "require"

    def test_serialization_roundtrip(self, sample_source):
        data = sample_source.model_dump()
        restored = DataSource.model_validate(data)
        assert restored.id == sample_source.id
        assert restored.name == sample_source.name


# ---------------------------------------------------------------------------
# DataObject
# ---------------------------------------------------------------------------


class TestDataObject:
    def test_create_minimal(self, sample_source):
        obj = DataObject(
            source_id=sample_source.id,
            object_type=DataObjectType.TABLE,
            name="users",
        )
        assert obj.schema_name is None
        assert obj.sql_definition is None

    def test_required_fields(self):
        with pytest.raises(ValidationError):
            DataObject(object_type=DataObjectType.TABLE, name="x")  # missing source_id

    def test_qualified_name_full(self, sample_object):
        assert sample_object.qualified_name == "analytics.public.orders"

    def test_qualified_name_no_db(self, sample_source):
        obj = DataObject(
            source_id=sample_source.id,
            object_type=DataObjectType.VIEW,
            name="v_sales",
            schema_name="dw",
        )
        assert obj.qualified_name == "dw.v_sales"

    def test_qualified_name_name_only(self, sample_source):
        obj = DataObject(
            source_id=sample_source.id,
            object_type=DataObjectType.TABLE,
            name="raw",
        )
        assert obj.qualified_name == "raw"

    def test_all_object_types_accepted(self, sample_source):
        for otype in DataObjectType:
            obj = DataObject(
                source_id=sample_source.id,
                object_type=otype,
                name="test_obj",
            )
            assert obj.object_type == otype

    def test_sql_definition_stored(self, sample_source):
        obj = DataObject(
            source_id=sample_source.id,
            object_type=DataObjectType.VIEW,
            name="v",
            sql_definition="SELECT 1",
        )
        assert obj.sql_definition == "SELECT 1"

    def test_serialization_roundtrip(self, sample_object):
        data = sample_object.model_dump()
        restored = DataObject.model_validate(data)
        assert restored.id == sample_object.id
        assert restored.qualified_name == sample_object.qualified_name


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------


class TestColumn:
    def test_create_minimal(self, sample_object):
        col = Column(object_id=sample_object.id, name="email")
        assert col.data_type is None
        assert col.is_nullable is True
        assert col.is_primary_key is False

    def test_required_fields(self):
        with pytest.raises(ValidationError):
            Column(name="x")  # missing object_id

    def test_primary_key_not_nullable(self, sample_column):
        assert sample_column.is_primary_key is True
        assert sample_column.is_nullable is False

    def test_ordinal_position_non_negative(self, sample_object):
        with pytest.raises(ValidationError):
            Column(object_id=sample_object.id, name="x", ordinal_position=-1)

    def test_empty_name_rejected(self, sample_object):
        with pytest.raises(ValidationError):
            Column(object_id=sample_object.id, name="")

    def test_serialization_roundtrip(self, sample_column):
        data = sample_column.model_dump()
        restored = Column.model_validate(data)
        assert restored.id == sample_column.id
        assert restored.name == sample_column.name


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


class TestLineage:
    def test_create_minimal(self, sample_object, sample_target_object):
        lin = Lineage(
            source_object_id=sample_object.id,
            target_object_id=sample_target_object.id,
        )
        assert lin.lineage_type == LineageType.DIRECT
        assert lin.column_mappings == []

    def test_self_reference_rejected(self, sample_object):
        with pytest.raises(ValidationError, match="must be different"):
            Lineage(
                source_object_id=sample_object.id,
                target_object_id=sample_object.id,
            )

    def test_all_lineage_types_accepted(self, sample_object, sample_target_object):
        for ltype in LineageType:
            lin = Lineage(
                source_object_id=sample_object.id,
                target_object_id=sample_target_object.id,
                lineage_type=ltype,
            )
            assert lin.lineage_type == ltype

    def test_column_mappings(self, sample_object, sample_target_object, sample_column):
        target_col_id = uuid4()
        mapping = ColumnLineageMap(
            source_column_id=sample_column.id,
            target_column_id=target_col_id,
            transformation="UPPER(order_id)",
        )
        lin = Lineage(
            source_object_id=sample_object.id,
            target_object_id=sample_target_object.id,
            column_mappings=[mapping],
        )
        assert len(lin.column_mappings) == 1
        assert lin.column_mappings[0].transformation == "UPPER(order_id)"

    def test_sql_stored(self, sample_object, sample_target_object):
        sql = "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days'"
        lin = Lineage(
            source_object_id=sample_object.id,
            target_object_id=sample_target_object.id,
            sql=sql,
        )
        assert lin.sql == sql

    def test_serialization_roundtrip(self, sample_object, sample_target_object):
        lin = Lineage(
            source_object_id=sample_object.id,
            target_object_id=sample_target_object.id,
            lineage_type=LineageType.DERIVED,
        )
        data = lin.model_dump()
        restored = Lineage.model_validate(data)
        assert restored.id == lin.id
        assert restored.lineage_type == LineageType.DERIVED


# ---------------------------------------------------------------------------
# JSON Schema Validation
# ---------------------------------------------------------------------------


class TestMetadataValidation:
    def test_valid_postgresql_metadata(self):
        errors = validate_metadata(
            {"ssl_mode": "require", "max_connections": 50},
            get_platform_schema("postgresql"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_invalid_postgresql_ssl_mode(self):
        errors = validate_metadata(
            {"ssl_mode": "not-a-valid-mode"},
            get_platform_schema("postgresql"),  # type: ignore[arg-type]
        )
        assert len(errors) > 0

    def test_invalid_type_for_max_connections(self):
        errors = validate_metadata(
            {"max_connections": "lots"},
            get_platform_schema("postgresql"),  # type: ignore[arg-type]
        )
        assert len(errors) > 0

    def test_additional_properties_allowed(self):
        errors = validate_metadata(
            {"custom_key": "custom_value"},
            get_platform_schema("postgresql"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_valid_tableau_metadata(self):
        errors = validate_metadata(
            {"site_id": "abc123", "server_url": "https://tableau.example.com"},
            get_platform_schema("tableau"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_valid_table_object_metadata(self):
        errors = validate_metadata(
            {"row_count": 1000, "size_bytes": 204800, "is_partitioned": True},
            get_object_type_schema("table"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_invalid_row_count_negative(self):
        errors = validate_metadata(
            {"row_count": -1},
            get_object_type_schema("table"),  # type: ignore[arg-type]
        )
        assert len(errors) > 0

    def test_empty_metadata_is_valid(self):
        schema = get_platform_schema("postgresql")
        assert schema is not None
        errors = validate_metadata({}, schema)
        assert errors == []

    def test_get_platform_schema_unknown_returns_none(self):
        assert get_platform_schema("nonexistent_platform") is None

    def test_get_object_type_schema_unknown_returns_none(self):
        assert get_object_type_schema("nonexistent_type") is None

    def test_valid_dbt_model_metadata(self):
        errors = validate_metadata(
            {"materialization": "incremental", "tags": ["finance", "daily"]},
            get_object_type_schema("model"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_invalid_dbt_materialization(self):
        errors = validate_metadata(
            {"materialization": "streaming"},
            get_object_type_schema("model"),  # type: ignore[arg-type]
        )
        assert len(errors) > 0

    def test_valid_semantic_model_metadata_dbt(self):
        errors = validate_metadata(
            {
                "model": "fct_orders",
                "dimensions": ["order_date", "status"],
                "measures": ["order_count", "revenue"],
            },
            get_object_type_schema("semantic_model"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_valid_semantic_model_metadata_looker(self):
        errors = validate_metadata(
            {"explore": "orders", "lookml_view": "order_items"},
            get_object_type_schema("semantic_model"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_valid_semantic_model_metadata_cube(self):
        errors = validate_metadata(
            {"cube_name": "Orders", "data_source": "postgres_prod"},
            get_object_type_schema("semantic_model"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_valid_metric_metadata(self):
        errors = validate_metadata(
            {
                "type": "simple",
                "expression": "SUM(revenue)",
                "time_granularity": "day",
                "tags": ["finance"],
            },
            get_object_type_schema("metric"),  # type: ignore[arg-type]
        )
        assert errors == []

    def test_invalid_metric_type(self):
        errors = validate_metadata(
            {"type": "running_total"},
            get_object_type_schema("metric"),  # type: ignore[arg-type]
        )
        assert len(errors) > 0

    def test_invalid_metric_time_granularity(self):
        errors = validate_metadata(
            {"time_granularity": "hourly"},
            get_object_type_schema("metric"),  # type: ignore[arg-type]
        )
        assert len(errors) > 0

    def test_valid_cube_platform_metadata(self):
        errors = validate_metadata(
            {"api_url": "https://cube.example.com/cubejs-api/v1"},
            get_platform_schema("cube"),  # type: ignore[arg-type]
        )
        assert errors == []


class TestUseCaseCoverage:
    """Verify the schema supports all four primary lineage use cases."""

    def test_uc1_db_object_lineage(self, sample_source):
        """TABLE → VIEW → PROCEDURE chain is representable."""
        table = DataObject(
            source_id=sample_source.id, object_type=DataObjectType.TABLE, name="raw_events"
        )
        view = DataObject(
            source_id=sample_source.id, object_type=DataObjectType.VIEW, name="v_events"
        )
        proc = DataObject(
            source_id=sample_source.id, object_type=DataObjectType.PROCEDURE, name="load_events"
        )
        l1 = Lineage(source_object_id=table.id, target_object_id=view.id)
        l2 = Lineage(source_object_id=view.id, target_object_id=proc.id)
        assert l1.source_object_id == table.id
        assert l2.target_object_id == proc.id

    def test_uc2_bi_object_lineage(self, sample_source):
        """DATASET → CHART → DASHBOARD chain is representable."""
        dataset = DataObject(
            source_id=sample_source.id, object_type=DataObjectType.DATASET, name="sales_ds"
        )
        chart = DataObject(
            source_id=sample_source.id, object_type=DataObjectType.CHART, name="revenue_chart"
        )
        dashboard = DataObject(
            source_id=sample_source.id,
            object_type=DataObjectType.DASHBOARD,
            name="exec_dashboard",
        )
        l1 = Lineage(source_object_id=dataset.id, target_object_id=chart.id)
        l2 = Lineage(source_object_id=chart.id, target_object_id=dashboard.id)
        assert l1.source_object_id == dataset.id
        assert l2.target_object_id == dashboard.id

    def test_uc3_cross_platform_db_to_bi_lineage(self):
        """TABLE (Postgres source) → DASHBOARD (Tableau source) is representable."""
        pg_source = DataSource(name="prod-postgres", platform=Platform.POSTGRESQL)
        tableau_source = DataSource(name="prod-tableau", platform=Platform.TABLEAU)
        table = DataObject(
            source_id=pg_source.id, object_type=DataObjectType.TABLE, name="orders"
        )
        dashboard = DataObject(
            source_id=tableau_source.id,
            object_type=DataObjectType.DASHBOARD,
            name="Sales Overview",
        )
        lin = Lineage(source_object_id=table.id, target_object_id=dashboard.id)
        assert lin.source_object_id == table.id
        assert lin.target_object_id == dashboard.id

    def test_uc4_semantic_layer_lineage(self):
        """TABLE → SEMANTIC_MODEL → METRIC → DASHBOARD chain is representable."""
        pg = DataSource(name="prod-postgres", platform=Platform.POSTGRESQL)
        cube = DataSource(name="cube-semantic", platform=Platform.CUBE)
        tableau = DataSource(name="prod-tableau", platform=Platform.TABLEAU)

        table = DataObject(
            source_id=pg.id, object_type=DataObjectType.TABLE, name="fct_orders"
        )
        sem_model = DataObject(
            source_id=cube.id,
            object_type=DataObjectType.SEMANTIC_MODEL,
            name="Orders",
            extra_metadata={"cube_name": "Orders", "data_source": "prod-postgres"},
        )
        metric = DataObject(
            source_id=cube.id,
            object_type=DataObjectType.METRIC,
            name="revenue",
            extra_metadata={"type": "simple", "expression": "SUM(amount)"},
        )
        dashboard = DataObject(
            source_id=tableau.id,
            object_type=DataObjectType.DASHBOARD,
            name="Revenue Dashboard",
        )

        l1 = Lineage(source_object_id=table.id, target_object_id=sem_model.id)
        l2 = Lineage(source_object_id=sem_model.id, target_object_id=metric.id)
        l3 = Lineage(source_object_id=metric.id, target_object_id=dashboard.id)

        assert l1.source_object_id == table.id
        assert l2.source_object_id == sem_model.id
        assert l3.target_object_id == dashboard.id
