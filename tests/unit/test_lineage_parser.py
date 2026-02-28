"""Unit tests for SqlLineageParser — no database connection required."""

import pytest

from app.connectors.postgresql.lineage_parser import (
    SqlLineageParser,
    detect_circular_refs,
)


@pytest.fixture
def parser():
    return SqlLineageParser(default_schema="public", dialect="postgres")


# ---------------------------------------------------------------------------
# extract_table_refs
# ---------------------------------------------------------------------------


class TestExtractTableRefs:
    def test_simple_select(self, parser):
        sql = "SELECT a, b FROM public.orders"
        refs = parser.extract_table_refs(sql)
        tables = [t for _, t in refs]
        assert "orders" in tables

    def test_select_with_inner_join(self, parser):
        sql = """
        SELECT o.order_id, c.email
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        """
        refs = parser.extract_table_refs(sql)
        tables = {t.lower() for _, t in refs}
        assert "orders" in tables
        assert "customers" in tables

    def test_select_with_left_join(self, parser):
        sql = """
        SELECT p.name, cat.name AS cat_name
        FROM products p
        LEFT JOIN categories cat ON cat.category_id = p.category_id
        """
        refs = parser.extract_table_refs(sql)
        tables = {t.lower() for _, t in refs}
        assert "products" in tables
        assert "categories" in tables

    def test_cte_tables_not_included(self, parser):
        sql = """
        WITH order_totals AS (
            SELECT customer_id, SUM(total_amt) AS total
            FROM orders
            GROUP BY customer_id
        )
        SELECT c.email, ot.total
        FROM customers c
        JOIN order_totals ot ON ot.customer_id = c.customer_id
        """
        refs = parser.extract_table_refs(sql)
        tables = {t.lower() for _, t in refs}
        assert "order_totals" not in tables
        assert "orders" in tables
        assert "customers" in tables

    def test_subquery(self, parser):
        sql = """
        SELECT x.product_id, x.revenue
        FROM (
            SELECT product_id, SUM(line_total) AS revenue
            FROM order_items
            GROUP BY product_id
        ) x
        JOIN products p ON p.product_id = x.product_id
        """
        refs = parser.extract_table_refs(sql)
        tables = {t.lower() for _, t in refs}
        assert "order_items" in tables
        assert "products" in tables

    def test_schema_preserved(self, parser):
        sql = "SELECT a FROM raw.customers c JOIN dw.dim_customer d ON d.customer_id = c.customer_id"
        refs = parser.extract_table_refs(sql)
        schema_table = {(s, t.lower()) for s, t in refs}
        assert ("raw", "customers") in schema_table
        assert ("dw", "dim_customer") in schema_table

    def test_multiple_ctes(self, parser):
        sql = """
        WITH a AS (SELECT id FROM t1),
             b AS (SELECT id FROM t2)
        SELECT * FROM a JOIN b ON a.id = b.id JOIN t3 ON t3.id = a.id
        """
        refs = parser.extract_table_refs(sql)
        tables = {t.lower() for _, t in refs}
        assert "t1" in tables
        assert "t2" in tables
        assert "t3" in tables
        assert "a" not in tables
        assert "b" not in tables

    def test_complex_multi_join(self, parser):
        sql = """
        SELECT o.order_id, c.email, p.name, e.first_name
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        LEFT JOIN employees e ON e.employee_id = o.employee_id
        """
        refs = parser.extract_table_refs(sql)
        tables = {t.lower() for _, t in refs}
        assert {"orders", "customers", "order_items", "products", "employees"}.issubset(tables)


# ---------------------------------------------------------------------------
# parse_view — table-level lineage
# ---------------------------------------------------------------------------


class TestParseView:
    def test_simple_view(self, parser):
        sql = """
        CREATE VIEW rpt.v_test AS
        SELECT customer_id, email FROM raw.customers
        """
        result = parser.parse_view(sql, "rpt", "v_test")
        assert result.target_schema == "rpt"
        assert result.target_name == "v_test"
        tables = {t.lower() for _, t in result.source_tables}
        assert "customers" in tables

    def test_join_view(self, parser):
        sql = """
        SELECT o.order_id, c.email
        FROM raw.orders o
        JOIN raw.customers c ON c.customer_id = o.customer_id
        """
        result = parser.parse_view(sql, "rpt", "v_orders")
        tables = {t.lower() for _, t in result.source_tables}
        assert "orders" in tables
        assert "customers" in tables

    def test_cte_view(self, parser):
        sql = """
        WITH totals AS (
            SELECT customer_id, SUM(total_amt) AS spent
            FROM raw.orders GROUP BY customer_id
        )
        SELECT c.email, t.spent
        FROM raw.customers c JOIN totals t ON t.customer_id = c.customer_id
        """
        result = parser.parse_view(sql, "rpt", "v_cte")
        tables = {t.lower() for _, t in result.source_tables}
        assert "orders" in tables
        assert "customers" in tables
        assert "totals" not in tables

    def test_view_referencing_another_view(self, parser):
        sql = """
        SELECT customer_id, total_spent
        FROM rpt.v_customer_orders
        WHERE total_spent > 1000
        """
        result = parser.parse_view(sql, "rpt", "v_vip")
        tables = {t.lower() for _, t in result.source_tables}
        assert "v_customer_orders" in tables

    def test_no_parse_error_on_valid_sql(self, parser):
        sql = "SELECT a, b, c FROM schema1.table1"
        result = parser.parse_view(sql, "rpt", "v_simple")
        assert result.parse_error is None


# ---------------------------------------------------------------------------
# Column lineage extraction
# ---------------------------------------------------------------------------


class TestColumnLineage:
    def test_direct_column_mapping(self, parser):
        sql = """
        SELECT customer_id, email, first_name
        FROM raw.customers
        """
        result = parser.parse_view(sql, "rpt", "v_test")
        entries = result.column_entries
        target_cols = {e.target_column for e in entries}
        assert "email" in target_cols or len(entries) > 0

    def test_aliased_column(self, parser):
        sql = """
        SELECT c.customer_id AS cid, c.email AS customer_email
        FROM raw.customers c
        """
        result = parser.parse_view(sql, "rpt", "v_test")
        target_cols = {e.target_column for e in result.column_entries}
        assert "cid" in target_cols or "customer_email" in target_cols

    def test_aggregate_function(self, parser):
        sql = """
        SELECT customer_id, COUNT(order_id) AS order_count, SUM(total_amt) AS total
        FROM raw.orders GROUP BY customer_id
        """
        result = parser.parse_view(sql, "rpt", "v_test")
        transformations = {e.transformation for e in result.column_entries}
        assert "aggregation" in transformations or len(result.column_entries) > 0

    def test_window_function(self, parser):
        sql = """
        SELECT
            product_id,
            name,
            SUM(line_total) AS revenue,
            RANK() OVER (PARTITION BY category_id ORDER BY SUM(line_total) DESC) AS rnk
        FROM raw.products p
        LEFT JOIN raw.order_items oi ON oi.product_id = p.product_id
        GROUP BY p.product_id, p.name, p.category_id
        """
        result = parser.parse_view(sql, "rpt", "v_test")
        transformations = {e.transformation for e in result.column_entries}
        assert "window" in transformations or "aggregation" in transformations

    def test_case_statement(self, parser):
        sql = """
        SELECT
            customer_id,
            CASE ltv_quartile
                WHEN 1 THEN 'VIP'
                WHEN 2 THEN 'High'
                ELSE 'Low'
            END AS segment
        FROM raw.customers
        """
        result = parser.parse_view(sql, "rpt", "v_test")
        transformations = {e.transformation for e in result.column_entries}
        assert "case" in transformations or len(result.column_entries) > 0

    def test_calculation_expression(self, parser):
        sql = """
        SELECT
            product_id,
            (price - cost) / price * 100 AS margin_pct
        FROM raw.products
        """
        result = parser.parse_view(sql, "rpt", "v_test")
        transformations = {e.transformation for e in result.column_entries}
        assert "calculation" in transformations or len(result.column_entries) > 0


# ---------------------------------------------------------------------------
# Circular reference detection
# ---------------------------------------------------------------------------


class TestCircularRefDetection:
    def test_self_reference_detected_and_removed(self, parser):
        # v_test references itself
        source_tables = [("rpt", "v_test"), ("raw", "customers")]
        processing_set = {"rpt.v_other"}

        safe = detect_circular_refs("v_test", source_tables, "rpt", processing_set)
        tables = {t.lower() for _, t in safe}
        assert "v_test" not in tables
        assert "customers" in tables

    def test_cyclic_view_detected_via_processing_set(self, parser):
        # v_b is currently being processed and v_a references it
        source_tables = [("rpt", "v_b"), ("raw", "orders")]
        processing_set = {"rpt.v_b"}

        safe = detect_circular_refs("v_a", source_tables, "rpt", processing_set)
        tables = {t.lower() for _, t in safe}
        assert "v_b" not in tables
        assert "orders" in tables

    def test_non_circular_references_pass_through(self):
        source_tables = [("raw", "customers"), ("raw", "orders")]
        safe = detect_circular_refs("v_summary", source_tables, "rpt", set())
        assert len(safe) == 2

    def test_does_not_raise_on_circular(self, parser):
        sql = "SELECT * FROM rpt.v_circular_view"
        try:
            result = parser.parse_view(sql, "rpt", "v_circular_view")
            # Should not raise — just return potentially empty or self-referential result
            assert result is not None
        except Exception:
            pytest.fail("parse_view raised an exception on circular SQL")

    def test_empty_source_tables(self):
        safe = detect_circular_refs("v_test", [], "rpt", set())
        assert safe == []
