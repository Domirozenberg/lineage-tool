"""SQL lineage extraction using sqlglot.

Parses view definitions, CTEs, and stored function bodies to produce
table-level and column-level lineage maps.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import sqlglot
import sqlglot.expressions as exp

logger = logging.getLogger(__name__)


@dataclass
class ColumnLineageEntry:
    """Lightweight column lineage record (pre-ID resolution)."""

    source_schema: Optional[str]
    source_table: str
    source_column: str
    target_column: str
    transformation: str = "direct"


@dataclass
class ParsedViewLineage:
    """Result of parsing one view's SQL."""

    target_schema: str
    target_name: str
    source_tables: list[tuple[Optional[str], str]] = field(default_factory=list)
    column_entries: list[ColumnLineageEntry] = field(default_factory=list)
    parse_error: Optional[str] = None


class SqlLineageParser:
    """Parse SQL (view definitions, CTEs, function bodies) to extract lineage.

    All parsing is best-effort: failures fall back to table-level lineage only.
    """

    def __init__(self, default_schema: str = "public", dialect: str = "postgres"):
        self.default_schema = default_schema
        self.dialect = dialect

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_view(
        self,
        sql: str,
        target_schema: str,
        target_name: str,
    ) -> ParsedViewLineage:
        """Parse a view/materialized view definition.

        Returns a ParsedViewLineage with source tables and best-effort
        column-level lineage.
        """
        result = ParsedViewLineage(target_schema=target_schema, target_name=target_name)

        try:
            table_refs = self.extract_table_refs(sql)
            result.source_tables = table_refs

            col_entries = self._extract_column_lineage(sql, target_schema, target_name)
            result.column_entries = col_entries
        except Exception as exc:
            logger.warning(
                "Column lineage parse failed for %s.%s, falling back to table-level: %s",
                target_schema,
                target_name,
                exc,
            )
            result.parse_error = str(exc)

        return result

    def extract_table_refs(self, sql: str) -> list[tuple[Optional[str], str]]:
        """Return (schema, table) tuples referenced in the SQL.

        Handles SELECT, CTEs, subqueries, JOINs.
        Excludes CTE names (they are not real tables).
        """
        try:
            statements = sqlglot.parse(sql, dialect=self.dialect, error_level=sqlglot.ErrorLevel.WARN)
        except Exception as exc:
            logger.warning("sqlglot parse error in extract_table_refs: %s", exc)
            return []

        refs: list[tuple[Optional[str], str]] = []
        seen: set[tuple[Optional[str], str]] = set()

        for stmt in statements:
            if stmt is None:
                continue
            cte_names = {cte.alias.lower() for cte in stmt.find_all(exp.CTE)}

            for table_node in stmt.find_all(exp.Table):
                tname = table_node.name
                if not tname:
                    continue
                tname_lower = tname.lower()
                if tname_lower in cte_names:
                    continue

                schema_node = table_node.args.get("db")
                schema = schema_node.name if schema_node else None

                key = (schema, tname_lower)
                if key not in seen:
                    seen.add(key)
                    refs.append((schema, tname))

        return refs

    # ------------------------------------------------------------------
    # Column-level lineage
    # ------------------------------------------------------------------

    def _extract_column_lineage(
        self,
        sql: str,
        target_schema: str,
        target_name: str,
    ) -> list[ColumnLineageEntry]:
        """Best-effort column lineage extraction."""
        statements = sqlglot.parse(sql, dialect=self.dialect, error_level=sqlglot.ErrorLevel.WARN)
        entries: list[ColumnLineageEntry] = []

        for stmt in statements:
            if stmt is None:
                continue

            # Unwrap CREATE VIEW / CREATE MATERIALIZED VIEW
            select = self._unwrap_to_select(stmt)
            if select is None:
                continue

            cte_names = {cte.alias.lower() for cte in stmt.find_all(exp.CTE)}

            table_aliases = self._collect_table_aliases(select, cte_names)
            entries.extend(
                self._process_select_columns(select, table_aliases, cte_names)
            )

        return entries

    def _unwrap_to_select(self, stmt) -> Optional[exp.Select]:
        """Return the innermost SELECT from a CREATE VIEW or direct SELECT."""
        if isinstance(stmt, exp.Select):
            return stmt
        if isinstance(stmt, exp.Create):
            expr = stmt.args.get("expression")
            if expr is not None:
                return self._unwrap_to_select(expr)
        if isinstance(stmt, exp.Subquery):
            return self._unwrap_to_select(stmt.this)
        return None

    def _collect_table_aliases(
        self,
        select: exp.Select,
        cte_names: set[str],
    ) -> dict[str, tuple[Optional[str], str]]:
        """Build alias → (schema, table) mapping for the FROM/JOIN clauses."""
        aliases: dict[str, tuple[Optional[str], str]] = {}

        for table_node in select.find_all(exp.Table):
            tname = table_node.name
            if not tname or tname.lower() in cte_names:
                continue

            schema_node = table_node.args.get("db")
            schema = schema_node.name if schema_node else None

            alias_node = table_node.args.get("alias")
            alias = alias_node.name if alias_node else tname

            aliases[alias.lower()] = (schema, tname)
            aliases[tname.lower()] = (schema, tname)

        return aliases

    def _process_select_columns(
        self,
        select: exp.Select,
        table_aliases: dict[str, tuple[Optional[str], str]],
        cte_names: set[str],
    ) -> list[ColumnLineageEntry]:
        """Walk SELECT expressions and produce ColumnLineageEntry records."""
        entries: list[ColumnLineageEntry] = []

        for i, expr in enumerate(select.expressions):
            target_col = self._resolve_alias(expr, i)

            transformation, source_cols = self._classify_expression(expr)

            for src_schema, src_table, src_col in source_cols:
                if not src_col or src_table.lower() in cte_names:
                    continue

                # Resolve table alias to real table name
                resolved = table_aliases.get(src_table.lower())
                if resolved:
                    src_schema, src_table = resolved
                elif not src_schema:
                    # Unknown table — keep as-is
                    pass

                entries.append(
                    ColumnLineageEntry(
                        source_schema=src_schema,
                        source_table=src_table,
                        source_column=src_col,
                        target_column=target_col,
                        transformation=transformation,
                    )
                )

        return entries

    def _resolve_alias(self, expr, position: int) -> str:
        """Get the output column name for a SELECT expression."""
        if isinstance(expr, exp.Alias):
            return expr.alias
        if isinstance(expr, exp.Column):
            return expr.name or f"col_{position}"
        return f"col_{position}"

    def _classify_expression(
        self,
        expr,
    ) -> tuple[str, list[tuple[Optional[str], str, str]]]:
        """Return (transformation_type, list_of_(schema, table, col)) for an expr."""
        # Unwrap Alias
        inner = expr.this if isinstance(expr, exp.Alias) else expr

        # Direct column reference: schema.table.col or table.col or col
        if isinstance(inner, exp.Column):
            table_ref = inner.table or ""
            schema_ref = inner.args.get("db")
            schema_str = schema_ref.name if schema_ref else None
            col = inner.name or ""
            return "direct", [(schema_str, table_ref, col)]

        # Window function
        if isinstance(inner, exp.Window):
            cols = self._gather_columns(inner)
            return "window", cols

        # Aggregate functions
        if isinstance(inner, exp.Anonymous) or any(
            isinstance(inner, agg_type)
            for agg_type in (
                exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min,
                exp.ArrayAgg, exp.GroupConcat,
            )
        ):
            cols = self._gather_columns(inner)
            return "aggregation", cols

        # CASE expression
        if isinstance(inner, exp.Case):
            cols = self._gather_columns(inner)
            return "case", cols

        # Arithmetic / string concat / etc.
        if isinstance(inner, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.DPipe, exp.Concat)):
            cols = self._gather_columns(inner)
            return "calculation", cols

        # Cast, coalesce, function calls → gather all column refs
        cols = self._gather_columns(inner)
        if cols:
            return "calculation", cols

        return "direct", []

    def _gather_columns(self, node) -> list[tuple[Optional[str], str, str]]:
        """Recursively collect all Column nodes beneath a given node."""
        results: list[tuple[Optional[str], str, str]] = []
        if node is None:
            return results
        for col_node in node.find_all(exp.Column):
            table_ref = col_node.table or ""
            schema_ref = col_node.args.get("db")
            schema_str = schema_ref.name if schema_ref else None
            col = col_node.name or ""
            if col:
                results.append((schema_str, table_ref, col))
        return results


# ---------------------------------------------------------------------------
# Module-level helpers for circular reference detection
# ---------------------------------------------------------------------------

def detect_circular_refs(
    view_name: str,
    source_tables: list[tuple[Optional[str], str]],
    schema: str,
    processing_set: set[str],
) -> list[tuple[Optional[str], str]]:
    """Filter out any source_tables that would create a circular reference.

    If a referenced table matches the view being processed, it is removed
    and a warning is logged.
    """
    safe: list[tuple[Optional[str], str]] = []
    for src_schema, src_table in source_tables:
        ref_key = f"{(src_schema or schema).lower()}.{src_table.lower()}"
        target_key = f"{schema.lower()}.{view_name.lower()}"
        if ref_key == target_key or ref_key in processing_set:
            logger.warning(
                "Circular reference detected: %s references %s — skipping edge.",
                target_key,
                ref_key,
            )
            continue
        safe.append((src_schema, src_table))
    return safe
