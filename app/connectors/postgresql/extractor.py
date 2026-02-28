"""Low-level SQL queries against information_schema and pg_catalog for PostgreSQL metadata."""

from __future__ import annotations

from typing import Any

import psycopg2.extras

_SYSTEM_SCHEMAS = frozenset({
    "pg_catalog",
    "information_schema",
    "pg_toast",
})


def _is_system_schema(name: str) -> bool:
    return name in _SYSTEM_SCHEMAS or name.startswith("pg_temp")


def get_schemas(conn) -> list[str]:
    """Return non-system schema names."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
        )
        return [row[0] for row in cur.fetchall() if not _is_system_schema(row[0])]


def get_tables(conn, schema: str) -> list[dict[str, Any]]:
    """Return tables, views, and materialized views for the given schema.

    Each row has keys: name, object_type, description, row_count_estimate, tablespace.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Regular tables and views from information_schema
        cur.execute(
            """
            SELECT
                t.table_name               AS name,
                t.table_type               AS raw_type,
                obj_description(
                    (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass::oid,
                    'pg_class'
                )                          AS description,
                pg_class.reltuples::BIGINT AS row_count_estimate,
                COALESCE(pg_tablespace.spcname, 'pg_default') AS tablespace
            FROM information_schema.tables t
            JOIN pg_class ON pg_class.relname = t.table_name
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                AND pg_namespace.nspname = t.table_schema
            LEFT JOIN pg_tablespace ON pg_tablespace.oid = pg_class.reltablespace
            WHERE t.table_schema = %s
            ORDER BY t.table_name
            """,
            (schema,),
        )
        rows = [dict(r) for r in cur.fetchall()]

        # Materialized views from pg_matviews
        cur.execute(
            """
            SELECT
                matviewname  AS name,
                'MATERIALIZED VIEW' AS raw_type,
                obj_description(
                    (quote_ident(schemaname) || '.' || quote_ident(matviewname))::regclass::oid,
                    'pg_class'
                )            AS description,
                pg_class.reltuples::BIGINT AS row_count_estimate,
                'pg_default' AS tablespace
            FROM pg_matviews
            JOIN pg_class ON pg_class.relname = matviewname
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                AND pg_namespace.nspname = schemaname
            WHERE schemaname = %s
            ORDER BY matviewname
            """,
            (schema,),
        )
        rows.extend(dict(r) for r in cur.fetchall())

    # Normalise raw_type → object_type string
    for row in rows:
        raw = row.pop("raw_type", "")
        if raw == "BASE TABLE":
            row["object_type"] = "TABLE"
        elif raw == "VIEW":
            row["object_type"] = "VIEW"
        elif raw == "MATERIALIZED VIEW":
            row["object_type"] = "MATERIALIZED_VIEW"
        else:
            row["object_type"] = "UNKNOWN"

    return rows


def get_columns(conn, schema: str, table: str) -> list[dict[str, Any]]:
    """Return columns for a given table/view.

    Each row has: name, data_type, pg_type, ordinal_position, is_nullable,
                  column_default, is_primary_key.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                c.column_name                                   AS name,
                c.data_type                                     AS pg_data_type,
                c.udt_name                                      AS udt_name,
                c.character_maximum_length                      AS char_max_len,
                c.numeric_precision                             AS num_precision,
                c.numeric_scale                                 AS num_scale,
                c.ordinal_position,
                c.is_nullable = 'YES'                           AS is_nullable,
                c.column_default,
                col_description(
                    (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass::oid,
                    c.ordinal_position
                )                                               AS description,
                EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON kcu.constraint_name = tc.constraint_name
                        AND kcu.table_schema   = tc.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema    = c.table_schema
                      AND tc.table_name      = c.table_name
                      AND kcu.column_name    = c.column_name
                ) AS is_primary_key
            FROM information_schema.columns c
            WHERE c.table_schema = %s
              AND c.table_name   = %s
            ORDER BY c.ordinal_position
            """,
            (schema, table),
        )
        rows = [dict(r) for r in cur.fetchall()]

    # Build a human-readable pg_type string with precision/length
    for row in rows:
        pg_data_type = row.pop("pg_data_type", "")
        udt_name = row.pop("udt_name", "")
        char_max = row.pop("char_max_len", None)
        num_prec = row.pop("num_precision", None)
        num_scale = row.pop("num_scale", None)

        if pg_data_type in ("character varying", "varchar", "char", "character") and char_max:
            row["pg_type"] = f"{pg_data_type}({char_max})"
        elif pg_data_type in ("numeric", "decimal") and num_prec is not None:
            row["pg_type"] = f"{pg_data_type}({num_prec},{num_scale or 0})"
        elif pg_data_type == "ARRAY":
            row["pg_type"] = f"{udt_name}[]"
        else:
            row["pg_type"] = pg_data_type or udt_name

        row["data_type"] = _map_pg_type(row["pg_type"])

    return rows


def get_foreign_keys(conn, schema: str) -> list[dict[str, Any]]:
    """Return FK relationships for all tables in the schema.

    Each row has: source_table, source_column, target_table, target_column,
                  constraint_name.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                kcu.table_name      AS source_table,
                kcu.column_name     AS source_column,
                ccu.table_name      AS target_table,
                ccu.column_name     AS target_column,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON kcu.constraint_name = tc.constraint_name
                AND kcu.table_schema   = tc.table_schema
            JOIN information_schema.referential_constraints rc
                ON rc.constraint_name        = tc.constraint_name
                AND rc.constraint_schema     = tc.constraint_schema
            JOIN information_schema.key_column_usage ccu
                ON ccu.constraint_name       = rc.unique_constraint_name
                AND ccu.table_schema         = rc.unique_constraint_schema
                AND ccu.ordinal_position     = kcu.ordinal_position
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema    = %s
            ORDER BY kcu.table_name, tc.constraint_name
            """,
            (schema,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_view_definitions(conn, schema: str) -> list[dict[str, Any]]:
    """Return view SQL definitions, including materialized views.

    Each row has: name, view_definition, is_materialized.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Regular views
        cur.execute(
            """
            SELECT
                table_name  AS name,
                view_definition,
                FALSE       AS is_materialized
            FROM information_schema.views
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (schema,),
        )
        rows = [dict(r) for r in cur.fetchall()]

        # Materialized views
        cur.execute(
            """
            SELECT
                matviewname  AS name,
                definition   AS view_definition,
                TRUE         AS is_materialized
            FROM pg_matviews
            WHERE schemaname = %s
            ORDER BY matviewname
            """,
            (schema,),
        )
        rows.extend(dict(r) for r in cur.fetchall())

    return rows


def get_functions(conn, schema: str) -> list[dict[str, Any]]:
    """Return stored functions/procedures in the schema.

    Each row has: name, return_type, argument_types, language, source, object_type.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                p.proname                           AS name,
                pg_get_function_result(p.oid)       AS return_type,
                pg_get_function_arguments(p.oid)    AS argument_types,
                l.lanname                           AS language,
                pg_get_functiondef(p.oid)           AS source,
                CASE p.prokind
                    WHEN 'p' THEN 'PROCEDURE'
                    ELSE 'FUNCTION'
                END                                 AS object_type,
                obj_description(p.oid, 'pg_proc')   AS description
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_language  l ON l.oid = p.prolang
            WHERE n.nspname = %s
            ORDER BY p.proname
            """,
            (schema,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_indexes(conn, schema: str, table: str) -> list[dict[str, Any]]:
    """Return index information for a table."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                i.relname       AS index_name,
                ix.indisprimary AS is_primary,
                ix.indisunique  AS is_unique,
                array_to_string(
                    ARRAY(
                        SELECT pg_get_indexdef(ix.indexrelid, k+1, TRUE)
                        FROM generate_subscripts(ix.indkey, 1) AS k
                    ), ', '
                ) AS columns
            FROM pg_class t
            JOIN pg_namespace n  ON n.oid = t.relnamespace
            JOIN pg_index ix     ON ix.indrelid = t.oid
            JOIN pg_class i      ON i.oid = ix.indexrelid
            WHERE t.relname  = %s
              AND n.nspname  = %s
              AND t.relkind IN ('r','m')
            ORDER BY i.relname
            """,
            (table, schema),
        )
        return [dict(r) for r in cur.fetchall()]


def get_pg_version(conn) -> str:
    """Return the PostgreSQL server version string."""
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Data type mapping
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[str, str] = {
    "integer": "integer",
    "int": "integer",
    "int4": "integer",
    "bigint": "integer",
    "int8": "integer",
    "smallint": "integer",
    "int2": "integer",
    "serial": "integer",
    "bigserial": "integer",
    "smallserial": "integer",
    "numeric": "decimal",
    "decimal": "decimal",
    "real": "decimal",
    "float4": "decimal",
    "double precision": "decimal",
    "float8": "decimal",
    "money": "decimal",
    "character varying": "string",
    "varchar": "string",
    "text": "string",
    "char": "string",
    "character": "string",
    "citext": "string",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp",
    "timestamptz": "timestamp",
    "time": "timestamp",
    "time without time zone": "timestamp",
    "time with time zone": "timestamp",
    "json": "json",
    "jsonb": "json",
    "uuid": "uuid",
    "inet": "string",
    "cidr": "string",
    "macaddr": "string",
    "bytea": "string",
    "xml": "string",
    "tsvector": "string",
    "tsquery": "string",
}


def _map_pg_type(pg_type: str) -> str:
    """Map a raw PostgreSQL type string to a canonical type name."""
    if not pg_type:
        return "unknown"
    normalised = pg_type.lower().split("(")[0].strip()
    if normalised.endswith("[]"):
        return "array"
    if normalised.startswith("_"):
        return "array"
    return _TYPE_MAP.get(normalised, pg_type.lower())
