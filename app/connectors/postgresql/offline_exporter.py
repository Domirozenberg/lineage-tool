"""Export PostgreSQL metadata to JSON files for offline mode.

Writes:
  tables.json          — {schema: [table_row, …]}
  columns.json         — {"schema|table": [column_row, …]}
  foreign_keys.json    — {schema: [fk_row, …]}
  view_definitions.json — {schema: [view_def_row, …]}
  functions.json       — {schema: [fn_row, …]}
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import psycopg2

from app.connectors.postgresql.extractor import (
    get_columns,
    get_foreign_keys,
    get_functions,
    get_schemas,
    get_tables,
    get_view_definitions,
)

logger = logging.getLogger(__name__)


def export_to_folder(
    connector_config: dict[str, Any],
    output_folder: str,
    schema_filter: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Connect to PostgreSQL, extract all metadata, and write JSON files.

    Args:
        connector_config: dict with host, port, dbname, user, password keys.
        output_folder: directory to write JSON files into (created if absent).
        schema_filter: optional list of schema names to include.

    Returns:
        Summary dict: {schemas, tables, views, functions, output_folder}.
    """
    os.makedirs(output_folder, exist_ok=True)

    conn = psycopg2.connect(
        host=connector_config.get("host", "localhost"),
        port=connector_config.get("port", 5432),
        dbname=connector_config.get("dbname", "postgres"),
        user=connector_config.get("user", "postgres"),
        password=connector_config.get("password", ""),
    )

    try:
        all_schemas = get_schemas(conn)
        if schema_filter:
            schemas = [s for s in all_schemas if s in schema_filter]
        else:
            schemas = all_schemas

        tables_out: dict[str, list] = {}
        columns_out: dict[str, list] = {}
        fk_out: dict[str, list] = {}
        view_out: dict[str, list] = {}
        fn_out: dict[str, list] = {}

        total_tables = total_views = total_fns = 0

        for schema in schemas:
            tables = get_tables(conn, schema)
            tables_out[schema] = tables

            for t in tables:
                if t["object_type"] in ("TABLE", "VIEW", "MATERIALIZED_VIEW"):
                    cols = get_columns(conn, schema, t["name"])
                    columns_out[f"{schema}|{t['name']}"] = cols
                if t["object_type"] == "TABLE":
                    total_tables += 1
                else:
                    total_views += 1

            fks = get_foreign_keys(conn, schema)
            if fks:
                fk_out[schema] = fks

            view_defs = get_view_definitions(conn, schema)
            if view_defs:
                view_out[schema] = view_defs

            fns = get_functions(conn, schema)
            if fns:
                fn_out[schema] = fns
                total_fns += len(fns)

    finally:
        conn.close()

    def _write(name: str, data: Any) -> None:
        path = os.path.join(output_folder, name)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        logger.info("Wrote %s", path)

    _write("tables.json", tables_out)
    _write("columns.json", columns_out)
    _write("foreign_keys.json", fk_out)
    _write("view_definitions.json", view_out)
    _write("functions.json", fn_out)

    summary = {
        "schemas": len(schemas),
        "tables": total_tables,
        "views": total_views,
        "functions": total_fns,
        "output_folder": output_folder,
        "files": {
            "tables.json": os.path.join(output_folder, "tables.json"),
            "columns.json": os.path.join(output_folder, "columns.json"),
            "foreign_keys.json": os.path.join(output_folder, "foreign_keys.json"),
            "view_definitions.json": os.path.join(output_folder, "view_definitions.json"),
            "functions.json": os.path.join(output_folder, "functions.json"),
        },
    }
    return summary
