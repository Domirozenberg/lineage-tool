"""Export a PostgreSQL database to an offline folder for use with offline mode.

Usage:
  python3 scripts/export_pg_offline.py \
      --host localhost --port 5433 --dbname lineage_sample \
      --user lineage --password lineage --output ./offline_export

  # With schema filter:
  python3 scripts/export_pg_offline.py \
      --host localhost --port 5433 --dbname lineage_sample \
      --user lineage --password lineage \
      --schemas raw dw rpt --output ./offline_export
"""

import argparse
import sys

# Ensure app is importable when run from project root
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a PostgreSQL database to an offline JSON folder."
    )
    parser.add_argument("--host",     default="localhost")
    parser.add_argument("--port",     type=int, default=5433)
    parser.add_argument("--dbname",   default="lineage_sample")
    parser.add_argument("--user",     default="lineage")
    parser.add_argument("--password", default="lineage")
    parser.add_argument(
        "--schemas",
        nargs="*",
        default=None,
        help="Optional list of schemas to export (default: all non-system schemas)",
    )
    parser.add_argument(
        "--output",
        default="./offline_export",
        help="Output directory (created if it does not exist)",
    )
    args = parser.parse_args()

    config = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password,
    }

    print(f"Connecting to PostgreSQL at {args.host}:{args.port}/{args.dbname}...")
    print(f"Output folder: {args.output}")
    if args.schemas:
        print(f"Schema filter: {args.schemas}")

    try:
        from app.connectors.postgresql.offline_exporter import export_to_folder

        summary = export_to_folder(config, args.output, schema_filter=args.schemas)
    except Exception as exc:
        print(f"Export failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print("\n=== Export Summary ===")
    print(f"  Schemas:  {summary['schemas']}")
    print(f"  Tables:   {summary['tables']}")
    print(f"  Views:    {summary['views']}")
    print(f"  Functions:{summary['functions']}")
    print(f"  Files written to: {summary['output_folder']}")
    for fname, fpath in summary["files"].items():
        size = os.path.getsize(fpath) if isinstance(fpath, str) else 0
        print(f"    {fname}: {size:,} bytes")
    print("\nDone. Use --folder-path with the offline connector to import.")


if __name__ == "__main__":
    main()
