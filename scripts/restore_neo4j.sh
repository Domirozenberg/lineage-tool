#!/usr/bin/env bash
# Restore the Neo4j database from an APOC Cypher export.
#
# WHY:  Replays the Cypher statements produced by backup_neo4j.sh.
#       Because the export uses MERGE (not CREATE), re-running on an
#       existing DB is idempotent — existing nodes are updated in place.
#
# WHAT: Copies the backup file into the container's import directory,
#       then replays it via cypher-shell.  Optionally wipes the DB
#       first with --clean for a true point-in-time restore.
#
# USAGE:
#   bash scripts/restore_neo4j.sh backups/neo4j_backup_20260228_120000.cypher
#   bash scripts/restore_neo4j.sh --clean backups/neo4j_backup_20260228_120000.cypher
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLEAN=false
BACKUP_FILE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --clean) CLEAN=true; shift ;;
    *) BACKUP_FILE="$1"; shift ;;
  esac
done

if [[ -z "$BACKUP_FILE" ]]; then
  echo "Usage: bash scripts/restore_neo4j.sh [--clean] <backup_file.cypher>"
  exit 1
fi

if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "ERROR: Backup file not found: ${BACKUP_FILE}"
  exit 1
fi

FILENAME=$(basename "$BACKUP_FILE")
CONTAINER_PATH="/var/lib/neo4j/import/${FILENAME}"

echo "==> Copying backup file into container..."
docker compose cp "${BACKUP_FILE}" "neo4j:${CONTAINER_PATH}"

if [[ "$CLEAN" == "true" ]]; then
  echo "==> --clean specified: wiping existing graph data..."
  docker compose exec -T neo4j cypher-shell \
    -u neo4j -p password \
    "MATCH (n) DETACH DELETE n;"
fi

echo "==> Replaying Cypher statements from ${FILENAME}..."
docker compose exec -T neo4j cypher-shell \
  -u neo4j -p password \
  --file "${CONTAINER_PATH}"

echo ""
echo "Restore complete from: ${BACKUP_FILE}"
