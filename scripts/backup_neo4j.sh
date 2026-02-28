#!/usr/bin/env bash
# Backup the Neo4j database (online export via APOC).
#
# WHY:  Neo4j Community Edition does not support hot backups via
#       neo4j-admin.  Instead we use the APOC library (already
#       enabled in docker-compose.yml) to export the full graph as
#       a Cypher script that can be replayed to restore.
#
# WHAT: Runs `apoc.export.cypher.all` inside the container, then
#       copies the resulting file to a local ./backups/ directory
#       with a timestamp in the filename.
#
# USAGE:
#   bash scripts/backup_neo4j.sh
#   bash scripts/backup_neo4j.sh --out ./my-backups
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="./backups"
while [[ $# -gt 0 ]]; do
  case $1 in
    --out) OUT_DIR="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILENAME="neo4j_backup_${TIMESTAMP}.cypher"
CONTAINER_PATH="/var/lib/neo4j/import/${BACKUP_FILENAME}"
LOCAL_PATH="${OUT_DIR}/${BACKUP_FILENAME}"

mkdir -p "$OUT_DIR"

echo "==> Starting APOC export inside container..."
docker compose exec -T neo4j cypher-shell \
  -u neo4j -p password \
  "CALL apoc.export.cypher.all('${BACKUP_FILENAME}', {format:'cypher-shell', useOptimizations:{type:'UNWIND_BATCH', unwindBatchSize:100}}) YIELD file, nodes, relationships RETURN file, nodes, relationships;"

echo "==> Copying backup from container to ${LOCAL_PATH}..."
docker compose cp "neo4j:${CONTAINER_PATH}" "${LOCAL_PATH}"

echo ""
echo "Backup complete: ${LOCAL_PATH}"
echo "  Nodes + relationships exported as Cypher statements."
echo "  To restore: bash scripts/restore_neo4j.sh ${LOCAL_PATH}"
