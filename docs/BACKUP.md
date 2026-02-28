# Neo4j Backup & Restore

This project uses **APOC's online Cypher export** for backups.
Neo4j Community Edition does not support `neo4j-admin` hot backups,
but the APOC library (enabled in `docker-compose.yml`) can export the
entire graph as a Cypher script that is fully replayable.

---

## Quick Reference

```bash
# Backup → ./backups/neo4j_backup_<timestamp>.cypher
bash scripts/backup_neo4j.sh

# Backup to a custom directory
bash scripts/backup_neo4j.sh --out /mnt/nfs/lineage-backups

# Restore (idempotent — merges into existing data)
bash scripts/restore_neo4j.sh backups/neo4j_backup_20260228_120000.cypher

# Restore with full wipe first (true point-in-time restore)
bash scripts/restore_neo4j.sh --clean backups/neo4j_backup_20260228_120000.cypher
```

---

## How it Works

### Backup (`scripts/backup_neo4j.sh`)

1. Calls `apoc.export.cypher.all()` inside the running container.
   APOC writes a `.cypher` file to `/var/lib/neo4j/import/` inside
   the container — this path is whitelisted for APOC file operations.
2. Copies the file from the container to the local `./backups/` directory
   (or a custom `--out` path).
3. The export uses `UNWIND_BATCH` optimisation to keep the file size
   manageable for large graphs.

The exported file contains `MERGE` statements so replaying it on an
existing database is **idempotent** — nodes that already exist are updated,
not duplicated.

### Restore (`scripts/restore_neo4j.sh`)

1. Copies the backup file into the container's import directory.
2. Optionally runs `MATCH (n) DETACH DELETE n` to wipe the DB first
   (`--clean` flag — use for a clean point-in-time restore).
3. Replays the Cypher file via `cypher-shell --file`.

---

## Backup Schedule (Recommended)

| Environment | Frequency | Retention |
|---|---|---|
| Development | On-demand | 7 days |
| Staging | Daily (cron) | 30 days |
| Production | Hourly incremental | 90 days |

For production, wrap `backup_neo4j.sh` in a cron job or a Kubernetes
CronJob and ship the output to S3/GCS:

```bash
# Example cron (daily at 02:00)
0 2 * * * cd /opt/lineage-tool && bash scripts/backup_neo4j.sh --out /mnt/backups 2>&1 | logger -t lineage-backup
```

---

## Alternative: Docker Volume Snapshot

For a full byte-for-byte backup (includes indexes, transaction logs):

```bash
# 1. Stop Neo4j cleanly
docker compose stop neo4j

# 2. Create a tar archive of the named volume
docker run --rm \
  -v lineage-tool_neo4j_data:/data \
  -v "$(pwd)/backups":/backup \
  alpine tar czf /backup/neo4j_volume_$(date +%Y%m%d_%H%M%S).tar.gz /data

# 3. Restart Neo4j
docker compose start neo4j

# Restore
docker compose stop neo4j
docker run --rm \
  -v lineage-tool_neo4j_data:/data \
  -v "$(pwd)/backups":/backup \
  alpine sh -c "cd / && tar xzf /backup/neo4j_volume_<timestamp>.tar.gz"
docker compose start neo4j
```

> **Note**: The volume snapshot method requires a brief downtime (~5 seconds).
> Use the APOC export method for zero-downtime backups.

---

## Verifying a Backup

After a backup, verify the export is valid:

```bash
# Check file exists and is non-empty
ls -lh backups/neo4j_backup_*.cypher

# Count the MERGE statements (rough node/relationship count)
grep -c "^MERGE" backups/neo4j_backup_<timestamp>.cypher
```

After a restore, verify counts match:

```bash
docker compose exec neo4j cypher-shell -u neo4j -p password \
  "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY label;"
```

---

*Scripts: `scripts/backup_neo4j.sh`, `scripts/restore_neo4j.sh`*
*APOC docs: https://neo4j.com/labs/apoc/5/export/cypher/*
