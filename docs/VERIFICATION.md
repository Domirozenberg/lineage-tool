# Manual Verification Checklist

Step-by-step instructions to manually verify each completed task.
Run these after setup or when validating the environment from scratch.

> **Note**: In code blocks below, copy only the command text — not the ` ```bash ` line.
> That is a syntax highlighting hint, not part of the command.

---

## Phase 1: Foundation & Core Architecture

### Task 1.1 — Project structure and development environment

#### 1. Create and activate virtual environment
```bash
cd /Users/drozenberg/lineage-tool
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements-dev.txt
```
**Expected**: No errors. `pip3 list` shows fastapi, neo4j, redis, celery, pytest, etc.

#### 2. Start infrastructure
```bash
docker compose up -d
docker compose ps
```
**Expected**: Both `lineage-neo4j` and `lineage-redis` show status `healthy`.

#### 3. Run automated tests
```bash
python3 -m pytest tests/unit/test_project_structure.py -v
```
**Expected**: `10 passed`.

#### 4. Start the API server
```bash
uvicorn app.main:app --reload
```
Then in a second terminal:
```bash
curl http://localhost:8000/health
```
**Expected**: `{"status":"ok","version":"0.1.0"}`

Open http://localhost:8000/docs in a browser.
**Expected**: Swagger UI loads with the health endpoint listed.

#### 5. Verify Neo4j
Open http://localhost:7474 in a browser.
Log in with `neo4j` / `password`.
**Expected**: Neo4j browser UI loads successfully.

#### 6. Verify Redis
```bash
docker compose exec redis redis-cli ping
```
**Expected**: `PONG`

#### 7. Verify pre-commit hooks
```bash
pre-commit install
pre-commit run --all-files
```
**Expected**: All checks pass (or auto-fix with no errors).

---

## Phase 1: Task 1.2 — Universal Metadata Schema

### 1. Verify models import cleanly
```bash
cd /Users/drozenberg/lineage-tool
source venv/bin/activate
python3 -c "from app.models import DataSource, DataObject, Column, Lineage, CURRENT_SCHEMA_VERSION; print('OK', CURRENT_SCHEMA_VERSION)"
```
**Expected**: `OK 1.0.0`

### 2. Run schema tests
```bash
python3 -m pytest tests/unit/test_schema.py -v
```
**Expected**: `44 passed`

### 3. Smoke-test entity creation
```bash
python3 - <<'EOF'
from app.models import DataSource, DataObject, DataObjectType, Platform
src = DataSource(name="prod-db", platform=Platform.POSTGRESQL, host="localhost", port=5432)
obj = DataObject(source_id=src.id, object_type=DataObjectType.TABLE,
                 name="orders", schema_name="public", database_name="analytics")
print("DataSource:", src.name, src.platform)
print("DataObject qualified_name:", obj.qualified_name)
EOF
```
**Expected**:
```
DataSource: prod-db postgresql
DataObject qualified_name: analytics.public.orders
```

### 4. Smoke-test metadata validation
```bash
python3 - <<'EOF'
from app.models import validate_metadata, get_platform_schema
schema = get_platform_schema("postgresql")
print("Valid:", validate_metadata({"ssl_mode": "require"}, schema))
print("Invalid:", validate_metadata({"ssl_mode": "bad-value"}, schema))
EOF
```
**Expected**:
```
Valid: []
Invalid: ['...is not one of...']
```

---

## Phase 1: Task 1.3 — Core Data Models / Persistence Layer

### 1. Ensure Neo4j is running
```bash
docker compose ps
```
**Expected**: `lineage-neo4j` shows status `healthy`.

### 2. Run integration tests
```bash
cd /Users/drozenberg/lineage-tool
source venv/bin/activate
python3 -m pytest tests/integration/test_repositories.py -v
```
**Expected**: `28 passed`

### 3. Verify constraints applied at startup
```bash
uvicorn app.main:app --reload
```
Open http://localhost:7474, run this Cypher:
```cypher
SHOW CONSTRAINTS
```
**Expected**: Constraints for `DataSource`, `DataObject`, `Column`, `Lineage` on `id` are listed.

### 4. Smoke-test a full lineage chain via Python
```bash
python3 - <<'EOF'
from app.db.neo4j import get_session
from app.db.constraints import apply_constraints_and_indexes
from app.db.repositories.data_source import DataSourceRepository
from app.db.repositories.data_object import DataObjectRepository
from app.db.repositories.lineage import LineageRepository
from app.models.schema import DataSource, DataObject, DataObjectType, Lineage, Platform

with get_session() as s:
    apply_constraints_and_indexes(s)
    src = DataSource(name="smoke-pg", platform=Platform.POSTGRESQL)
    DataSourceRepository(s).create(src)
    tbl = DataObject(source_id=src.id, object_type=DataObjectType.TABLE, name="smoke-orders")
    vw  = DataObject(source_id=src.id, object_type=DataObjectType.VIEW, name="smoke-v-orders")
    DataObjectRepository(s).create(tbl)
    DataObjectRepository(s).create(vw)
    lin = Lineage(source_object_id=tbl.id, target_object_id=vw.id)
    LineageRepository(s).create(lin)
    downstream = LineageRepository(s).get_downstream(tbl.id)
    print("Downstream nodes:", [d["props"]["name"] for d in downstream])
    # cleanup
    s.run("MATCH (n) WHERE n.name STARTS WITH 'smoke-' DETACH DELETE n")
EOF
```
**Expected**: `Downstream nodes: ['smoke-v-orders']`

---

*Add a new section here after each completed task.*
