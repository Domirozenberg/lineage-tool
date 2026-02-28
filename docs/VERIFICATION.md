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

*Add a new section here after each completed task.*
