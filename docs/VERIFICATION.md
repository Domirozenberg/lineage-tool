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

*Add a new section here after each completed task.*
