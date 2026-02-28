# Tooling & Scripts Reference

This document describes every tool, library, and script used in the project:
why it was chosen, what it does, and where it appears.

> **Sections**
> 1. [Runtime Dependencies](#1-runtime-dependencies)
> 2. [Development & Test Dependencies](#2-development--test-dependencies)
> 3. [Infrastructure](#3-infrastructure)
> 4. [Code-Quality Hooks](#4-code-quality-hooks)
> 5. [Scripts](#5-scripts)
> 6. [Configuration Files](#6-configuration-files)
> 7. [Future Tools (planned phases)](#7-future-tools-planned-phases)

---

## 1. Runtime Dependencies

Defined in `requirements.txt`. Installed in production and development.

### FastAPI `>=0.110.0`
| | |
|---|---|
| **Why** | Modern, high-performance Python web framework built on Starlette and Pydantic. Chosen for automatic OpenAPI/Swagger docs generation, first-class async support, and tight Pydantic integration. |
| **What it does** | Powers the REST API layer (Phase 1.5). Handles routing, request/response parsing, dependency injection, and middleware. |
| **Used in** | `app/main.py`, `app/api/v1/` routers (Phase 1.5+) |

### Uvicorn `>=0.27.0` (with standard extras)
| | |
|---|---|
| **Why** | ASGI server required to run FastAPI. `[standard]` extras add watchfiles for `--reload` in development. |
| **What it does** | Serves the FastAPI application over HTTP. |
| **Used in** | `uvicorn app.main:app --reload` (dev), Dockerfile CMD (production) |

### Pydantic `>=2.6.0`
| | |
|---|---|
| **Why** | Data validation library used by FastAPI. Pydantic v2 is significantly faster than v1 and has a cleaner API. |
| **What it does** | Defines and validates all domain models (`DataSource`, `DataObject`, `Column`, `Lineage`), API request/response schemas, and app configuration. |
| **Used in** | `app/models/schema.py`, `app/core/config.py`, all API route models |

### Pydantic-Settings `>=2.2.0`
| | |
|---|---|
| **Why** | Official Pydantic extension for managing application configuration from environment variables and `.env` files. |
| **What it does** | Loads `Settings` class from env vars / `.env`, with typed, validated fields. |
| **Used in** | `app/core/config.py` — `Settings` class used throughout the app via `settings` singleton |

### Neo4j Python Driver `>=5.18.0`
| | |
|---|---|
| **Why** | Official Bolt-protocol driver for Neo4j. Graph database is the natural fit for lineage data (nodes = objects, edges = lineage relationships). |
| **What it does** | Opens connections to Neo4j, executes Cypher queries, and maps results to Python objects. |
| **Used in** | `app/db/` (Phase 1.4) — graph repository layer for all CRUD operations |

### Redis `>=5.0.0`
| | |
|---|---|
| **Why** | In-memory data store used as both a cache and a Celery message broker. |
| **What it does** | Caches expensive graph queries; acts as the broker for async background extraction jobs (Celery). |
| **Used in** | `app/core/` cache helpers (Phase 1.4+), Celery broker config |

### Celery `>=5.3.0`
| | |
|---|---|
| **Why** | Distributed task queue for running long-running connector extraction jobs asynchronously without blocking the API. |
| **What it does** | Schedules and executes metadata extraction tasks (e.g. full database scan, incremental sync). |
| **Used in** | `app/workers/` (Phase 2+) — async extraction tasks per connector |

### python-jose `>=3.3.0` (with cryptography)
| | |
|---|---|
| **Why** | JWT encoding/decoding library. Used for stateless authentication. |
| **What it does** | Creates and validates JWT access tokens for the API authentication system. |
| **Used in** | `app/core/security.py` (Phase 1.6) — `create_access_token`, `decode_token` |

### passlib `>=1.7.4` (with bcrypt)
| | |
|---|---|
| **Why** | Password hashing library. bcrypt is the industry-standard algorithm for storing passwords securely. |
| **What it does** | Hashes user passwords on registration; verifies passwords on login. |
| **Used in** | `app/core/security.py` (Phase 1.6) — `hash_password`, `verify_password` |

### python-multipart `>=0.0.9`
| | |
|---|---|
| **Why** | Required by FastAPI to parse `multipart/form-data` requests (e.g. login form, file upload). |
| **What it does** | Parses form-encoded request bodies. |
| **Used in** | FastAPI form endpoints (Phase 1.6 login endpoint) |

### httpx `>=0.27.0`
| | |
|---|---|
| **Why** | Async-first HTTP client. Used by connectors that call external REST APIs (Tableau, PowerBI, Airflow). Also used by FastAPI's `TestClient` in tests. |
| **What it does** | Makes authenticated HTTP requests to BI tool APIs, handles retries, streaming. |
| **Used in** | `app/connectors/tableau/` (Phase 3), `app/connectors/powerbi/` (Phase 5), test client |

### sqlglot `>=23.0.0`
| | |
|---|---|
| **Why** | SQL parser and transpiler that handles dialect-specific syntax (PostgreSQL, Snowflake, BigQuery, etc.). Zero external dependencies. |
| **What it does** | Parses SQL view definitions and stored procedures to extract table/column references for lineage. Identifies `SELECT`, `JOIN`, `CTE`, `INSERT`, `UPDATE` dependencies. |
| **Used in** | `app/connectors/postgres/sql_parser.py` (Phase 2.4), all future DB connectors |

### jsonschema `>=4.21.0`
| | |
|---|---|
| **Why** | Validates free-form `extra_metadata` dicts against JSON Schema definitions, ensuring platform-specific metadata follows expected shapes. |
| **What it does** | Runs JSON Schema Draft 7 validation; returns human-readable error messages. |
| **Used in** | `app/models/validators.py` — `validate_metadata()`, called by connector extractors before storing metadata |

### python-dotenv `>=1.0.0`
| | |
|---|---|
| **Why** | Loads `.env` files into environment variables for local development. pydantic-settings uses it automatically. |
| **What it does** | Reads `.env` at startup so developers don't need to export variables manually. |
| **Used in** | `app/core/config.py` via `pydantic-settings` `env_file=".env"` config |

---

## 2. Development & Test Dependencies

Defined in `requirements-dev.txt` (extends `requirements.txt`). Development only.

### pytest `>=8.0.0`
| | |
|---|---|
| **Why** | Standard Python test runner. Simple fixture model, rich plugin ecosystem. |
| **What it does** | Discovers and runs all files matching `tests/**/*test_*.py`, reports results. |
| **Used in** | All `tests/unit/` and `tests/integration/` files. Run via `python3 -m pytest tests/` |

### pytest-asyncio `>=0.23.0`
| | |
|---|---|
| **Why** | FastAPI route handlers are async; without this plugin, pytest can't `await` them. |
| **What it does** | Provides an event loop for async test functions. Configured in `pyproject.toml` with `asyncio_mode = "auto"`. |
| **Used in** | Any `async def test_*` functions (Phase 1.5+ API tests) |

### pytest-cov `>=5.0.0`
| | |
|---|---|
| **Why** | Measures test coverage and surfaces untested code paths. |
| **What it does** | Instruments `app/` code and generates a terminal coverage report after each test run. Configured in `pyproject.toml`. |
| **Used in** | Every `pytest` run via `addopts = "--cov=app --cov-report=term-missing"` |

### pre-commit `>=3.6.0`
| | |
|---|---|
| **Why** | Runs linters and formatters automatically before each `git commit`, preventing bad code from entering the repo. |
| **What it does** | Manages git hooks defined in `.pre-commit-config.yaml`. Installed once via `pre-commit install`. |
| **Used in** | `.pre-commit-config.yaml`, `.git/hooks/pre-commit` |

### black `>=24.3.0`
| | |
|---|---|
| **Why** | Opinionated, deterministic Python formatter. Eliminates style debates. |
| **What it does** | Reformats all `.py` files to a consistent style (88-char lines, double quotes). |
| **Used in** | Pre-commit hook (auto-runs on `git commit`), configured in `pyproject.toml [tool.black]` |

### isort `>=5.13.0`
| | |
|---|---|
| **Why** | Sorts and groups Python import statements automatically. |
| **What it does** | Reorganises imports into stdlib / third-party / local groups. Uses `profile = "black"` so it's compatible with Black's formatting. |
| **Used in** | Pre-commit hook, configured in `pyproject.toml [tool.isort]` |

### flake8 `>=7.0.0` (with flake8-bugbear)
| | |
|---|---|
| **Why** | Linter that catches logical errors and style violations Black doesn't fix. `flake8-bugbear` adds extra checks for common bugs. |
| **What it does** | Reports unused imports, undefined names, complexity issues, and Bugbear bug patterns. |
| **Used in** | Pre-commit hook, configured in `pyproject.toml` |

### mypy `>=1.9.0`
| | |
|---|---|
| **Why** | Static type checker for Python. Catches type errors before runtime. |
| **What it does** | Analyses type annotations across `app/` and reports mismatches. Configured in `pyproject.toml [tool.mypy]`. |
| **Used in** | Run manually: `python3 -m mypy app/`. Will be added to CI (Phase 1+) |

### types-redis, types-passlib
| | |
|---|---|
| **Why** | Mypy stub packages that add type information to `redis` and `passlib`, which don't ship their own stubs. |
| **What it does** | Enables mypy to type-check code that calls Redis and passlib APIs. |
| **Used in** | Consumed silently by `mypy` during static analysis |

---

## 3. Infrastructure

Defined in `docker-compose.yml`. Run via `docker compose up -d`.

### Neo4j 5.18 (with APOC plugin)
| | |
|---|---|
| **Why** | Graph database purpose-built for relationship traversal. Lineage queries ("what does this table affect?") run orders of magnitude faster in a graph than in a relational DB. APOC provides utility procedures (path algorithms, data import). |
| **What it does** | Stores all `DataSource`, `DataObject`, `Column`, `Lineage` nodes and edges. Answers impact-analysis queries via Cypher. |
| **Ports** | `7474` (browser UI), `7687` (Bolt protocol for the driver) |
| **Used in** | `app/db/` repository layer (Phase 1.4+). Neo4j browser at `http://localhost:7474` for manual inspection |

### Redis 7 (Alpine)
| | |
|---|---|
| **Why** | Lightweight, fast in-memory store. Used as both a cache and a message broker for Celery. |
| **What it does** | Caches query results; brokers async extraction tasks submitted by the API to background workers. |
| **Port** | `6379` |
| **Used in** | `app/core/cache.py` (Phase 1.4+), Celery `broker_url` config |

---

## 4. Code-Quality Hooks

Defined in `.pre-commit-config.yaml`. Installed via `pre-commit install`.

| Hook | Trigger | What it does |
|---|---|---|
| `trailing-whitespace` | commit | Removes trailing spaces from all files |
| `end-of-file-fixer` | commit | Ensures every file ends with a newline |
| `check-yaml` | commit | Validates YAML files are well-formed |
| `check-toml` | commit | Validates TOML files (e.g. `pyproject.toml`) |
| `check-added-large-files` | commit | Blocks accidental commit of binary/large files |
| `check-merge-conflict` | commit | Blocks committing unresolved merge conflict markers |
| `debug-statements` | commit | Blocks `breakpoint()` / `pdb` left in code |
| `black` | commit | Auto-formats Python files |
| `isort` | commit | Auto-sorts Python imports |
| `flake8` (+ bugbear) | commit | Lints Python files; blocks commit if errors found |

---

## 5. Scripts

All internal scripts live in `scripts/` per project conventions.

### `scripts/setup_dev.sh`
| | |
|---|---|
| **Why** | Automates the full local dev environment setup so new contributors (or CI) can get started with one command. |
| **What it does** | 1. Checks Python 3.9+ is available. 2. Creates `venv/`. 3. Installs `requirements-dev.txt`. 4. Installs pre-commit hooks. 5. Starts Docker services (Neo4j + Redis). 6. Waits for each service to become healthy. |
| **How to run** | `bash scripts/setup_dev.sh` from the project root |
| **Used in** | Onboarding, CI environment setup |

> **Planned scripts** — see [section 7](#7-future-tools-planned-phases).

---

## 6. Configuration Files

### `pyproject.toml`
Central configuration for all Python tooling. Sections:

| Section | Configures | Key settings |
|---|---|---|
| `[tool.black]` | Black formatter | `line-length = 88`, targets Python 3.9–3.11 |
| `[tool.isort]` | isort | `profile = "black"` for Black compatibility |
| `[tool.pytest.ini_options]` | pytest | `testpaths = ["tests"]`, `asyncio_mode = "auto"`, auto-coverage on every run |
| `[tool.mypy]` | mypy | Python 3.9 target, `ignore_missing_imports`, `warn_return_any` |
| `[tool.coverage.*]` | pytest-cov | Source = `app/`, shows missing lines |

### `docker-compose.yml`
Defines the local infrastructure services (Neo4j, Redis) with health checks, named volumes for persistence, and correct port mappings.

### `.pre-commit-config.yaml`
Declares all git pre-commit hooks with pinned revisions for reproducibility.

### `.env` (not committed)
Local environment overrides (DB credentials, secret keys). Loaded automatically by `pydantic-settings`. Template values are in `app/core/config.py` defaults.

---

## 7. Future Tools (Planned Phases)

Tools that will be added as the project progresses.

### Phase 1.3 — Core data models (next)
| Tool | Purpose |
|---|---|
| Neo4j driver (already installed) | Repository classes for CRUD on all four entity types |

### Phase 1.4 — Graph database setup
| Tool | Purpose |
|---|---|
| Neo4j driver + APOC | Cypher-based repository layer, index creation, constraint enforcement |

### Phase 1.5 — API framework
| Tool | Purpose |
|---|---|
| FastAPI routers | `/api/v1/sources`, `/api/v1/objects`, `/api/v1/lineage` endpoints |
| Pydantic request/response models | Input validation and serialised API responses |

### Phase 1.6 — Authentication
| Tool | Purpose |
|---|---|
| python-jose | JWT token generation and validation |
| passlib/bcrypt | Password hashing |
| FastAPI `Depends` | Protected route guards |

### Phase 2 — PostgreSQL Connector
| Tool | Purpose |
|---|---|
| `psycopg2` or `asyncpg` | Connect to PostgreSQL and query `information_schema` |
| sqlglot | Parse view/procedure SQL to extract column-level lineage |
| Celery | Run extraction jobs asynchronously |

### Phase 3 — Tableau Connector
| Tool | Purpose |
|---|---|
| httpx | Call Tableau REST API; download `.twb`/`.twbx` files |
| `xml.etree` / `lxml` | Parse Tableau workbook XML |
| sqlglot | Parse custom SQL in Tableau data sources |

### Phase 4 — Visualization UI
| Tool | Purpose |
|---|---|
| React + TypeScript | Frontend framework |
| React Flow or Cytoscape.js | Interactive lineage graph rendering |
| Material-UI or Ant Design | Component library |

### Phase 5+ — Additional Connectors
| Tool | Purpose |
|---|---|
| `snowflake-connector-python` | Snowflake metadata extraction |
| `google-cloud-bigquery` | BigQuery lineage |
| dbt Core Python API | dbt manifest parsing |
| Apache Airflow REST API | DAG/task dependency extraction |

### Observability (Phase 6)
| Tool | Purpose |
|---|---|
| Prometheus + `prometheus-fastapi-instrumentator` | API metrics |
| ELK Stack | Centralised log aggregation |
| Sentry SDK | Error tracking |

---

*Update this file when a new tool is added or a planned tool is implemented.*
