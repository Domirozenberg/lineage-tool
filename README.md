# Universal Data Lineage Tool

A generic data lineage tool for multi-platform environments — databases, BI tools, and ETL platforms.

## Overview

Provides comprehensive visibility into data dependencies, showing relationships between dashboards, charts, datasets, tables, views, and procedures across heterogeneous technology stacks.

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd lineage-tool

# 2. Run the setup script (creates venv, installs deps, starts Docker services)
bash scripts/setup_dev.sh

# 3. Activate the virtual environment
source venv/bin/activate

# 4. Start the API server
uvicorn app.main:app --reload
```

The API will be available at:
- API: http://localhost:8000
- Docs: http://localhost:8000/docs
- Neo4j: http://localhost:7474

## Project Structure

```
lineage-tool/
├── app/
│   ├── main.py            # FastAPI application entry point
│   ├── api/               # API routes (v1)
│   ├── connectors/        # Metadata extractor plugins
│   │   └── base.py        # BaseConnector abstract class
│   ├── models/            # Universal data schema (Pydantic)
│   ├── db/                # Neo4j connection and queries
│   └── core/              # Config, auth, utilities
├── tests/
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── scripts/
│   └── setup_dev.sh       # One-command dev environment setup
├── docs/                  # Documentation
├── docker-compose.yml     # Neo4j + Redis
├── requirements.txt       # Production dependencies
└── requirements-dev.txt   # Development dependencies
```

## Infrastructure

| Service | URL | Credentials |
|---------|-----|-------------|
| FastAPI  | http://localhost:8000 | — |
| Neo4j browser | http://localhost:7474 | neo4j / password |
| Redis | localhost:6379 | — |

Start/stop infrastructure:

```bash
docker compose up -d     # start
docker compose down      # stop
docker compose logs -f   # view logs
```

## Development

```bash
# Install dependencies
pip3 install -r requirements-dev.txt

# Run tests
python3 -m pytest tests/

# Run tests with coverage
python3 -m pytest tests/ --cov=app

# Run pre-commit checks
pre-commit run --all-files

# Start API in dev mode
uvicorn app.main:app --reload
```

## Verification

See [`docs/VERIFICATION.md`](docs/VERIFICATION.md) for step-by-step manual verification checklists for each completed task.

## Architecture

See `docs/architecture.excalidraw` for the full architecture diagram and `.cursor/plan.md` for the implementation plan.

### Core Components

1. **Metadata Extractors** — Plugin-based connectors for databases, BI tools, and ETL platforms
2. **Metadata Normalization** — Universal schema (DataSource, DataObject, Lineage, Column)
3. **Lineage Engine** — Neo4j graph database for relationship storage and traversal
4. **API Layer** — FastAPI REST + GraphQL
5. **Visualization** — React/Vue frontend with interactive graph UI

## Connector Support (Roadmap)

| Platform | Type | Status |
|----------|------|--------|
| PostgreSQL | Database | Phase 2 |
| Snowflake | Database | Phase 5 |
| Tableau | BI Tool | Phase 3 |
| PowerBI | BI Tool | Phase 5 |
| dbt | ETL | Phase 5 |
| Airflow | ETL | Phase 5 |
