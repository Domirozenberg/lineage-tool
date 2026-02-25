# Universal Data Lineage Tool - Agent Instructions

## Project Overview

Build a generic data lineage tool for multi-platform environments (databases, BI tools, ETL). See `.cursor/plan.md` for the full plan.

## Conventions

### Python
- **Always use `pip3` and `python3`**—never `pip` or `python`
- Virtual env: `python3 -m venv venv`
- Run scripts: `python3 scripts/foo.py`
- Tests: `python3 -m pytest tests/`

### Scripts
- Internal scripts go in `scripts/` at project root
- Do not place scripts in project root or `app/`/`src/`

### Progress
- Update `PROGRESS.md` when a task and its tests are completed
- Mark `[ ]` → `[x]` only when implementation + tests pass

## Key Paths

| Path | Purpose |
|------|---------|
| `.cursor/plan.md` | Full project plan, phases, acceptance tests |
| `PROGRESS.md` | Task completion tracking |
| `scripts/` | Internal/dev scripts |
| `app/` or `src/` | Application code (when created) |

## Architecture (from plan)

1. Metadata Extractors (plugin-based, offline/online)
2. Metadata Normalization (DataSource, DataObject, Lineage, Column)
3. Lineage Engine (Neo4j)
4. API Layer (FastAPI)
5. Visualization (React/Vue + graph lib)

## Prohibitions

- Do not use `pip` or `python` in commands
- Do not place internal scripts outside `scripts/`
- Do not mark tasks complete in PROGRESS.md without tests passing
