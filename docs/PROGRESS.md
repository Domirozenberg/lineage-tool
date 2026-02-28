# Project Progress

Track task completion and associated tests. Update when a task and its tests are done.

## Phase 1: Foundation & Core Architecture (Weeks 1-3)

- [x] 1.1 Set up project structure and development environment
  - [x] tests/unit/test_project_structure.py (10/10 passing)
- [x] 1.2 Design universal metadata schema
  - [x] tests/unit/test_schema.py (44/44 passing)
- [x] 1.3 Implement core data models
  - [x] tests/integration/test_repositories.py (28/28 passing)
- [x] 1.4 Set up graph database
  - [x] tests/integration/test_graph_database.py (21/21 passing)
- [x] 1.5 Create basic API framework
  - [x] tests/unit/test_api_models.py (22/22 passing)
  - [x] tests/integration/test_api.py (30/30 passing — updated for auth)
- [x] 1.6 Implement authentication system
  - [x] tests/unit/test_security.py (25/25 passing)
  - [x] tests/integration/test_auth.py (22/22 passing)

## Phase 2: First Connector - Database Lineage (Weeks 4-6)

- [x] 2.1 Build PostgreSQL metadata extractor
  - [x] app/connectors/postgresql/connector.py — PostgreSQLConnector (online + offline modes)
  - [x] app/connectors/postgresql/extractor.py — low-level SQL queries against information_schema and pg_catalog
  - [x] app/connectors/postgresql/offline_exporter.py — export to JSON for offline mode
  - [x] scripts/seed_test_db.py — 35 tables, 14 views, 2 mat views, 2 functions across raw/dw/rpt schemas
  - [x] docker-compose.yml — postgres:16-alpine service on port 5433
  - [x] tests/integration/test_postgresql_connector.py (22/22 passing)
- [x] 2.2 Implement table/view/procedure parsing
  - [x] Extracts TABLE, VIEW, MATERIALIZED_VIEW, FUNCTION, PROCEDURE objects
  - [x] Extracts columns with data type mapping, nullable, PK flags
  - [x] Extracts indexes via pg_catalog
- [x] 2.3 Create column-level lineage tracking
  - [x] app/connectors/postgresql/lineage_parser.py — SqlLineageParser using sqlglot
  - [x] Column lineage for direct refs, JOINs, CTEs, window functions, CASE, aggregates, calculations
  - [x] ColumnLineageMap records linked to Lineage edges in Neo4j
  - [x] tests/unit/test_lineage_parser.py (24/24 passing)
- [x] 2.4 Build SQL parser for dependency extraction
  - [x] SqlLineageParser: extract_table_refs(), parse_view() with full dialect support
  - [x] Circular reference detection with processing_set guard
  - [x] Foreign key lineage extraction from information_schema
  - [x] View-to-view multi-hop lineage (e.g. rpt.v_vip_customer_orders → rpt.v_customer_orders)
- [x] 2.5 Test with sample database (lineage-postgres)
  - [x] 35+ tables extracted (raw=20, dw=15)
  - [x] 16+ views/mat views extracted
  - [x] 200+ columns extracted with canonical type mapping
  - [x] FK and view lineage edges produced
  - [x] Full extract completes in < 30 seconds
  - [x] Upsert idempotency verified
- [x] 2.6 Document connector patterns
  - [x] app/api/v1/routers/connectors.py — 4 API endpoints
  - [x] app/api/v1/models/connectors.py — request/response Pydantic models
  - [x] scripts/export_pg_offline.py — offline export convenience script
  - [x] tests/integration/test_connector_api.py (12/12 passing)

**Phase 2 total: 58 new tests (24 unit + 22 connector integration + 12 API integration)**
**Full suite: 266 tests passing (208 Phase 1 + 58 Phase 2)**

---

*See `.cursor/plan.md` for full plan and acceptance criteria.*
