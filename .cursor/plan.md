---
planStatus:
  planId: plan-data-lineage-001
  title: Universal Data Lineage Tool for Multi-Platform Environments
  status: draft
  planType: system-design
  priority: high
  owner: drozenberg
  stakeholders: []
  tags:
    - data-engineering
    - lineage
    - etl
    - bi-tools
    - databases
    - metadata
  created: "2026-02-25"
  updated: "2026-02-25T13:00:00.000Z"
  progress: 0
---
# Universal Data Lineage Tool

## Executive Summary

Build a generic data lineage tool that can integrate with multiple customer environments using different databases, BI tools, and ETL platforms. The tool will provide comprehensive visibility into data dependencies, showing relationships between dashboards, charts, datasets, tables, views, and procedures across heterogeneous technology stacks.

## Goals & Objectives

### Primary Goals
- Create a unified lineage view across different customer technology stacks
- Support multiple BI tools (Tableau, PowerBI, Looker, Qlik, etc.)
- Support multiple databases (PostgreSQL, MySQL, Snowflake, BigQuery, Redshift, etc.)
- Support multiple ETL tools (Airflow, dbt, Informatica, Talend, etc.)
- Provide impact analysis capabilities
- Enable dependency tracking and change management

### Success Criteria
- Ability to trace data flow from source to consumption
- Support for at least 3 major BI platforms
- Support for at least 5 major database platforms
- Automated metadata extraction
- Interactive visualization of lineage graphs
- API for programmatic access

## Technical Architecture

### Core Components

#### 1. Metadata Extractors
- **Purpose**: Connect to various platforms and extract metadata
- **Key Features**:
  - Plugin-based architecture for extensibility
  - Standardized extraction interface
  - Multiple connection modes (offline/online)
  - Incremental extraction capabilities
  - Error handling and retry logic
  - Authentication abstraction layer

#### 2. Metadata Normalization Layer
- **Purpose**: Convert platform-specific metadata to common schema
- **Key Features**:
  - Universal data model for lineage
  - Mapping configurations per platform
  - Data quality validation
  - Version tracking

#### 3. Lineage Engine
- **Purpose**: Build and maintain lineage graph
- **Key Features**:
  - Graph database storage (Neo4j or similar)
  - Relationship inference
  - Impact analysis algorithms
  - Change detection

#### 4. API Layer
- **Purpose**: Provide programmatic access to lineage data
- **Key Features**:
  - RESTful API
  - GraphQL endpoint
  - Webhook support for real-time updates
  - Authentication and authorization

#### 5. Visualization Layer
- **Purpose**: Interactive UI for exploring lineage
- **Key Features**:
  - Web-based interface
  - Interactive graph visualization
  - Search and filter capabilities
  - Export functionality

## Implementation Phases

### Phase 1: Foundation & Core Architecture (Weeks 1-3)
- [ ] Set up project structure and development environment
- [ ] Design universal metadata schema
- [ ] Implement core data models
- [ ] Set up graph database
- [ ] Create basic API framework
- [ ] Implement authentication system

#### Phase 1 Acceptance Tests
```yaml
Environment Setup:
  - [ ] Python 3.9+ environment with virtual environment configured
  - [ ] Docker and Docker Compose running successfully
  - [ ] Neo4j database accessible on localhost:7474
  - [ ] Redis cache accessible on localhost:6379
  - [ ] All dependencies installed via requirements.txt
  - [ ] Pre-commit hooks configured and working

Universal Schema Tests:
  - [ ] Schema supports all core entity types (DataSource, DataObject, Lineage, Column)
  - [ ] JSON schema validation for platform-specific metadata
  - [ ] Test data successfully loads into schema
  - [ ] Schema handles nullable and required fields correctly
  - [ ] Versioning system tracks schema changes

Graph Database Tests:
  - [ ] Neo4j connection established programmatically
  - [ ] Create, read, update, delete operations work for all entities
  - [ ] Cypher queries return correct lineage relationships
  - [ ] Performance: Query 1000 nodes in <100ms
  - [ ] Backup and restore procedures documented and tested

API Framework Tests:
  - [ ] FastAPI server starts on port 8000
  - [ ] Health check endpoint returns 200 OK
  - [ ] OpenAPI documentation auto-generated at /docs
  - [ ] CORS configuration works for frontend origins
  - [ ] Request/response validation with Pydantic models
  - [ ] Error handling returns proper HTTP status codes

Authentication Tests:
  - [ ] JWT token generation and validation working
  - [ ] Login endpoint authenticates users successfully
  - [ ] Protected endpoints require valid tokens
  - [ ] Token refresh mechanism implemented
  - [ ] Role-based access control (RBAC) for admin/user roles
  - [ ] API key authentication for service accounts
  - [ ] Offline mode: Import folder validation works
  - [ ] Username/password authentication with MFA support
  - [ ] Okta SAML integration successful
  - [ ] Azure AD OAuth flow working
  - [ ] Key file authentication (JSON/PEM) validated
  - [ ] Service account credentials properly handled
  - [ ] Certificate-based auth for enterprise systems
  - [ ] Credential storage encrypted in system keyring
  - [ ] Authentication fallback mechanisms work
```

### Phase 2: First Connector - Database Lineage (Weeks 4-6)
- [ ] Build PostgreSQL metadata extractor
- [ ] Implement table/view/procedure parsing
- [ ] Create column-level lineage tracking
- [ ] Build SQL parser for dependency extraction
- [ ] Test with sample database
- [ ] Document connector patterns

#### Phase 2 Acceptance Tests
```yaml
PostgreSQL Connector Tests:
  - [ ] Connect to PostgreSQL 12+ successfully
  - [ ] Extract all tables from information_schema
  - [ ] Extract all views with their SQL definitions
  - [ ] Extract stored procedures and functions
  - [ ] Handle multiple schemas correctly
  - [ ] Connection pooling with configurable limits

Metadata Extraction Tests:
  - [ ] Table metadata includes columns, data types, constraints
  - [ ] View dependencies correctly identified from SQL
  - [ ] Foreign key relationships mapped accurately
  - [ ] Index information captured
  - [ ] Comments and descriptions preserved
  - [ ] Case-sensitive identifiers handled correctly

SQL Parser Tests:
  - [ ] Parse SELECT statements with JOINs
  - [ ] Extract table references from CTEs
  - [ ] Handle subqueries and derived tables
  - [ ] Parse INSERT/UPDATE/DELETE for data flow
  - [ ] Identify column-level transformations
  - [ ] Handle PostgreSQL-specific syntax (arrays, JSON operators)

Column Lineage Tests:
  - [ ] Direct column mappings (SELECT a FROM t)
  - [ ] Calculated columns tracked (SELECT a + b AS c)
  - [ ] JOIN columns linked correctly
  - [ ] CASE statements dependency tracked
  - [ ] Aggregate functions preserve source columns
  - [ ] Window functions maintain lineage

Integration Tests:
  - [ ] Sample database with 50+ tables loads successfully
  - [ ] View dependencies form correct DAG
  - [ ] Circular dependencies detected and reported
  - [ ] Performance: Extract 100 tables in <10 seconds
  - [ ] Incremental sync detects schema changes
  - [ ] Error recovery from connection failures

Documentation Tests:
  - [ ] Connector interface documented with examples
  - [ ] Configuration options explained
  - [ ] Troubleshooting guide includes common issues
  - [ ] Code includes inline documentation
  - [ ] API endpoints for connector management documented
```

### Phase 3: BI Tool Integration - Tableau (Weeks 7-9)
- [ ] Build Tableau REST API connector
- [ ] Extract workbook metadata
- [ ] Map dashboards, worksheets, and data sources
- [ ] Link to database objects
- [ ] Implement incremental sync
- [ ] Create integration tests

#### Phase 3 Acceptance Tests
```yaml
Tableau Connector Tests:
  - [ ] Authenticate with Tableau Server/Cloud successfully
  - [ ] Handle personal access tokens and username/password
  - [ ] Support multiple Tableau sites
  - [ ] API rate limiting handled gracefully
  - [ ] Connection retry with exponential backoff
  - [ ] Tableau version compatibility (2020.1+)

Workbook Extraction Tests:
  - [ ] List all workbooks user has access to
  - [ ] Extract workbook metadata (name, owner, project)
  - [ ] Download workbook XML (.twb) when available
  - [ ] Parse packaged workbooks (.twbx)
  - [ ] Handle published vs embedded data sources
  - [ ] Extract custom SQL from workbooks

Dashboard & Worksheet Tests:
  - [ ] Map dashboard to worksheet relationships
  - [ ] Extract dashboard filters and parameters
  - [ ] Identify cross-dashboard actions
  - [ ] Parse calculated fields and their formulas
  - [ ] Extract worksheet dimensions and measures
  - [ ] Handle dashboard zones and containers

Data Source Mapping Tests:
  - [ ] Identify database connections (type, server, database)
  - [ ] Map Tableau fields to database columns
  - [ ] Handle data source filters
  - [ ] Extract join conditions between tables
  - [ ] Parse data source calculations
  - [ ] Handle published vs embedded data sources

Cross-Platform Lineage Tests:
  - [ ] Tableau dashboard linked to PostgreSQL tables
  - [ ] Column-level lineage from DB to Tableau fields
  - [ ] Calculated fields trace to source columns
  - [ ] Multiple data sources in single workbook handled
  - [ ] Cross-database joins mapped correctly
  - [ ] Custom SQL queries parsed for dependencies

Incremental Sync Tests:
  - [ ] Detect new workbooks since last sync
  - [ ] Identify modified workbooks by timestamp
  - [ ] Handle deleted workbooks (soft delete)
  - [ ] Sync only changed objects (delta sync)
  - [ ] Maintain sync state between runs
  - [ ] Recovery from partial sync failures

Performance Tests:
  - [ ] Extract 100 workbooks in <60 seconds
  - [ ] Handle sites with 1000+ workbooks
  - [ ] Parallel extraction of workbook details
  - [ ] Caching of unchanged metadata
  - [ ] Memory usage stays under 1GB for large extracts
```

### Phase 4: Visualization & UI (Weeks 10-12)
- [ ] Design UI/UX for lineage visualization
- [ ] Implement interactive graph component
- [ ] Build search and filter features
- [ ] Create detail views for objects
- [ ] Add impact analysis views
- [ ] Implement export capabilities

#### Phase 4 Acceptance Tests
```yaml
UI Framework Tests:
  - [ ] React/Vue app builds without errors
  - [ ] Development server runs on port 3000
  - [ ] Production build optimized (<5MB bundle)
  - [ ] Responsive design works on mobile/tablet/desktop
  - [ ] Browser compatibility (Chrome, Firefox, Safari, Edge)
  - [ ] Accessibility: WCAG 2.1 AA compliant

Graph Visualization Tests:
  - [ ] Render graph with 100+ nodes smoothly
  - [ ] Zoom and pan controls work intuitively
  - [ ] Node selection highlights connected nodes
  - [ ] Edge labels show relationship types
  - [ ] Different node types have distinct visual styles
  - [ ] Graph layouts: hierarchical, force-directed, circular

Interactive Features Tests:
  - [ ] Click node to show details panel
  - [ ] Double-click to expand/collapse node connections
  - [ ] Drag nodes to reposition
  - [ ] Multi-select nodes with ctrl/cmd+click
  - [ ] Right-click context menu for actions
  - [ ] Keyboard navigation (tab, arrow keys)

Search & Filter Tests:
  - [ ] Search by object name (autocomplete)
  - [ ] Filter by object type (table, view, dashboard)
  - [ ] Filter by data source/platform
  - [ ] Filter by owner/creator
  - [ ] Date range filters for last modified
  - [ ] Save and load filter presets
  - [ ] Search results highlight in graph

Detail View Tests:
  - [ ] Object metadata displayed clearly
  - [ ] Show upstream dependencies
  - [ ] Show downstream dependencies
  - [ ] Display column-level details for tables
  - [ ] Show SQL/calculation definitions
  - [ ] Link to source system (Tableau, database)
  - [ ] Edit notes and tags for objects

Impact Analysis Tests:
  - [ ] "What if I change this?" analysis
  - [ ] Show all affected downstream objects
  - [ ] Highlight critical paths
  - [ ] Estimate impact scope (# of objects)
  - [ ] Show affected users/teams
  - [ ] Generate impact report

Export Features Tests:
  - [ ] Export graph as PNG/SVG image
  - [ ] Export lineage data as CSV
  - [ ] Export subgraph for selected nodes
  - [ ] Generate PDF lineage report
  - [ ] Export to GraphML/GEXF format
  - [ ] API endpoint for programmatic export

Performance Tests:
  - [ ] Initial page load <3 seconds
  - [ ] Graph renders 500 nodes in <2 seconds
  - [ ] Search returns results in <500ms
  - [ ] Smooth interaction at 60fps
  - [ ] Memory usage <500MB for large graphs
```

### Phase 5: Additional Connectors (Weeks 13-16)
- [ ] Add Snowflake connector
- [ ] Add PowerBI connector
- [ ] Add dbt integration
- [ ] Add Airflow DAG parsing
- [ ] Create connector development guide
- [ ] Build connector marketplace structure

#### Phase 5 Acceptance Tests
```yaml
Snowflake Connector Tests:
  - [ ] Connect using account identifier and credentials
  - [ ] Support key-pair authentication
  - [ ] Extract databases, schemas, tables, views
  - [ ] Handle Snowflake-specific objects (stages, pipes, streams)
  - [ ] Parse Snowflake SQL dialect correctly
  - [ ] Extract time-travel and clone lineage
  - [ ] Query history for usage patterns

PowerBI Connector Tests:
  - [ ] Authenticate with Azure AD
  - [ ] Extract workspaces and datasets
  - [ ] Parse DAX expressions for measures
  - [ ] Map Power Query transformations
  - [ ] Handle DirectQuery vs Import mode
  - [ ] Extract dataflow dependencies
  - [ ] Link to Azure SQL/Synapse sources

dbt Integration Tests:
  - [ ] Parse dbt project structure
  - [ ] Extract model dependencies from ref()
  - [ ] Parse source definitions
  - [ ] Handle dbt tests and documentation
  - [ ] Extract compiled SQL from models
  - [ ] Map seeds and snapshots
  - [ ] Integration with dbt Cloud API

Airflow DAG Tests:
  - [ ] Parse Python DAG files
  - [ ] Extract task dependencies
  - [ ] Identify external data sources
  - [ ] Parse SQL operators for lineage
  - [ ] Handle dynamic DAGs
  - [ ] Extract scheduling information
  - [ ] Link to upstream/downstream systems

Connector Framework Tests:
  - [ ] New connector template works
  - [ ] Plugin registration system functions
  - [ ] Configuration schema validation
  - [ ] Common utilities (SQL parser, auth) reusable
  - [ ] Error handling standardized
  - [ ] Logging format consistent

Marketplace Structure Tests:
  - [ ] List available connectors
  - [ ] Install/uninstall connectors
  - [ ] Version management for connectors
  - [ ] Connector health checks
  - [ ] Usage statistics per connector
  - [ ] Community connector submissions

Integration Tests:
  - [ ] Snowflake → dbt → Tableau lineage traced
  - [ ] Airflow → PostgreSQL → PowerBI flow mapped
  - [ ] Cross-platform column lineage maintained
  - [ ] Conflicting object names handled
  - [ ] Performance with 5+ connectors active
```

### Phase 6: Advanced Features (Weeks 17-20)
- [ ] Implement data quality metrics integration
- [ ] Add lineage versioning and history
- [ ] Build alerting system for changes
- [ ] Create compliance reporting features
- [ ] Add cost analysis capabilities
- [ ] Implement performance optimization

#### Phase 6 Acceptance Tests
```yaml
Data Quality Integration Tests:
  - [ ] Import data quality scores from external tools
  - [ ] Display quality metrics on lineage nodes
  - [ ] Track quality score trends over time
  - [ ] Quality-based filtering in UI
  - [ ] Alert on quality degradation
  - [ ] Custom quality rule definitions

Lineage Versioning Tests:
  - [ ] Capture lineage snapshots daily
  - [ ] Compare lineage between dates
  - [ ] Show added/removed/modified objects
  - [ ] Restore previous lineage state
  - [ ] Track schema evolution
  - [ ] Audit trail for all changes

Alerting System Tests:
  - [ ] Configure alerts for object changes
  - [ ] Email notifications working
  - [ ] Slack/Teams integration available
  - [ ] Alert on broken dependencies
  - [ ] Schedule-based alerts
  - [ ] Alert suppression and snoozing
  - [ ] Webhook support for custom integrations

Compliance Reporting Tests:
  - [ ] GDPR data flow tracking
  - [ ] PII field identification
  - [ ] Data retention policy checks
  - [ ] Access control audit reports
  - [ ] Regulatory compliance dashboards
  - [ ] Export compliance documentation

Cost Analysis Tests:
  - [ ] Track compute costs by lineage path
  - [ ] Identify expensive queries/transformations
  - [ ] Cost allocation by team/project
  - [ ] Optimization recommendations
  - [ ] Historical cost trending
  - [ ] ROI analysis for data assets

Performance Optimization Tests:
  - [ ] Query optimization for 10K+ nodes
  - [ ] Caching strategy reduces API calls by 70%
  - [ ] Lazy loading for large graphs
  - [ ] Database indexing optimized
  - [ ] Async processing for heavy operations
  - [ ] CDN integration for static assets
  - [ ] Load testing: Handle 100 concurrent users
```

## Technical Stack Recommendations

### Backend
- **Language**: Python (for data engineering familiarity)
- **Framework**: FastAPI (modern, async, auto-documentation)
- **Graph Database**: Neo4j or Amazon Neptune
- **Cache**: Redis
- **Queue**: Celery with RabbitMQ/Redis

### Frontend
- **Framework**: React or Vue.js
- **Graph Visualization**: D3.js, Cytoscape.js, or React Flow
- **State Management**: Redux/Zustand or Pinia
- **UI Components**: Material-UI or Ant Design

### Infrastructure
- **Containerization**: Docker
- **Orchestration**: Kubernetes (for scale)
- **Monitoring**: Prometheus + Grafana
- **Logging**: ELK Stack

## Configuration Management

### Connection Configuration Examples

#### Offline Mode - File-based Import
```yaml
connections:
  - id: customer_a_postgres_offline
    name: "Customer A PostgreSQL (Offline)"
    connector_type: postgresql
    auth_mode: offline
    config:
      import_path: /imports/customer_a/postgres/
      file_pattern: "*.sql"
      parse_ddl: true
      parse_data_lineage: true

  - id: customer_b_tableau_offline
    name: "Customer B Tableau (Offline)"
    connector_type: tableau
    auth_mode: offline
    config:
      import_path: /imports/customer_b/tableau/
      include_twb: true
      include_twbx: true
      extract_calculations: true
```

#### Online Mode - Direct Connection
```yaml
connections:
  - id: prod_snowflake
    name: "Production Snowflake"
    connector_type: snowflake
    auth_mode: key_file
    config:
      account: mycompany.us-east-1
      warehouse: COMPUTE_WH
      database: ANALYTICS
      role: LINEAGE_READER
      key_file_path: /secure/snowflake_key.pem
      key_passphrase: ${SNOWFLAKE_KEY_PASSPHRASE}

  - id: tableau_server_okta
    name: "Tableau Server (Okta SSO)"
    connector_type: tableau
    auth_mode: saml
    config:
      server_url: https://tableau.company.com
      site_id: default
      okta_domain: company.okta.com
      okta_app_id: tableau_prod
      okta_client_id: ${OKTA_CLIENT_ID}
      okta_client_secret: ${OKTA_CLIENT_SECRET}
```

#### Hybrid Mode - Mixed Access
```yaml
connections:
  - id: hybrid_setup
    name: "Hybrid Customer Setup"
    connector_type: multi
    components:
      - connector: tableau
        auth_mode: api_key
        config:
          server_url: https://tableau.customer.com
          api_key: ${TABLEAU_API_KEY}
      - connector: postgresql
        auth_mode: offline
        config:
          import_path: /imports/database_exports/
      - connector: dbt
        auth_mode: service_account
        config:
          cloud_api_url: https://cloud.getdbt.com/api/v2
          service_token: ${DBT_SERVICE_TOKEN}
```

### Security Configuration
```yaml
security:
  credential_storage:
    provider: keyring  # Options: keyring, vault, aws_secrets, azure_keyvault
    encryption: true
    rotation_days: 90

  authentication:
    session_timeout_minutes: 60
    mfa_required: true
    allowed_auth_methods:
      - okta_saml
      - azure_ad
      - api_key
      - offline

  audit:
    log_authentication: true
    log_data_access: true
    retention_days: 365

  network:
    allowed_ips:
      - 10.0.0.0/8
      - 192.168.0.0/16
    require_tls: true
    min_tls_version: "1.2"
```

## Data Model Design

### Core Entities

```yaml
DataSource:
  - id: unique identifier
  - name: display name
  - type: database/bi_tool/etl_tool
  - platform: postgres/tableau/airflow/etc
  - connection_info: encrypted credentials
  - last_sync: timestamp
  - sync_status: active/paused/error

DataObject:
  - id: unique identifier
  - source_id: reference to DataSource
  - object_type: table/view/dashboard/chart/dataset/etc
  - native_id: ID in source system
  - name: display name
  - metadata: JSON blob of platform-specific data
  - created_at: timestamp
  - updated_at: timestamp
  - deleted_at: soft delete timestamp

Lineage:
  - id: unique identifier
  - upstream_object_id: reference to DataObject
  - downstream_object_id: reference to DataObject
  - relationship_type: direct/derived/aggregated/etc
  - confidence_score: 0-1 score
  - metadata: additional relationship data
  - discovered_at: timestamp

Column:
  - id: unique identifier
  - object_id: reference to DataObject
  - name: column name
  - data_type: data type
  - is_nullable: boolean
  - metadata: JSON blob

ColumnLineage:
  - id: unique identifier
  - upstream_column_id: reference to Column
  - downstream_column_id: reference to Column
  - transformation: SQL/expression if available
```

## Connector Development Pattern

### Connection Modes

#### 1. Offline Mode
- **Use Case**: High-security environments, air-gapped systems
- **Process**: Users export metadata files manually
- **Supported Formats**:
  - Tableau: .twb/.twbx files, Server REST API export
  - PowerBI: .pbix files, workspace metadata export
  - Databases: DDL scripts, information_schema dumps
  - dbt: manifest.json, catalog.json
  - Airflow: DAG Python files

#### 2. Online Mode - Direct Connection
- **Use Case**: Standard enterprise environments
- **Authentication Methods**:
  - Username/Password with MFA support
  - API Keys/Personal Access Tokens
  - Service Account Credentials
  - OAuth 2.0 flow
  - SAML/SSO integration (Okta, Azure AD, Ping)
  - Certificate-based authentication
  - Key file authentication (JSON/PEM)

#### 3. Hybrid Mode
- **Use Case**: Partial access scenarios
- **Process**: Combine online API access with offline exports
- **Example**: Online Tableau API + offline database DDL

### Authentication Manager

```python
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict, Any
import keyring
from cryptography.fernet import Fernet

class AuthMode(Enum):
    OFFLINE = "offline"
    USERNAME_PASSWORD = "username_password"
    API_KEY = "api_key"
    OAUTH = "oauth"
    SAML = "saml"
    CERTIFICATE = "certificate"
    KEY_FILE = "key_file"
    SERVICE_ACCOUNT = "service_account"

class AuthConfig:
    """Authentication configuration"""
    def __init__(self, mode: AuthMode, **kwargs):
        self.mode = mode
        self.config = kwargs
        self.encrypted_fields = ['password', 'api_key', 'private_key']

    def get_credential(self, field: str) -> Optional[str]:
        """Retrieve credential from secure storage"""
        if field in self.encrypted_fields:
            # Use system keyring for sensitive data
            return keyring.get_password("lineage_tool", f"{self.config.get('connection_id')}_{field}")
        return self.config.get(field)

class BaseAuthenticator(ABC):
    """Base class for authentication handlers"""

    @abstractmethod
    def authenticate(self, config: AuthConfig) -> Any:
        """Perform authentication and return connection object"""
        pass

    @abstractmethod
    def refresh_token(self) -> bool:
        """Refresh authentication token if applicable"""
        pass

class OktaAuthenticator(BaseAuthenticator):
    """Okta SAML authentication handler"""

    def authenticate(self, config: AuthConfig) -> Any:
        # Implement Okta SAML flow
        # 1. Redirect to Okta login
        # 2. Handle SAML response
        # 3. Exchange for API token
        pass

    def refresh_token(self) -> bool:
        # Refresh Okta session
        pass

class OfflineAuthenticator(BaseAuthenticator):
    """Handler for offline file-based extraction"""

    def authenticate(self, config: AuthConfig) -> Any:
        # No authentication needed, just validate file paths
        import_path = config.get_credential('import_path')
        if not os.path.exists(import_path):
            raise ValueError(f"Import path does not exist: {import_path}")
        return {"mode": "offline", "path": import_path}

    def refresh_token(self) -> bool:
        return True  # No token to refresh

### Enhanced Connector Interface

```python
class BaseConnector(ABC):
    def __init__(self, auth_config: AuthConfig):
        self.auth_config = auth_config
        self.authenticator = self._get_authenticator()
        self.connection = None

    def _get_authenticator(self) -> BaseAuthenticator:
        """Factory method to get appropriate authenticator"""
        authenticators = {
            AuthMode.OFFLINE: OfflineAuthenticator,
            AuthMode.OAUTH: OAuthAuthenticator,
            AuthMode.SAML: OktaAuthenticator,
            AuthMode.API_KEY: ApiKeyAuthenticator,
            AuthMode.USERNAME_PASSWORD: BasicAuthenticator,
            AuthMode.KEY_FILE: KeyFileAuthenticator,
            AuthMode.SERVICE_ACCOUNT: ServiceAccountAuthenticator,
        }
        return authenticators[self.auth_config.mode]()

    def connect(self) -> bool:
        """Establish connection using configured authentication"""
        try:
            self.connection = self.authenticator.authenticate(self.auth_config)
            return self.test_connection()
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if connection is valid"""
        pass

    @abstractmethod
    def extract_metadata(self) -> Dict:
        """Extract all metadata from source"""
        pass

    @abstractmethod
    def extract_lineage(self) -> List[LineageRelation]:
        """Extract lineage relationships"""
        pass

    def get_incremental_changes(self, last_sync: datetime) -> Dict:
        """Get changes since last sync (not available in offline mode)"""
        if self.auth_config.mode == AuthMode.OFFLINE:
            raise NotImplementedError("Incremental sync not available in offline mode")
        return self._fetch_incremental_changes(last_sync)
```

## Integration Challenges & Solutions

### Challenge 1: Heterogeneous Metadata Formats
**Solution**: Create comprehensive mapping rules and use a flexible JSON schema for platform-specific metadata storage

### Challenge 2: Performance with Large Datasets
**Solution**: Implement pagination, caching, and incremental sync strategies

### Challenge 3: Security & Access Control
**Solution**: Use credential vaulting, implement row-level security, and maintain audit logs

### Challenge 4: Real-time Updates
**Solution**: Implement webhooks where available, polling for others, with configurable sync frequencies

### Challenge 5: Complex SQL Parsing
**Solution**: Use established SQL parsing libraries (sqlparse, sqlglot) with platform-specific extensions

## Testing Strategy

### Unit Tests
- Connector methods (mock external APIs)
- Metadata normalization functions
- Graph operations (CRUD, traversal)
- API endpoints (request/response validation)
- SQL parser accuracy
- Authentication/authorization logic

### Integration Tests
- End-to-end connector flows with real systems
- Multi-platform lineage scenarios
- Performance benchmarks
- Data consistency checks
- Error recovery scenarios
- Concurrent operation handling

### User Acceptance Tests
- Lineage accuracy validation with customer data
- UI/UX testing with actual users
- Performance under production load
- Cross-browser compatibility
- Mobile responsiveness
- Accessibility compliance

### End-to-End Acceptance Test Scenarios

#### Scenario 1: Database to BI Tool Lineage
```yaml
Given:
  - PostgreSQL database with 50 tables
  - Tableau workbook using 10 of those tables
  - Complex SQL with JOINs and CTEs

Then verify:
  - [ ] All 50 tables discovered and mapped
  - [ ] Tableau fields linked to correct columns
  - [ ] JOIN relationships preserved
  - [ ] Calculated fields traced to source columns
  - [ ] Impact analysis shows affected dashboards
  - [ ] Search finds objects by name
  - [ ] Graph visualization renders cleanly
```

#### Scenario 2: Multi-Hop Lineage
```yaml
Given:
  - Snowflake raw data tables
  - dbt transformation models
  - PowerBI reports on transformed data

Then verify:
  - [ ] Raw → Transform → Report lineage visible
  - [ ] Column-level transformations tracked
  - [ ] dbt tests reflected in quality metrics
  - [ ] PowerBI measures linked to dbt models
  - [ ] Change in raw table shows PowerBI impact
  - [ ] Historical lineage comparison works
```

#### Scenario 3: Complex ETL Pipeline
```yaml
Given:
  - Airflow DAG with 20+ tasks
  - Multiple data sources (S3, APIs, databases)
  - Target data warehouse tables

Then verify:
  - [ ] DAG structure correctly mapped
  - [ ] External data sources identified
  - [ ] Task dependencies preserved
  - [ ] SQL transformations parsed
  - [ ] Failed task impacts highlighted
  - [ ] Schedule information displayed
```

#### Scenario 4: Performance at Scale
```yaml
Given:
  - 5000+ database objects
  - 500+ BI reports
  - 100+ active users

Then verify:
  - [ ] Initial load completes in <5 minutes
  - [ ] Search returns results in <1 second
  - [ ] Graph renders 1000 nodes in <3 seconds
  - [ ] API response time <500ms (p95)
  - [ ] Memory usage <2GB per connector
  - [ ] Concurrent user operations don't conflict
```

#### Scenario 5: Authentication Modes
```yaml
Given:
  - Offline mode with exported files
  - Online mode with Okta SSO
  - API key authentication
  - Service account with key file

Then verify:
  - [ ] Offline import processes all files correctly
  - [ ] Okta SAML flow redirects and authenticates
  - [ ] API keys stored securely in keyring
  - [ ] Key files validated and permissions checked
  - [ ] Failed auth attempts logged and rate-limited
  - [ ] Credentials never exposed in logs
  - [ ] Token refresh happens automatically
  - [ ] Connection fallback to offline mode works
```

#### Scenario 6: Compliance and Security
```yaml
Given:
  - Tables with PII data
  - GDPR compliance requirements
  - Role-based access control

Then verify:
  - [ ] PII fields automatically tagged
  - [ ] Data flow report shows PII movement
  - [ ] Access logs capture all operations
  - [ ] Users see only authorized objects
  - [ ] Encryption for sensitive metadata
  - [ ] Audit trail cannot be modified
```

## Deployment Strategy

### Development Environment
- Docker Compose setup
- Mock data generators
- Local development guide

### Staging Environment
- Kubernetes deployment
- Sample customer data (anonymized)
- Performance testing setup

### Production Environment
- Multi-region deployment options
- Backup and disaster recovery
- Monitoring and alerting
- Auto-scaling configuration

## Success Metrics

- **Coverage**: Number of platforms supported
- **Accuracy**: Lineage correctness rate (>95%)
- **Performance**: Query response time (<2s for standard queries)
- **Adoption**: Number of active users/customers
- **Reliability**: System uptime (>99.9%)
- **Scalability**: Number of objects tracked (millions)

## Risk Mitigation

### Technical Risks
- **API Changes**: Version detection and adapter pattern
- **Scale Issues**: Horizontal scaling and caching strategies
- **Data Quality**: Validation rules and confidence scoring

### Business Risks
- **Platform Support**: Prioritize based on customer needs
- **Maintenance Burden**: Automated testing and monitoring
- **Security Concerns**: Regular security audits and compliance checks

## Next Steps

1. **Validate Requirements**: Review with potential users
2. **Technology POC**: Build minimal prototype with one connector
3. **Architecture Review**: Get feedback on proposed design
4. **Development Setup**: Initialize repository and CI/CD
5. **Begin Phase 1**: Start foundation development

## Quick Start Guide

### Local Development Setup

```bash
# Clone repository
git clone https://github.com/yourusername/lineage-tool.git
cd lineage-tool

# Set up Python environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Start infrastructure with Docker Compose
docker-compose up -d

# Run database migrations
alembic upgrade head

# Start the API server
uvicorn app.main:app --reload --port 8000

# In another terminal, start the frontend
cd frontend
npm install
npm run dev
```

### Running Acceptance Tests

```bash
# Run all unit tests
pytest tests/unit -v

# Run integration tests (requires Docker)
pytest tests/integration -v

# Run specific phase acceptance tests
pytest tests/acceptance/test_phase1.py -v

# Run performance tests
pytest tests/performance -v --benchmark

# Generate test coverage report
pytest --cov=app --cov-report=html

# Run end-to-end tests with real data
pytest tests/e2e --env=staging
```

### Continuous Integration

```yaml
# .github/workflows/ci.yml
name: CI Pipeline

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt
      - name: Run acceptance tests
        run: |
          docker-compose up -d
          pytest tests/acceptance -v
      - name: Check test coverage
        run: |
          pytest --cov=app --cov-fail-under=80
```

## Resources & References

- [OpenLineage](https://openlineage.io/) - Open standard for lineage
- [DataHub](https://datahubproject.io/) - Open source metadata platform
- [Apache Atlas](https://atlas.apache.org/) - Data governance and metadata
- [dbt Docs](https://docs.getdbt.com/) - For dbt integration patterns
- [Tableau REST API](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
- [PowerBI REST API](https://docs.microsoft.com/en-us/rest/api/power-bi/)

## Appendix: Sample Connector Implementations

### PostgreSQL Connector Implementation

```python
class PostgreSQLConnector(BaseConnector):
    def extract_metadata(self):
        if self.auth_config.mode == AuthMode.OFFLINE:
            return self._extract_from_dump()
        else:
            return self._extract_online()

    def _extract_from_dump(self):
        """Extract metadata from SQL dump file"""
        dump_path = self.auth_config.get_credential('import_path')

        with open(dump_path, 'r') as f:
            sql_content = f.read()

        # Parse CREATE TABLE statements
        tables = self._parse_create_tables(sql_content)
        # Parse CREATE VIEW statements
        views = self._parse_create_views(sql_content)
        # Parse CREATE FUNCTION/PROCEDURE
        procedures = self._parse_create_procedures(sql_content)

        return {
            'tables': tables,
            'views': views,
            'procedures': procedures,
            'extraction_mode': 'offline',
            'extraction_time': datetime.now()
        }

    def _extract_online(self):
        """Extract metadata via direct database connection"""
        # Query information_schema
        tables = self.query_tables()
        views = self.query_views()
        procedures = self.query_procedures()
        columns = self.query_columns()

        # Parse view definitions for dependencies
        view_dependencies = self.parse_view_sql()

        # Parse procedure code for dependencies
        proc_dependencies = self.parse_procedure_code()

        return {
            'tables': tables,
            'views': views,
            'procedures': procedures,
            'columns': columns,
            'dependencies': {**view_dependencies, **proc_dependencies},
            'extraction_mode': 'online',
            'extraction_time': datetime.now()
        }
```

### Tableau Connector Implementation

```python
class TableauConnector(BaseConnector):
    def extract_metadata(self):
        if self.auth_config.mode == AuthMode.OFFLINE:
            return self._extract_from_files()
        else:
            return self._extract_via_api()

    def _extract_from_files(self):
        """Extract from exported .twb/.twbx files"""
        import zipfile
        import xml.etree.ElementTree as ET

        export_dir = self.auth_config.get_credential('import_path')
        workbooks = []

        for file_path in Path(export_dir).glob('*.twb*'):
            if file_path.suffix == '.twbx':
                # Extract packaged workbook
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    twb_content = zip_ref.read('*.twb')
            else:
                with open(file_path, 'r') as f:
                    twb_content = f.read()

            # Parse XML content
            root = ET.fromstring(twb_content)
            workbook_data = self._parse_workbook_xml(root)
            workbooks.append(workbook_data)

        return {
            'workbooks': workbooks,
            'extraction_mode': 'offline',
            'extraction_time': datetime.now()
        }

    def _extract_via_api(self):
        """Extract via Tableau REST API with various auth methods"""
        # Authenticate based on configuration
        if self.auth_config.mode == AuthMode.SAML:
            # Use Okta SAML flow
            self._authenticate_via_okta()
        elif self.auth_config.mode == AuthMode.API_KEY:
            # Use personal access token
            token = self.auth_config.get_credential('api_key')
            self.client.auth_token = token

        # Use Tableau REST API
        workbooks = self.client.get_workbooks()

        for workbook in workbooks:
            dashboards = self.client.get_dashboards(workbook.id)
            worksheets = self.client.get_worksheets(workbook.id)
            datasources = self.client.get_datasources(workbook.id)

            # Extract connections and map to database objects
            connections = self.parse_datasource_connections(datasources)

        return {
            'workbooks': workbooks,
            'dashboards': dashboards,
            'worksheets': worksheets,
            'datasources': datasources,
            'connections': connections,
            'extraction_mode': 'online',
            'extraction_time': datetime.now()
        }

    def _authenticate_via_okta(self):
        """Handle Okta SAML authentication for Tableau"""
        # Implementation for Okta SAML flow
        pass
```

---

*This plan is a living document and will be updated as the project progresses.*
