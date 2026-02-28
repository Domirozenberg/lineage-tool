"""Neo4j schema: constraints and indexes.

Run once at application startup (or manually) to ensure the graph DB
has the right uniqueness constraints and lookup indexes.

Each entity type is stored as a Neo4j node label:
  DataSource | DataObject | Column | Lineage

Lineage *edges* (HAS_LINEAGE relationships) are stored between
DataObject nodes, with ColumnLineageMap data serialised onto the edge.
"""

from neo4j import Session

# Constraints guarantee uniqueness and implicitly create an index.
# Indexes speed up property lookups that don't need to be unique.
_CONSTRAINTS = [
    "CREATE CONSTRAINT datasource_id_unique IF NOT EXISTS "
    "FOR (n:DataSource) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT dataobject_id_unique IF NOT EXISTS "
    "FOR (n:DataObject) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT column_id_unique IF NOT EXISTS "
    "FOR (n:Column) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT lineage_id_unique IF NOT EXISTS "
    "FOR (n:Lineage) REQUIRE n.id IS UNIQUE",
]

_INDEXES = [
    "CREATE INDEX datasource_name IF NOT EXISTS "
    "FOR (n:DataSource) ON (n.name)",
    "CREATE INDEX datasource_platform IF NOT EXISTS "
    "FOR (n:DataSource) ON (n.platform)",
    "CREATE INDEX dataobject_name IF NOT EXISTS "
    "FOR (n:DataObject) ON (n.name)",
    "CREATE INDEX dataobject_type IF NOT EXISTS "
    "FOR (n:DataObject) ON (n.object_type)",
    "CREATE INDEX dataobject_source_id IF NOT EXISTS "
    "FOR (n:DataObject) ON (n.source_id)",
    "CREATE INDEX column_object_id IF NOT EXISTS "
    "FOR (n:Column) ON (n.object_id)",
    "CREATE INDEX column_name IF NOT EXISTS "
    "FOR (n:Column) ON (n.name)",
]


def apply_constraints_and_indexes(session: Session) -> None:
    """Create all constraints and indexes (idempotent — safe to re-run)."""
    for statement in _CONSTRAINTS + _INDEXES:
        session.run(statement)
