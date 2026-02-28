"""Repository for Lineage nodes and HAS_LINEAGE relationships.

Storage design
--------------
Each Lineage record is stored in two complementary ways:

1. A **:Lineage node** — holds all scalar properties (id, lineage_type,
   sql, description, etc.) for easy lookup by ID.

2. A **:HAS_LINEAGE relationship** — connects the source DataObject node
   to the target DataObject node, with the lineage id as a property.
   This is what makes traversal queries ("what does table X affect?")
   fast without joining through an intermediate node.

ColumnLineageMap entries are serialised as a JSON string on both the
:Lineage node and the :HAS_LINEAGE relationship for convenience.
"""

from uuid import UUID

from app.db.base_repository import BaseRepository
from app.models.schema import Lineage


class LineageRepository(BaseRepository):

    def create(self, entity: Lineage) -> Lineage:
        props = self._to_neo4j(entity)
        self._session.run(
            """
            MERGE (n:Lineage {id: $id}) SET n += $props
            WITH n
            MATCH (src:DataObject {id: $source_id})
            MATCH (tgt:DataObject {id: $target_id})
            MERGE (src)-[r:HAS_LINEAGE {lineage_id: $id}]->(tgt)
            SET r += {lineage_type: $lineage_type, column_mappings: $column_mappings}
            """,
            id=props["id"],
            props=props,
            source_id=props["source_object_id"],
            target_id=props["target_object_id"],
            lineage_type=props["lineage_type"],
            column_mappings=props["column_mappings"],
        )
        return entity

    def get_by_id(self, entity_id: UUID) -> Lineage | None:
        result = self._session.run(
            "MATCH (n:Lineage {id: $id}) RETURN properties(n) AS props",
            id=str(entity_id),
        )
        record = result.single()
        if record is None:
            return None
        return Lineage.model_validate(self._from_record(dict(record["props"])))

    def list_all(self) -> list[Lineage]:
        result = self._session.run(
            "MATCH (n:Lineage) RETURN properties(n) AS props"
        )
        return [Lineage.model_validate(self._from_record(dict(r["props"]))) for r in result]

    def list_by_source(self, source_object_id: UUID) -> list[Lineage]:
        """Return all lineage edges originating from a given DataObject."""
        result = self._session.run(
            "MATCH (n:Lineage {source_object_id: $id}) RETURN properties(n) AS props",
            id=str(source_object_id),
        )
        return [Lineage.model_validate(self._from_record(dict(r["props"]))) for r in result]

    def list_by_target(self, target_object_id: UUID) -> list[Lineage]:
        """Return all lineage edges pointing to a given DataObject."""
        result = self._session.run(
            "MATCH (n:Lineage {target_object_id: $id}) RETURN properties(n) AS props",
            id=str(target_object_id),
        )
        return [Lineage.model_validate(self._from_record(dict(r["props"]))) for r in result]

    def get_downstream(self, object_id: UUID, max_depth: int = 10) -> list[dict]:
        """Return all DataObject nodes reachable downstream from object_id.

        Returns a list of dicts with keys: node_props, depth, lineage_id.

        Note: max_depth is interpolated directly into the Cypher query because
        Neo4j does not allow parameters in variable-length pattern ranges.
        It is always an integer, never user-supplied string input.
        """
        query = f"""
            MATCH path = (start:DataObject {{id: $id}})-[:HAS_LINEAGE*1..{max_depth}]->(downstream:DataObject)
            RETURN properties(downstream) AS props,
                   length(path) AS depth,
                   last(relationships(path)).lineage_id AS lineage_id
            ORDER BY depth
        """
        result = self._session.run(query, id=str(object_id))
        return [
            {
                "props": self._from_record(dict(r["props"])),
                "depth": r["depth"],
                "lineage_id": r["lineage_id"],
            }
            for r in result
        ]

    def get_upstream(self, object_id: UUID, max_depth: int = 10) -> list[dict]:
        """Return all DataObject nodes that are upstream of object_id.

        Note: max_depth is interpolated directly — see get_downstream docstring.
        """
        query = f"""
            MATCH path = (upstream:DataObject)-[:HAS_LINEAGE*1..{max_depth}]->(end:DataObject {{id: $id}})
            RETURN properties(upstream) AS props,
                   length(path) AS depth,
                   last(relationships(path)).lineage_id AS lineage_id
            ORDER BY depth
        """
        result = self._session.run(query, id=str(object_id))
        return [
            {
                "props": self._from_record(dict(r["props"])),
                "depth": r["depth"],
                "lineage_id": r["lineage_id"],
            }
            for r in result
        ]

    def update(self, entity: Lineage) -> Lineage:
        props = self._to_neo4j(entity)
        self._session.run(
            "MATCH (n:Lineage {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def delete(self, entity_id: UUID) -> bool:
        result = self._session.run(
            """
            MATCH (n:Lineage {id: $id})
            WITH n, n.source_object_id AS src_id, n.target_object_id AS tgt_id, n.id AS lid
            OPTIONAL MATCH (src:DataObject {id: src_id})-[r:HAS_LINEAGE {lineage_id: lid}]->(tgt:DataObject {id: tgt_id})
            DELETE r
            DETACH DELETE n
            RETURN count(n) AS deleted
            """,
            id=str(entity_id),
        )
        record = result.single()
        return bool(record and record["deleted"] > 0)
