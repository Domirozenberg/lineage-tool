"""Repository for Column nodes."""

from uuid import UUID

from app.db.base_repository import BaseRepository
from app.models.schema import Column


class ColumnRepository(BaseRepository):

    def create(self, entity: Column) -> Column:
        props = self._to_neo4j(entity)
        self._session.run(
            "MERGE (n:Column {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def get_by_id(self, entity_id: UUID) -> Column | None:
        result = self._session.run(
            "MATCH (n:Column {id: $id}) RETURN properties(n) AS props",
            id=str(entity_id),
        )
        record = result.single()
        if record is None:
            return None
        return Column.model_validate(self._from_record(dict(record["props"])))

    def list_all(self) -> list[Column]:
        result = self._session.run(
            "MATCH (n:Column) RETURN properties(n) AS props ORDER BY n.name"
        )
        return [Column.model_validate(self._from_record(dict(r["props"]))) for r in result]

    def list_by_object(self, object_id: UUID) -> list[Column]:
        """Return all columns belonging to a DataObject, ordered by position."""
        result = self._session.run(
            "MATCH (n:Column {object_id: $object_id}) "
            "RETURN properties(n) AS props "
            "ORDER BY coalesce(n.ordinal_position, 9999), n.name",
            object_id=str(object_id),
        )
        return [Column.model_validate(self._from_record(dict(r["props"]))) for r in result]

    def update(self, entity: Column) -> Column:
        props = self._to_neo4j(entity)
        self._session.run(
            "MATCH (n:Column {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def delete(self, entity_id: UUID) -> bool:
        result = self._session.run(
            "MATCH (n:Column {id: $id}) "
            "DETACH DELETE n "
            "RETURN count(n) AS deleted",
            id=str(entity_id),
        )
        record = result.single()
        return bool(record and record["deleted"] > 0)
