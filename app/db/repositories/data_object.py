"""Repository for DataObject nodes."""

from uuid import UUID

from app.db.base_repository import BaseRepository
from app.models.schema import DataObject, DataObjectType


class DataObjectRepository(BaseRepository):

    def create(self, entity: DataObject) -> DataObject:
        props = self._to_neo4j(entity)
        self._session.run(
            "MERGE (n:DataObject {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def get_by_id(self, entity_id: UUID) -> DataObject | None:
        result = self._session.run(
            "MATCH (n:DataObject {id: $id}) RETURN properties(n) AS props",
            id=str(entity_id),
        )
        record = result.single()
        if record is None:
            return None
        return DataObject.model_validate(self._from_record(dict(record["props"])))

    def list_all(self) -> list[DataObject]:
        result = self._session.run(
            "MATCH (n:DataObject) RETURN properties(n) AS props ORDER BY n.name"
        )
        return [
            DataObject.model_validate(self._from_record(dict(r["props"]))) for r in result
        ]

    def list_by_source(self, source_id: UUID) -> list[DataObject]:
        result = self._session.run(
            "MATCH (n:DataObject {source_id: $source_id}) "
            "RETURN properties(n) AS props ORDER BY n.name",
            source_id=str(source_id),
        )
        return [
            DataObject.model_validate(self._from_record(dict(r["props"]))) for r in result
        ]

    def list_by_type(self, object_type: DataObjectType) -> list[DataObject]:
        result = self._session.run(
            "MATCH (n:DataObject {object_type: $object_type}) "
            "RETURN properties(n) AS props ORDER BY n.name",
            object_type=object_type.value,
        )
        return [
            DataObject.model_validate(self._from_record(dict(r["props"]))) for r in result
        ]

    def update(self, entity: DataObject) -> DataObject:
        props = self._to_neo4j(entity)
        self._session.run(
            "MATCH (n:DataObject {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def delete(self, entity_id: UUID) -> bool:
        result = self._session.run(
            "MATCH (n:DataObject {id: $id}) "
            "DETACH DELETE n "
            "RETURN count(n) AS deleted",
            id=str(entity_id),
        )
        record = result.single()
        return bool(record and record["deleted"] > 0)
