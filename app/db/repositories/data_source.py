"""Repository for DataSource nodes."""

from uuid import UUID

from app.db.base_repository import BaseRepository
from app.models.schema import DataSource, Platform


class DataSourceRepository(BaseRepository):

    def create(self, entity: DataSource) -> DataSource:
        props = self._to_neo4j(entity)
        self._session.run(
            "MERGE (n:DataSource {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def get_by_id(self, entity_id: UUID) -> DataSource | None:
        result = self._session.run(
            "MATCH (n:DataSource {id: $id}) RETURN properties(n) AS props",
            id=str(entity_id),
        )
        record = result.single()
        if record is None:
            return None
        return DataSource.model_validate(self._from_record(dict(record["props"])))

    def list_all(self) -> list[DataSource]:
        result = self._session.run(
            "MATCH (n:DataSource) RETURN properties(n) AS props ORDER BY n.name"
        )
        return [
            DataSource.model_validate(self._from_record(dict(r["props"]))) for r in result
        ]

    def list_by_platform(self, platform: Platform) -> list[DataSource]:
        result = self._session.run(
            "MATCH (n:DataSource {platform: $platform}) "
            "RETURN properties(n) AS props ORDER BY n.name",
            platform=platform.value,
        )
        return [
            DataSource.model_validate(self._from_record(dict(r["props"]))) for r in result
        ]

    def update(self, entity: DataSource) -> DataSource:
        props = self._to_neo4j(entity)
        self._session.run(
            "MATCH (n:DataSource {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def delete(self, entity_id: UUID) -> bool:
        result = self._session.run(
            "MATCH (n:DataSource {id: $id}) "
            "DETACH DELETE n "
            "RETURN count(n) AS deleted",
            id=str(entity_id),
        )
        record = result.single()
        return bool(record and record["deleted"] > 0)
