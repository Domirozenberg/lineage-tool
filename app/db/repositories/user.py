"""Repository for User nodes."""

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from app.db.base_repository import BaseRepository


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class UserRole:
    ADMIN = "admin"
    USER = "user"
    SERVICE = "service"

    ALL = {ADMIN, USER, SERVICE}


class User(BaseModel):
    """Domain model for an authenticated user."""

    id: UUID = Field(default_factory=uuid4)
    email: str
    full_name: Optional[str] = None
    hashed_password: str
    role: str = UserRole.USER
    api_key_hash: Optional[str] = None
    is_active: bool = True
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)

    model_config = {"from_attributes": True}


class UserRepository(BaseRepository):

    def create(self, entity: User) -> User:
        props = self._to_neo4j(entity)
        self._session.run(
            "CREATE (n:User {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def get_by_id(self, entity_id: UUID) -> User | None:
        result = self._session.run(
            "MATCH (n:User {id: $id}) RETURN properties(n) AS props",
            id=str(entity_id),
        )
        record = result.single()
        if record is None:
            return None
        return User.model_validate(self._from_record(dict(record["props"])))

    def get_by_email(self, email: str) -> User | None:
        result = self._session.run(
            "MATCH (n:User {email: $email}) RETURN properties(n) AS props",
            email=email.lower(),
        )
        record = result.single()
        if record is None:
            return None
        return User.model_validate(self._from_record(dict(record["props"])))

    def get_by_api_key_hash(self, api_key_hash: str) -> User | None:
        result = self._session.run(
            "MATCH (n:User {api_key_hash: $hash}) RETURN properties(n) AS props",
            hash=api_key_hash,
        )
        record = result.single()
        if record is None:
            return None
        return User.model_validate(self._from_record(dict(record["props"])))

    def list_all(self) -> list[User]:
        result = self._session.run(
            "MATCH (n:User) RETURN properties(n) AS props ORDER BY n.email"
        )
        return [User.model_validate(self._from_record(dict(r["props"]))) for r in result]

    def update(self, entity: User) -> User:
        props = self._to_neo4j(entity)
        self._session.run(
            "MATCH (n:User {id: $id}) SET n += $props",
            id=props["id"],
            props=props,
        )
        return entity

    def delete(self, entity_id: UUID) -> bool:
        result = self._session.run(
            "MATCH (n:User {id: $id}) DELETE n RETURN count(n) AS deleted",
            id=str(entity_id),
        )
        record = result.single()
        return bool(record and record["deleted"] > 0)

    def count(self) -> int:
        result = self._session.run("MATCH (n:User) RETURN count(n) AS cnt")
        record = result.single()
        return int(record["cnt"]) if record else 0
