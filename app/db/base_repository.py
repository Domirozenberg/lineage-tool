"""Abstract base repository.

All entity repositories inherit from BaseRepository, which provides:
  - A reference to the active Neo4j session
  - A shared helper for serialising UUIDs and datetimes to Neo4j-safe types
  - Type-safe signatures that subclasses must implement
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from uuid import UUID

from neo4j import Session


class BaseRepository(ABC):
    """Common persistence operations over a Neo4j session."""

    def __init__(self, session: Session) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def create(self, entity: Any) -> Any:
        """Persist a new entity and return it."""

    @abstractmethod
    def get_by_id(self, entity_id: UUID) -> Any | None:
        """Fetch a single entity by its UUID, or None if not found."""

    @abstractmethod
    def list_all(self) -> list[Any]:
        """Return all entities of this type."""

    @abstractmethod
    def update(self, entity: Any) -> Any:
        """Overwrite an existing entity's properties and return it."""

    @abstractmethod
    def delete(self, entity_id: UUID) -> bool:
        """Delete an entity by UUID. Return True if it existed."""

    # ------------------------------------------------------------------
    # Shared serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_neo4j(entity: Any) -> dict[str, Any]:
        """Convert a Pydantic model to a flat Neo4j-safe property dict.

        - UUIDs      → str
        - datetimes  → ISO-8601 str
        - dicts      → JSON str  (Neo4j doesn't store nested maps natively)
        - enums      → their .value str
        """
        raw = entity.model_dump()
        return BaseRepository._flatten(raw)

    @staticmethod
    def _flatten(d: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for k, v in d.items():
            if isinstance(v, UUID):
                result[k] = str(v)
            elif isinstance(v, datetime):
                result[k] = v.isoformat()
            elif isinstance(v, dict):
                result[k] = json.dumps(v)
            elif isinstance(v, list):
                # Encode list as JSON string (used for column_mappings)
                result[k] = json.dumps(
                    [BaseRepository._flatten(i) if isinstance(i, dict) else str(i) for i in v]
                )
            else:
                result[k] = v
        return result

    @staticmethod
    def _from_record(record: dict[str, Any]) -> dict[str, Any]:
        """Reverse Neo4j property types back to Python-native values.

        JSON strings are decoded back to dicts/lists; everything else
        is left as-is for Pydantic to validate.
        """
        result: dict[str, Any] = {}
        for k, v in record.items():
            if isinstance(v, str):
                # Attempt JSON decode for known complex fields
                if k in ("extra_metadata", "column_mappings"):
                    try:
                        result[k] = json.loads(v)
                    except (json.JSONDecodeError, ValueError):
                        result[k] = v
                else:
                    result[k] = v
            else:
                result[k] = v
        return result
