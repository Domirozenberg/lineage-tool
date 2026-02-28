"""FastAPI dependencies shared across all v1 routers."""

from typing import Annotated, Generator

from fastapi import Depends, Query
from neo4j import Session

from app.db.neo4j import get_session


# ---------------------------------------------------------------------------
# Database session
# ---------------------------------------------------------------------------


def db_session() -> Generator[Session, None, None]:
    """Yield a Neo4j session for the duration of a request."""
    with get_session() as session:
        yield session


DbSession = Annotated[Session, Depends(db_session)]


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


class Pagination:
    def __init__(
        self,
        skip: int = Query(0, ge=0, description="Number of records to skip"),
        limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    ) -> None:
        self.skip = skip
        self.limit = limit


PaginationDep = Annotated[Pagination, Depends(Pagination)]
