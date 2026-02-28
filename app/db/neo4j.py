"""Neo4j connection manager.

Provides a singleton driver and a context-manager session factory.
The driver is initialised lazily on first use and closed explicitly
at application shutdown (called from app lifespan).

Usage::

    from app.db.neo4j import get_session

    with get_session() as session:
        session.run("MATCH (n) RETURN count(n) AS total")
"""

from contextlib import contextmanager
from typing import Generator

from neo4j import Driver, GraphDatabase, Session

from app.core.config import settings

_driver: Driver | None = None


def get_driver() -> Driver:
    """Return (and lazily create) the module-level Neo4j driver."""
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        )
    return _driver


def close_driver() -> None:
    """Close the driver and release all connections. Call at shutdown."""
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a Neo4j session, closing it when the block exits."""
    driver = get_driver()
    session = driver.session()
    try:
        yield session
    finally:
        session.close()


def verify_connectivity() -> bool:
    """Return True if the Neo4j server is reachable, False otherwise."""
    try:
        get_driver().verify_connectivity()
        return True
    except Exception:
        return False
