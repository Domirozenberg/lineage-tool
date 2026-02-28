"""Neo4j connection manager.

Provides a singleton driver (with configurable connection pool) and a
context-manager session factory.  The driver is initialised lazily on
first use and closed explicitly at application shutdown via close_driver(),
which is called from the FastAPI lifespan handler in app/main.py.

Pool settings are read from app.core.config.Settings and can be overridden
via environment variables or a .env file:

  NEO4J_MAX_CONNECTION_POOL_SIZE          (default 50)
  NEO4J_MAX_CONNECTION_LIFETIME_S         (default 3600)
  NEO4J_CONNECTION_ACQUISITION_TIMEOUT_S  (default 60)
  NEO4J_CONNECTION_TIMEOUT_S              (default 30)

Usage::

    from app.db.neo4j import get_session

    with get_session() as session:
        result = session.run("MATCH (n) RETURN count(n) AS total")
        print(result.single()["total"])
"""

from contextlib import contextmanager
from typing import Any, Generator

from neo4j import Driver, GraphDatabase, Session

from app.core.config import settings

_driver: Driver | None = None


def get_driver() -> Driver:
    """Return (and lazily create) the module-level Neo4j driver.

    The driver maintains an internal connection pool whose size and
    lifetime are controlled by the NEO4J_* settings.
    """
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
            max_connection_pool_size=settings.NEO4J_MAX_CONNECTION_POOL_SIZE,
            max_connection_lifetime=settings.NEO4J_MAX_CONNECTION_LIFETIME_S,
            connection_acquisition_timeout=settings.NEO4J_CONNECTION_ACQUISITION_TIMEOUT_S,
            connection_timeout=settings.NEO4J_CONNECTION_TIMEOUT_S,
        )
    return _driver


def close_driver() -> None:
    """Close the driver and release all pooled connections. Call at shutdown."""
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Yield a Neo4j session, returning it to the pool when the block exits."""
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


def get_db_status() -> dict[str, Any]:
    """Return a status dict suitable for embedding in the health endpoint.

    Keys:
      connected  (bool)   — whether a Bolt connection could be opened
      uri        (str)    — configured Bolt URI
      pool_size  (int)    — configured max pool size
      error      (str)    — error message when connected is False
    """
    status: dict[str, Any] = {
        "connected": False,
        "uri": settings.NEO4J_URI,
        "pool_size": settings.NEO4J_MAX_CONNECTION_POOL_SIZE,
        "error": None,
    }
    try:
        get_driver().verify_connectivity()
        status["connected"] = True
    except Exception as exc:
        status["error"] = str(exc)
    return status
