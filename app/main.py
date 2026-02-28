from contextlib import asynccontextmanager
from typing import Any

import redis as redis_lib
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.routers import columns, connectors, lineage, objects, sources
from app.api.v1.routers import auth as auth_router
from app.core.config import settings
from app.core.errors import (
    ConflictError,
    NotFoundError,
    UnprocessableError,
    conflict_handler,
    generic_error_handler,
    not_found_handler,
    unprocessable_handler,
)
from app.core.security import hash_password
from app.db.constraints import apply_constraints_and_indexes
from app.db.neo4j import close_driver, get_db_status, get_session
from app.db.repositories.user import User, UserRepository, UserRole


def _seed_first_admin() -> None:
    """Create the bootstrap admin user if no users exist in the database."""
    with get_session() as session:
        repo = UserRepository(session)
        if repo.count() == 0:
            admin = User(
                email=settings.FIRST_ADMIN_EMAIL.lower(),
                hashed_password=hash_password(settings.FIRST_ADMIN_PASSWORD),
                role=UserRole.ADMIN,
                full_name="Bootstrap Admin",
            )
            repo.create(admin)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with get_session() as session:
        apply_constraints_and_indexes(session)
    _seed_first_admin()
    yield
    close_driver()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Exception handlers ---
app.add_exception_handler(NotFoundError, not_found_handler)  # type: ignore[arg-type]
app.add_exception_handler(ConflictError, conflict_handler)  # type: ignore[arg-type]
app.add_exception_handler(UnprocessableError, unprocessable_handler)  # type: ignore[arg-type]
app.add_exception_handler(Exception, generic_error_handler)  # type: ignore[arg-type]

# --- Routers ---
app.include_router(auth_router.router, prefix=settings.API_V1_STR)
app.include_router(sources.router, prefix=settings.API_V1_STR)
app.include_router(objects.router, prefix=settings.API_V1_STR)
app.include_router(columns.router, prefix=settings.API_V1_STR)
app.include_router(lineage.router, prefix=settings.API_V1_STR)
app.include_router(connectors.router, prefix=settings.API_V1_STR)


# --- Health ---
def _redis_status() -> dict[str, Any]:
    status: dict[str, Any] = {"connected": False, "url": settings.REDIS_URL, "error": None}
    try:
        r = redis_lib.from_url(settings.REDIS_URL, socket_connect_timeout=2)
        r.ping()
        status["connected"] = True
    except Exception as exc:
        status["error"] = str(exc)
    return status


@app.get("/health", tags=["health"])
async def health_check() -> dict:
    neo4j = get_db_status()
    redis = _redis_status()
    all_healthy = neo4j["connected"] and redis["connected"]
    return {
        "status": "ok" if all_healthy else "degraded",
        "version": settings.VERSION,
        "services": {
            "neo4j": neo4j,
            "redis": redis,
        },
    }
