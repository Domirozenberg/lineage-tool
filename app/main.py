from contextlib import asynccontextmanager
from typing import Any

import redis as redis_lib
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.constraints import apply_constraints_and_indexes
from app.db.neo4j import close_driver, get_db_status, get_session


@asynccontextmanager
async def lifespan(app: FastAPI):
    with get_session() as session:
        apply_constraints_and_indexes(session)
    yield
    close_driver()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
