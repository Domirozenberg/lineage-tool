"""Custom exception classes and FastAPI exception handlers.

Register all handlers in app/main.py via app.add_exception_handler().
"""

from uuid import UUID

from fastapi import Request
from fastapi.responses import JSONResponse


# ---------------------------------------------------------------------------
# Exception classes
# ---------------------------------------------------------------------------


class NotFoundError(Exception):
    """Raised when an entity with the given ID does not exist."""

    def __init__(self, entity: str, entity_id: UUID) -> None:
        self.entity = entity
        self.entity_id = entity_id
        super().__init__(f"{entity} {entity_id} not found")


class ConflictError(Exception):
    """Raised when a create/update would violate a uniqueness constraint."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


class UnprocessableError(Exception):
    """Raised when business-logic validation fails (beyond Pydantic)."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


# ---------------------------------------------------------------------------
# FastAPI exception handlers
# ---------------------------------------------------------------------------


async def not_found_handler(request: Request, exc: NotFoundError) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={"detail": str(exc)},
    )


async def conflict_handler(request: Request, exc: ConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": exc.detail},
    )


async def unprocessable_handler(request: Request, exc: UnprocessableError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"detail": exc.detail},
    )


async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected internal error occurred."},
    )
