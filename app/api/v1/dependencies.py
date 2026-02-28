"""FastAPI dependencies shared across all v1 routers."""

from typing import Annotated, Generator, Optional

from fastapi import Depends, HTTPException, Query, Security, status
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from neo4j import Session

from app.core.security import decode_token, hash_api_key, verify_api_key
from app.db.neo4j import get_session
from app.db.repositories.user import User, UserRepository, UserRole

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


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_current_user(
    session: DbSession,
    bearer_creds: Annotated[
        Optional[HTTPAuthorizationCredentials], Security(_bearer)
    ] = None,
    api_key_header: Annotated[Optional[str], Security(_api_key_header)] = None,
) -> User:
    """Resolve the caller as a User via JWT bearer token or X-API-Key header.

    Raises 401 when no valid credential is supplied.
    """
    repo = UserRepository(session)

    # --- JWT bearer ---
    if bearer_creds is not None:
        token = bearer_creds.credentials
        # Also accept API keys via Bearer (lng_...)
        from app.core.security import API_KEY_PREFIX
        if token.startswith(API_KEY_PREFIX):
            user = repo.get_by_api_key_hash(hash_api_key(token))
            if user and user.is_active:
                return user
        else:
            try:
                payload = decode_token(token)
                if payload.get("type") != "access":
                    raise JWTError("not an access token")
                user_id = payload.get("sub")
                if user_id is None:
                    raise JWTError("missing sub")
                user = repo.get_by_id(__import__("uuid").UUID(user_id))
                if user and user.is_active:
                    return user
            except JWTError:
                pass

    # --- X-API-Key header ---
    if api_key_header is not None:
        user = repo.get_by_api_key_hash(hash_api_key(api_key_header))
        if user and user.is_active:
            return user

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )


CurrentUser = Annotated[User, Depends(get_current_user)]


def require_admin(current_user: CurrentUser) -> User:
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user


def require_writer(current_user: CurrentUser) -> User:
    """Allow admin and user roles; block service accounts (read-only)."""
    if current_user.role == UserRole.SERVICE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Service accounts have read-only access",
        )
    return current_user


AdminUser = Annotated[User, Depends(require_admin)]
WriterUser = Annotated[User, Depends(require_writer)]
