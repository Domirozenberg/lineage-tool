"""Authentication endpoints — /api/v1/auth."""

from fastapi import APIRouter, HTTPException, status
from jose import JWTError

from app.api.v1.dependencies import AdminUser, CurrentUser, DbSession
from app.api.v1.models.auth import (
    ApiKeyResponse,
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    TokenResponse,
    UserResponse,
)
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    generate_api_key,
    hash_api_key,
    hash_password,
    verify_password,
)
from app.db.repositories.user import User, UserRepository, UserRole

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register(body: RegisterRequest, session: DbSession, _: AdminUser) -> UserResponse:
    """Create a new user.  Admin-only — except when no users exist yet."""
    repo = UserRepository(session)

    if repo.get_by_email(body.email.lower()):
        raise HTTPException(status_code=409, detail="Email already registered")

    if body.role not in UserRole.ALL:
        raise HTTPException(status_code=422, detail=f"Invalid role: {body.role}")

    user = User(
        email=body.email.lower(),
        full_name=body.full_name,
        hashed_password=hash_password(body.password),
        role=body.role,
    )
    repo.create(user)
    return UserResponse(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        role=user.role,
        is_active=user.is_active,
        created_at=user.created_at,
    )


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest, session: DbSession) -> TokenResponse:
    """Exchange email + password for an access token and a refresh token."""
    repo = UserRepository(session)
    user = repo.get_by_email(body.email.lower())

    if user is None or not verify_password(body.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is inactive")

    return TokenResponse(
        access_token=create_access_token(str(user.id), user.role),
        refresh_token=create_refresh_token(str(user.id)),
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh_token(body: RefreshRequest, session: DbSession) -> TokenResponse:
    """Exchange a refresh token for a new access + refresh token pair."""
    try:
        payload = decode_token(body.refresh_token)
        if payload.get("type") != "refresh":
            raise JWTError("not a refresh token")
        user_id = payload.get("sub")
        if not user_id:
            raise JWTError("missing sub")
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )

    import uuid
    user = UserRepository(session).get_by_id(uuid.UUID(user_id))
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")

    return TokenResponse(
        access_token=create_access_token(str(user.id), user.role),
        refresh_token=create_refresh_token(str(user.id)),
    )


@router.get("/me", response_model=UserResponse)
def get_me(current_user: CurrentUser) -> UserResponse:
    """Return the profile of the currently authenticated user."""
    return UserResponse(
        id=current_user.id,
        email=current_user.email,
        full_name=current_user.full_name,
        role=current_user.role,
        is_active=current_user.is_active,
        created_at=current_user.created_at,
    )


@router.post("/api-key", response_model=ApiKeyResponse)
def create_api_key(session: DbSession, admin: AdminUser) -> ApiKeyResponse:
    """Generate a new API key for the calling admin user (shown once)."""
    plain_key = generate_api_key()
    admin.api_key_hash = hash_api_key(plain_key)
    UserRepository(session).update(admin)
    return ApiKeyResponse(api_key=plain_key)
