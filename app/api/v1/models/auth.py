"""API request/response models for authentication."""

from datetime import datetime
from typing import Annotated, Optional
from uuid import UUID

import email_validator as _ev
from pydantic import BaseModel, BeforeValidator, Field

from app.db.repositories.user import UserRole


def _validate_email(v: object) -> str:
    """Normalize and validate an email.

    Uses  check_deliverability=False  so that private/enterprise domains
    (e.g. .local, .internal, .corp) are accepted without DNS lookups.
    """
    if not isinstance(v, str):
        raise ValueError("email must be a string")
    try:
        info = _ev.validate_email(v, check_deliverability=False)
        return info.normalized
    except _ev.EmailNotValidError as exc:
        raise ValueError(str(exc)) from exc


NormalizedEmail = Annotated[str, BeforeValidator(_validate_email)]


class RegisterRequest(BaseModel):
    email: NormalizedEmail
    password: str = Field(..., min_length=8)
    full_name: Optional[str] = None
    role: str = Field(default=UserRole.USER)

    model_config = {"json_schema_extra": {"example": {
        "email": "alice@example.com",
        "password": "s3cr3t!!",
        "full_name": "Alice Smith",
        "role": "user",
    }}}


class LoginRequest(BaseModel):
    email: NormalizedEmail
    password: str

    model_config = {"json_schema_extra": {"example": {
        "email": "admin@lineage-tool.dev",
        "password": "change-me-in-production",
    }}}


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


class UserResponse(BaseModel):
    id: UUID
    email: str
    full_name: Optional[str]
    role: str
    is_active: bool
    created_at: datetime


class ApiKeyResponse(BaseModel):
    api_key: str
    note: str = "Store this key securely — it will not be shown again."
