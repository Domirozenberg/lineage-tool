"""Security utilities: password hashing, JWT encoding/decoding, API keys.

Design decisions
----------------
- Passwords: bcrypt via passlib (slow by design, resistant to brute-force)
- Access tokens: short-lived JWT (default 30 min), signed with HS256
- Refresh tokens: longer-lived JWT (default 7 days), same secret key
  but carries  {"type": "refresh"}  so it cannot be used as an access token
- API keys: random 32-byte hex prefixed with "lng_" (format: lng_<64 hex chars>)
  Stored as a SHA-256 hex digest so the plaintext is never persisted.
  API keys are shown exactly once at generation time.

Offline folder validation
-------------------------
Connectors that use  AuthMode.OFFLINE  read metadata from a local folder
instead of connecting to a live system.  validate_offline_folder() checks
that a given directory exists and contains all expected files.
"""

import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt
from jose import JWTError, jwt

from app.core.config import settings

# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------
# Uses bcrypt directly to avoid passlib 1.7.4 / bcrypt 4.0+ incompatibility.

API_KEY_PREFIX = "lng_"


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


# ---------------------------------------------------------------------------
# JWT tokens
# ---------------------------------------------------------------------------


def create_access_token(
    subject: str,
    role: str,
    expires_delta: timedelta | None = None,
) -> str:
    """Return a signed JWT access token."""
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    payload: dict[str, Any] = {
        "sub": subject,
        "role": role,
        "type": "access",
        "exp": expire,
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")


def create_refresh_token(subject: str) -> str:
    """Return a signed JWT refresh token (7-day lifetime)."""
    expire = datetime.now(timezone.utc) + timedelta(
        days=settings.REFRESH_TOKEN_EXPIRE_DAYS
    )
    payload: dict[str, Any] = {
        "sub": subject,
        "type": "refresh",
        "exp": expire,
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")


def decode_token(token: str) -> dict[str, Any]:
    """Decode and verify a JWT.  Raises JWTError on any failure."""
    return jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])


# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------


def generate_api_key() -> str:
    """Generate a new plaintext API key.  Store the *hash*, show this once."""
    random_part = secrets.token_hex(32)
    return f"{API_KEY_PREFIX}{random_part}"


def hash_api_key(key: str) -> str:
    """Return the SHA-256 hex digest of an API key (stored in DB)."""
    return hashlib.sha256(key.encode()).hexdigest()


def verify_api_key(plain: str, stored_hash: str) -> bool:
    """Return True if the plaintext API key matches the stored hash."""
    return hash_api_key(plain) == stored_hash


# ---------------------------------------------------------------------------
# Offline folder validation (connector bootstrap)
# ---------------------------------------------------------------------------


def validate_offline_folder(
    folder_path: str, required_files: list[str]
) -> list[str]:
    """Validate that an offline import folder exists and contains required files.

    Used by connectors with  AuthMode.OFFLINE  to verify their input folder
    before starting an extraction run.

    Returns:
        A list of error strings.  Empty list means the folder is valid.
    """
    errors: list[str] = []
    if not os.path.isdir(folder_path):
        errors.append(f"Folder does not exist: {folder_path}")
        return errors
    for filename in required_files:
        full_path = os.path.join(folder_path, filename)
        if not os.path.isfile(full_path):
            errors.append(f"Required file missing: {filename}")
    return errors
