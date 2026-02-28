"""Shared fixtures and setup for all integration tests.

Ensures the bootstrap admin user exists before any test runs, even when
the test client's ASGI lifespan hasn't fired yet.
"""

import pytest

from app.core.config import settings
from app.core.security import hash_password
from app.db.constraints import apply_constraints_and_indexes
from app.db.neo4j import get_session, verify_connectivity
from app.db.repositories.user import User, UserRepository, UserRole


@pytest.fixture(scope="session", autouse=True)
def ensure_admin_user():
    """Create constraints and seed the bootstrap admin once per test session."""
    if not verify_connectivity():
        return  # Integration tests will be skipped anyway

    with get_session() as session:
        apply_constraints_and_indexes(session)
        repo = UserRepository(session)
        existing = repo.get_by_email(settings.FIRST_ADMIN_EMAIL.lower())
        if existing is None:
            admin = User(
                email=settings.FIRST_ADMIN_EMAIL.lower(),
                hashed_password=hash_password(settings.FIRST_ADMIN_PASSWORD),
                role=UserRole.ADMIN,
                full_name="Bootstrap Admin",
            )
            repo.create(admin)
