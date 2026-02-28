"""Integration tests for Task 1.6 — Authentication & Authorization.

Covers:
- Login with correct / wrong credentials
- JWT access + refresh token flow
- GET /auth/me with valid token
- Protected endpoints require authentication (401)
- Admin-only endpoints enforce RBAC (403 for non-admin)
- API key generation and usage
- Service accounts have read-only access (403 on write)
- Offline folder validation (unit-level in test_security.py; API bootstrapped here)

Requires Neo4j to be running (docker compose up -d).
"""

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import settings
from app.db.neo4j import verify_connectivity
from app.main import app

pytestmark = pytest.mark.skipif(
    not verify_connectivity(),
    reason="Neo4j is not reachable — start docker compose up -d",
)

BASE = "/api/v1"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture()
async def admin_client(client: AsyncClient):
    resp = await client.post(
        f"{BASE}/auth/login",
        json={
            "email": settings.FIRST_ADMIN_EMAIL,
            "password": settings.FIRST_ADMIN_PASSWORD,
        },
    )
    assert resp.status_code == 200, f"Admin login failed: {resp.text}"
    token = resp.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"
    return client


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------


class TestLogin:
    async def test_login_correct_credentials(self, client: AsyncClient):
        resp = await client.post(
            f"{BASE}/auth/login",
            json={
                "email": settings.FIRST_ADMIN_EMAIL,
                "password": settings.FIRST_ADMIN_PASSWORD,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "access_token" in body
        assert "refresh_token" in body
        assert body["token_type"] == "bearer"

    async def test_login_wrong_password(self, client: AsyncClient):
        resp = await client.post(
            f"{BASE}/auth/login",
            json={"email": settings.FIRST_ADMIN_EMAIL, "password": "wrong!"},
        )
        assert resp.status_code == 401

    async def test_login_unknown_email(self, client: AsyncClient):
        resp = await client.post(
            f"{BASE}/auth/login",
            json={"email": "nobody@example.com", "password": "whatever"},
        )
        assert resp.status_code == 401

    async def test_login_missing_fields(self, client: AsyncClient):
        resp = await client.post(f"{BASE}/auth/login", json={"email": "x@x.com"})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------


class TestTokenRefresh:
    async def test_refresh_returns_new_tokens(self, client: AsyncClient):
        login = await client.post(
            f"{BASE}/auth/login",
            json={
                "email": settings.FIRST_ADMIN_EMAIL,
                "password": settings.FIRST_ADMIN_PASSWORD,
            },
        )
        refresh_token = login.json()["refresh_token"]

        resp = await client.post(
            f"{BASE}/auth/refresh", json={"refresh_token": refresh_token}
        )
        assert resp.status_code == 200
        body = resp.json()
        # Token is present and is a valid JWT (decodable)
        assert "access_token" in body
        assert "refresh_token" in body
        from app.core.security import decode_token
        payload = decode_token(body["access_token"])
        assert payload["type"] == "access"

    async def test_refresh_with_access_token_fails(self, client: AsyncClient):
        login = await client.post(
            f"{BASE}/auth/login",
            json={
                "email": settings.FIRST_ADMIN_EMAIL,
                "password": settings.FIRST_ADMIN_PASSWORD,
            },
        )
        access_token = login.json()["access_token"]
        resp = await client.post(
            f"{BASE}/auth/refresh", json={"refresh_token": access_token}
        )
        assert resp.status_code == 401

    async def test_refresh_with_garbage_fails(self, client: AsyncClient):
        resp = await client.post(
            f"{BASE}/auth/refresh", json={"refresh_token": "not.a.token"}
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# /auth/me
# ---------------------------------------------------------------------------


class TestGetMe:
    async def test_me_returns_profile(self, admin_client: AsyncClient):
        resp = await admin_client.get(f"{BASE}/auth/me")
        assert resp.status_code == 200
        body = resp.json()
        assert body["email"] == settings.FIRST_ADMIN_EMAIL.lower()
        assert body["role"] == "admin"
        assert "id" in body

    async def test_me_requires_auth(self, client: AsyncClient):
        resp = await client.get(f"{BASE}/auth/me")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Protected routes enforce authentication
# ---------------------------------------------------------------------------


class TestProtectedRoutes:
    async def test_sources_list_without_token_returns_401(self, client: AsyncClient):
        resp = await client.get(f"{BASE}/sources/")
        assert resp.status_code == 401

    async def test_objects_list_without_token_returns_401(self, client: AsyncClient):
        resp = await client.get(f"{BASE}/objects/")
        assert resp.status_code == 401

    async def test_invalid_token_returns_401(self, client: AsyncClient):
        client.headers["Authorization"] = "Bearer this.is.invalid"
        resp = await client.get(f"{BASE}/sources/")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# RBAC — non-admin cannot register users
# ---------------------------------------------------------------------------


class TestRbac:
    async def _create_regular_user(self, admin_client: AsyncClient) -> dict:
        import uuid
        email = f"user_{uuid.uuid4().hex[:8]}@test.example"
        resp = await admin_client.post(
            f"{BASE}/auth/register",
            json={"email": email, "password": "test1234!", "role": "user"},
        )
        assert resp.status_code == 201, resp.text
        return {"email": email, "password": "test1234!"}

    async def test_admin_can_register_user(self, admin_client: AsyncClient):
        import uuid
        email = f"newuser_{uuid.uuid4().hex[:8]}@test.example"
        resp = await admin_client.post(
            f"{BASE}/auth/register",
            json={"email": email, "password": "secure123!", "role": "user"},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["role"] == "user"
        assert body["email"] == email.lower()

    async def test_regular_user_cannot_register(self, client: AsyncClient, admin_client: AsyncClient):
        creds = await self._create_regular_user(admin_client)
        login = await client.post(f"{BASE}/auth/login", json=creds)
        user_token = login.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {user_token}"

        import uuid
        resp = await client.post(
            f"{BASE}/auth/register",
            json={"email": f"another_{uuid.uuid4().hex[:8]}@test.example", "password": "test1234!", "role": "user"},
        )
        assert resp.status_code == 403

    async def test_duplicate_email_returns_409(self, admin_client: AsyncClient):
        import uuid
        email = f"dup_{uuid.uuid4().hex[:8]}@test.example"
        await admin_client.post(
            f"{BASE}/auth/register",
            json={"email": email, "password": "test1234!"},
        )
        resp = await admin_client.post(
            f"{BASE}/auth/register",
            json={"email": email, "password": "test1234!"},
        )
        assert resp.status_code == 409

    async def test_invalid_role_returns_422(self, admin_client: AsyncClient):
        import uuid
        resp = await admin_client.post(
            f"{BASE}/auth/register",
            json={
                "email": f"badrole_{uuid.uuid4().hex[:8]}@test.example",
                "password": "test1234!",
                "role": "superuser",
            },
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# API key authentication
# ---------------------------------------------------------------------------


class TestApiKeyAuth:
    async def test_admin_can_generate_api_key(self, admin_client: AsyncClient):
        resp = await admin_client.post(f"{BASE}/auth/api-key")
        assert resp.status_code == 200
        body = resp.json()
        assert body["api_key"].startswith("lng_")
        assert "note" in body

    async def test_api_key_can_authenticate(self, admin_client: AsyncClient, client: AsyncClient):
        key_resp = await admin_client.post(f"{BASE}/auth/api-key")
        api_key = key_resp.json()["api_key"]

        client.headers.pop("Authorization", None)
        client.headers["X-API-Key"] = api_key
        resp = await client.get(f"{BASE}/auth/me")
        assert resp.status_code == 200

    async def test_api_key_via_bearer(self, admin_client: AsyncClient, client: AsyncClient):
        key_resp = await admin_client.post(f"{BASE}/auth/api-key")
        api_key = key_resp.json()["api_key"]

        client.headers.pop("X-API-Key", None)
        client.headers["Authorization"] = f"Bearer {api_key}"
        resp = await client.get(f"{BASE}/auth/me")
        assert resp.status_code == 200

    async def test_wrong_api_key_returns_401(self, client: AsyncClient):
        client.headers["X-API-Key"] = "lng_wrongkey12345"
        resp = await client.get(f"{BASE}/sources/")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Service account — read-only
# ---------------------------------------------------------------------------


class TestServiceAccount:
    async def _create_service_user(self, admin_client: AsyncClient, client: AsyncClient) -> AsyncClient:
        import uuid
        email = f"svc_{uuid.uuid4().hex[:8]}@test.example"
        await admin_client.post(
            f"{BASE}/auth/register",
            json={"email": email, "password": "svc1234!", "role": "service"},
        )
        login = await client.post(
            f"{BASE}/auth/login",
            json={"email": email, "password": "svc1234!"},
        )
        new_client_headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
        return new_client_headers

    async def test_service_can_read(self, client: AsyncClient, admin_client: AsyncClient):
        headers = await self._create_service_user(admin_client, client)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as svc:
            svc.headers.update(headers)
            resp = await svc.get(f"{BASE}/sources/")
            assert resp.status_code == 200

    async def test_service_cannot_write(self, client: AsyncClient, admin_client: AsyncClient):
        headers = await self._create_service_user(admin_client, client)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as svc:
            svc.headers.update(headers)
            resp = await svc.post(
                f"{BASE}/sources/",
                json={"name": "svc_write_attempt", "platform": "postgresql"},
            )
            assert resp.status_code == 403
