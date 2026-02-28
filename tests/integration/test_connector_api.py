"""Integration tests for the connector API endpoints.

Requires:
  - The FastAPI app (mocked or live Neo4j) to handle requests
  - lineage-postgres on localhost:5433 for live extract tests

Endpoints under test:
  POST /api/v1/connectors/postgresql/test-connection
  POST /api/v1/connectors/postgresql/extract
  GET  /api/v1/connectors/postgresql/status
  POST /api/v1/connectors/offline/validate
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from app.core.config import settings

# ---------------------------------------------------------------------------
# Fixtures — reuse from test_api.py pattern
# ---------------------------------------------------------------------------

try:
    import psycopg2
    _HAS_PSYCOPG2 = True
except ImportError:
    _HAS_PSYCOPG2 = False


def _pg_available() -> bool:
    if not _HAS_PSYCOPG2:
        return False
    try:
        conn = psycopg2.connect(
            host="localhost", port=5433, dbname="lineage_sample",
            user="lineage", password="lineage", connect_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def client():
    from app.main import app

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture(scope="module")
def auth_headers(client):
    """Return Bearer token headers for the bootstrap admin user."""
    resp = client.post(
        f"{settings.API_V1_STR}/auth/login",
        json={
            "email": settings.FIRST_ADMIN_EMAIL,
            "password": settings.FIRST_ADMIN_PASSWORD,
        },
    )
    if resp.status_code != 200:
        pytest.skip(f"Cannot authenticate (status {resp.status_code}): {resp.text}")
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------


class TestConnectorStatus:
    def test_status_requires_auth(self, client):
        resp = client.get(f"{settings.API_V1_STR}/connectors/postgresql/status")
        assert resp.status_code == 401

    def test_status_returns_available(self, client, auth_headers):
        resp = client.get(
            f"{settings.API_V1_STR}/connectors/postgresql/status",
            headers=auth_headers,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "available"
        assert "username_password" in body["auth_modes"]


# ---------------------------------------------------------------------------
# Test-connection endpoint
# ---------------------------------------------------------------------------


class TestPostgresTestConnection:
    def test_requires_auth(self, client):
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/postgresql/test-connection",
            json={"host": "localhost", "port": 5433, "dbname": "lineage_sample",
                  "user": "lineage", "password": "lineage"},
        )
        assert resp.status_code == 401

    def test_good_credentials_returns_connected(self, client, auth_headers):
        if not _pg_available():
            pytest.skip("lineage-postgres not reachable")
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/postgresql/test-connection",
            headers=auth_headers,
            json={"host": "localhost", "port": 5433, "dbname": "lineage_sample",
                  "user": "lineage", "password": "lineage"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["connected"] is True
        assert "PostgreSQL" in (body.get("version") or "")
        assert isinstance(body["schemas"], list)

    def test_bad_credentials_returns_not_connected(self, client, auth_headers):
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/postgresql/test-connection",
            headers=auth_headers,
            json={"host": "localhost", "port": 5433, "dbname": "lineage_sample",
                  "user": "lineage", "password": "definitely_wrong"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["connected"] is False


# ---------------------------------------------------------------------------
# Extract endpoint
# ---------------------------------------------------------------------------


class TestPostgresExtract:
    def test_requires_auth(self, client):
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/postgresql/extract",
            json={"source_name": "test", "host": "localhost", "port": 5433,
                  "dbname": "lineage_sample", "user": "lineage", "password": "lineage"},
        )
        assert resp.status_code == 401

    def test_extract_returns_summary(self, client, auth_headers):
        if not _pg_available():
            pytest.skip("lineage-postgres not reachable")
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/postgresql/extract",
            headers=auth_headers,
            json={
                "source_name": "test_extract",
                "host": "localhost",
                "port": 5433,
                "dbname": "lineage_sample",
                "user": "lineage",
                "password": "lineage",
                "schemas": ["raw"],
                "include_column_lineage": False,
            },
            timeout=60,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "source_id" in body
        assert body["objects"] >= 20
        assert body["columns"] >= 100
        assert body["duration_seconds"] >= 0

    def test_bad_connection_returns_400(self, client, auth_headers):
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/postgresql/extract",
            headers=auth_headers,
            json={"source_name": "bad", "host": "localhost", "port": 9999,
                  "dbname": "nonexistent", "user": "nobody", "password": "wrong"},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Offline validate endpoint
# ---------------------------------------------------------------------------


class TestOfflineValidate:
    def test_requires_auth(self, client):
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/offline/validate",
            json={"folder_path": "/tmp"},
        )
        assert resp.status_code == 401

    def test_valid_folder_with_all_files(self, client, auth_headers, tmp_path):
        required_files = [
            "tables.json",
            "columns.json",
            "foreign_keys.json",
            "view_definitions.json",
            "functions.json",
        ]
        for fname in required_files:
            (tmp_path / fname).write_text("{}")

        resp = client.post(
            f"{settings.API_V1_STR}/connectors/offline/validate",
            headers=auth_headers,
            json={"folder_path": str(tmp_path)},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True
        assert body["errors"] == []

    def test_missing_files_returns_errors(self, client, auth_headers, tmp_path):
        # Only create one file — the rest are missing
        (tmp_path / "tables.json").write_text("{}")

        resp = client.post(
            f"{settings.API_V1_STR}/connectors/offline/validate",
            headers=auth_headers,
            json={"folder_path": str(tmp_path)},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert len(body["errors"]) >= 4

    def test_nonexistent_folder_is_invalid(self, client, auth_headers):
        resp = client.post(
            f"{settings.API_V1_STR}/connectors/offline/validate",
            headers=auth_headers,
            json={"folder_path": "/nonexistent/folder_xyz_12345"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert len(body["errors"]) > 0
