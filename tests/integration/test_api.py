"""Integration tests for Task 1.5 — REST API endpoints.

Uses httpx.AsyncClient with ASGITransport to exercise the full FastAPI
application (routing, validation, error handlers, DB) without starting
a real HTTP server.

Requires Neo4j to be running (docker compose up -d).
Skipped automatically when Neo4j is unreachable.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from app.db.neo4j import verify_connectivity
from app.main import app

pytestmark = pytest.mark.skipif(
    not verify_connectivity(),
    reason="Neo4j is not reachable — start docker compose up -d",
)

BASE = "/api/v1"
_PREFIX = "api_test_"


@pytest.fixture()
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestHealth:
    async def test_health_ok(self, client: AsyncClient):
        resp = await client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["services"]["neo4j"]["connected"] is True

    async def test_openapi_docs_available(self, client: AsyncClient):
        resp = await client.get(f"{BASE}/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert "paths" in schema
        assert "openapi" in schema


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


class TestSourcesEndpoints:
    async def test_create_source(self, client: AsyncClient):
        resp = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}pg", "platform": "postgresql", "host": "localhost"},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["name"] == f"{_PREFIX}pg"
        assert body["platform"] == "postgresql"
        assert "id" in body
        assert "created_at" in body

    async def test_get_source(self, client: AsyncClient):
        create = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}get_src", "platform": "mysql"},
        )
        src_id = create.json()["id"]

        resp = await client.get(f"{BASE}/sources/{src_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == src_id

    async def test_get_source_not_found(self, client: AsyncClient):
        from uuid import uuid4
        resp = await client.get(f"{BASE}/sources/{uuid4()}")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]

    async def test_list_sources(self, client: AsyncClient):
        await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}list_a", "platform": "postgresql"},
        )
        resp = await client.get(f"{BASE}/sources/")
        assert resp.status_code == 200
        body = resp.json()
        assert "items" in body
        assert "count" in body
        assert body["count"] >= 1

    async def test_list_sources_filter_platform(self, client: AsyncClient):
        await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}plat_tb", "platform": "tableau"},
        )
        resp = await client.get(f"{BASE}/sources/?platform=tableau")
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            assert item["platform"] == "tableau"

    async def test_update_source(self, client: AsyncClient):
        create = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}upd_src", "platform": "postgresql"},
        )
        src_id = create.json()["id"]

        resp = await client.put(
            f"{BASE}/sources/{src_id}",
            json={"description": "updated"},
        )
        assert resp.status_code == 200
        assert resp.json()["description"] == "updated"

    async def test_update_source_not_found(self, client: AsyncClient):
        from uuid import uuid4
        resp = await client.put(
            f"{BASE}/sources/{uuid4()}",
            json={"description": "x"},
        )
        assert resp.status_code == 404

    async def test_delete_source(self, client: AsyncClient):
        create = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}del_src", "platform": "postgresql"},
        )
        src_id = create.json()["id"]

        resp = await client.delete(f"{BASE}/sources/{src_id}")
        assert resp.status_code == 204

        resp = await client.get(f"{BASE}/sources/{src_id}")
        assert resp.status_code == 404

    async def test_delete_source_not_found(self, client: AsyncClient):
        from uuid import uuid4
        resp = await client.delete(f"{BASE}/sources/{uuid4()}")
        assert resp.status_code == 404

    async def test_create_source_invalid_platform(self, client: AsyncClient):
        resp = await client.post(
            f"{BASE}/sources/",
            json={"name": "x", "platform": "not_a_platform"},
        )
        assert resp.status_code == 422

    async def test_create_source_missing_name(self, client: AsyncClient):
        resp = await client.post(f"{BASE}/sources/", json={"platform": "postgresql"})
        assert resp.status_code == 422

    async def test_pagination(self, client: AsyncClient):
        resp = await client.get(f"{BASE}/sources/?skip=0&limit=2")
        assert resp.status_code == 200
        assert len(resp.json()["items"]) <= 2


# ---------------------------------------------------------------------------
# Objects
# ---------------------------------------------------------------------------


class TestObjectsEndpoints:
    async def _make_source(self, client: AsyncClient) -> str:
        r = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}obj_src", "platform": "postgresql"},
        )
        return r.json()["id"]

    async def test_create_object(self, client: AsyncClient):
        src_id = await self._make_source(client)
        resp = await client.post(
            f"{BASE}/objects/",
            json={
                "source_id": src_id,
                "object_type": "table",
                "name": f"{_PREFIX}orders",
                "schema_name": "public",
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["qualified_name"] == f"public.{_PREFIX}orders"
        assert "id" in body

    async def test_get_object_not_found(self, client: AsyncClient):
        from uuid import uuid4
        resp = await client.get(f"{BASE}/objects/{uuid4()}")
        assert resp.status_code == 404

    async def test_list_objects_by_source(self, client: AsyncClient):
        src_id = await self._make_source(client)
        await client.post(
            f"{BASE}/objects/",
            json={"source_id": src_id, "object_type": "table", "name": f"{_PREFIX}t1"},
        )
        resp = await client.get(f"{BASE}/objects/?source_id={src_id}")
        assert resp.status_code == 200
        assert resp.json()["count"] >= 1

    async def test_list_objects_by_type(self, client: AsyncClient):
        src_id = await self._make_source(client)
        await client.post(
            f"{BASE}/objects/",
            json={"source_id": src_id, "object_type": "dashboard", "name": f"{_PREFIX}dash"},
        )
        resp = await client.get(f"{BASE}/objects/?object_type=dashboard")
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            assert item["object_type"] == "dashboard"

    async def test_delete_object(self, client: AsyncClient):
        src_id = await self._make_source(client)
        create = await client.post(
            f"{BASE}/objects/",
            json={"source_id": src_id, "object_type": "table", "name": f"{_PREFIX}del_obj"},
        )
        obj_id = create.json()["id"]
        assert (await client.delete(f"{BASE}/objects/{obj_id}")).status_code == 204
        assert (await client.get(f"{BASE}/objects/{obj_id}")).status_code == 404


# ---------------------------------------------------------------------------
# Columns
# ---------------------------------------------------------------------------


class TestColumnsEndpoints:
    async def _make_object(self, client: AsyncClient) -> str:
        src = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}col_src", "platform": "postgresql"},
        )
        obj = await client.post(
            f"{BASE}/objects/",
            json={
                "source_id": src.json()["id"],
                "object_type": "table",
                "name": f"{_PREFIX}col_tbl",
            },
        )
        return obj.json()["id"]

    async def test_create_column(self, client: AsyncClient):
        obj_id = await self._make_object(client)
        resp = await client.post(
            f"{BASE}/columns/",
            json={
                "object_id": obj_id,
                "name": f"{_PREFIX}order_id",
                "data_type": "integer",
                "is_primary_key": True,
                "is_nullable": False,
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["is_primary_key"] is True

    async def test_list_columns_by_object(self, client: AsyncClient):
        obj_id = await self._make_object(client)
        await client.post(
            f"{BASE}/columns/",
            json={"object_id": obj_id, "name": f"{_PREFIX}col_a"},
        )
        resp = await client.get(f"{BASE}/columns/?object_id={obj_id}")
        assert resp.status_code == 200
        assert resp.json()["count"] >= 1

    async def test_update_column(self, client: AsyncClient):
        obj_id = await self._make_object(client)
        create = await client.post(
            f"{BASE}/columns/",
            json={"object_id": obj_id, "name": f"{_PREFIX}col_upd"},
        )
        col_id = create.json()["id"]
        resp = await client.put(
            f"{BASE}/columns/{col_id}",
            json={"data_type": "varchar"},
        )
        assert resp.status_code == 200
        assert resp.json()["data_type"] == "varchar"


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


class TestLineageEndpoints:
    async def _make_two_objects(self, client: AsyncClient) -> tuple[str, str]:
        src = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}lin_src", "platform": "postgresql"},
        )
        src_id = src.json()["id"]
        a = await client.post(
            f"{BASE}/objects/",
            json={"source_id": src_id, "object_type": "table", "name": f"{_PREFIX}lin_a"},
        )
        b = await client.post(
            f"{BASE}/objects/",
            json={"source_id": src_id, "object_type": "view", "name": f"{_PREFIX}lin_b"},
        )
        return a.json()["id"], b.json()["id"]

    async def test_create_lineage(self, client: AsyncClient):
        a_id, b_id = await self._make_two_objects(client)
        resp = await client.post(
            f"{BASE}/lineage/",
            json={
                "source_object_id": a_id,
                "target_object_id": b_id,
                "lineage_type": "derived",
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["lineage_type"] == "derived"

    async def test_self_reference_rejected(self, client: AsyncClient):
        a_id, _ = await self._make_two_objects(client)
        resp = await client.post(
            f"{BASE}/lineage/",
            json={"source_object_id": a_id, "target_object_id": a_id},
        )
        assert resp.status_code == 422

    async def test_get_lineage_not_found(self, client: AsyncClient):
        from uuid import uuid4
        resp = await client.get(f"{BASE}/lineage/{uuid4()}")
        assert resp.status_code == 404

    async def test_list_lineage_by_source(self, client: AsyncClient):
        a_id, b_id = await self._make_two_objects(client)
        await client.post(
            f"{BASE}/lineage/",
            json={"source_object_id": a_id, "target_object_id": b_id},
        )
        resp = await client.get(f"{BASE}/lineage/?source_object_id={a_id}")
        assert resp.status_code == 200
        assert resp.json()["count"] >= 1

    async def test_delete_lineage(self, client: AsyncClient):
        a_id, b_id = await self._make_two_objects(client)
        create = await client.post(
            f"{BASE}/lineage/",
            json={"source_object_id": a_id, "target_object_id": b_id},
        )
        lin_id = create.json()["id"]
        assert (await client.delete(f"{BASE}/lineage/{lin_id}")).status_code == 204
        assert (await client.get(f"{BASE}/lineage/{lin_id}")).status_code == 404

    async def test_downstream_impact(self, client: AsyncClient):
        src = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}impact_src", "platform": "postgresql"},
        )
        sid = src.json()["id"]
        a = (await client.post(f"{BASE}/objects/", json={"source_id": sid, "object_type": "table", "name": f"{_PREFIX}imp_a"})).json()["id"]
        b = (await client.post(f"{BASE}/objects/", json={"source_id": sid, "object_type": "view", "name": f"{_PREFIX}imp_b"})).json()["id"]
        c = (await client.post(f"{BASE}/objects/", json={"source_id": sid, "object_type": "dashboard", "name": f"{_PREFIX}imp_c"})).json()["id"]
        await client.post(f"{BASE}/lineage/", json={"source_object_id": a, "target_object_id": b})
        await client.post(f"{BASE}/lineage/", json={"source_object_id": b, "target_object_id": c})

        resp = await client.get(f"{BASE}/lineage/impact/{a}/downstream")
        assert resp.status_code == 200
        body = resp.json()
        assert body["direction"] == "downstream"
        node_ids = {n["id"] for n in body["nodes"]}
        assert b in node_ids
        assert c in node_ids

    async def test_upstream_impact(self, client: AsyncClient):
        src = await client.post(
            f"{BASE}/sources/",
            json={"name": f"{_PREFIX}up_src2", "platform": "postgresql"},
        )
        sid = src.json()["id"]
        a = (await client.post(f"{BASE}/objects/", json={"source_id": sid, "object_type": "table", "name": f"{_PREFIX}up2_a"})).json()["id"]
        b = (await client.post(f"{BASE}/objects/", json={"source_id": sid, "object_type": "view", "name": f"{_PREFIX}up2_b"})).json()["id"]
        await client.post(f"{BASE}/lineage/", json={"source_object_id": a, "target_object_id": b})

        resp = await client.get(f"{BASE}/lineage/impact/{b}/upstream")
        assert resp.status_code == 200
        assert body["direction"] == "upstream" if (body := resp.json()) else True
        assert any(n["id"] == a for n in resp.json()["nodes"])
