import logging

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from bioindex.middleware.portal import PortalResolveMiddleware
from bioindex.lib.portal_registry import init_registry
from bioindex.lib.portal_context import PortalContext


@pytest.fixture
def access_log_records(caplog):
    caplog.set_level(logging.INFO, logger="bioindex.access")
    yield caplog


def _make_app():
    init_registry([
        PortalContext(name="cfde", config=object(), engine=object(),
                      portal=None, indexes={}, gql_schema=None),
    ])
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=("health",))

    @app.get("/api/bio/query/{index}")
    def query(index: str, request: Request, q: str = ""):
        return {"index": index, "q": q}

    @app.get("/health")
    def health():
        return {"ok": True}
    return app


def test_access_log_emitted_with_full_fields(access_log_records):
    client = TestClient(_make_app())
    r = client.get("/cfde/api/bio/query/gene?q=SLC30A8&fmt=row")
    assert r.status_code == 200

    records = [r for r in access_log_records.records
               if r.name == "bioindex.access"]
    assert len(records) == 1
    rec = records[0]
    assert rec.portal == "cfde"
    assert rec.method == "GET"
    assert rec.route == "/api/bio/query/{index}"
    assert rec.path == "/api/bio/query/gene"
    assert "q=SLC30A8" in rec.query
    assert rec.status == 200
    assert rec.response_bytes > 0
    assert rec.latency_ms >= 0
    assert isinstance(rec.request_id, str)


def test_access_log_for_reserved_path_has_null_portal(access_log_records):
    client = TestClient(_make_app())
    r = client.get("/health")
    assert r.status_code == 200
    records = [r for r in access_log_records.records
               if r.name == "bioindex.access"]
    assert len(records) == 1
    assert records[0].portal is None
    assert records[0].route == "/health"


def test_unknown_portal_request_is_logged_with_null_portal(access_log_records):
    client = TestClient(_make_app())
    r = client.get("/nope/api/bio/query/gene")
    assert r.status_code == 404
    records = [r for r in access_log_records.records
               if r.name == "bioindex.access"]
    assert len(records) == 1
    assert records[0].portal is None
    assert records[0].status == 404
