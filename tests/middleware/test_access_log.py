import logging
import re

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


def test_continuation_token_redacted_in_log(access_log_records):
    client = TestClient(_make_app())
    long_token = "abc.def.ghi" * 50
    client.get(f"/cfde/api/bio/query/gene?q=SLC30A8&token={long_token}")
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    assert "<redacted>" in records[0].query
    assert long_token not in records[0].query


def test_access_token_redacted_in_log(access_log_records):
    client = TestClient(_make_app())
    client.get("/cfde/api/bio/query/gene?q=foo&access_token=secret_oauth_token_here")
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    assert "<redacted>" in records[0].query
    assert "secret_oauth_token_here" not in records[0].query


def test_request_id_rejected_when_oversized_or_invalid(access_log_records):
    client = TestClient(_make_app())
    long_id = "a" * 200
    r = client.get("/cfde/api/bio/query/gene", headers={"X-Request-Id": long_id})
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    # over-long ID rejected; UUID hex used instead (32 chars, hex only)
    assert records[0].request_id != long_id
    assert re.fullmatch(r"[0-9a-f]{32}", records[0].request_id)


def test_request_id_accepted_when_well_formed(access_log_records):
    client = TestClient(_make_app())
    good_id = "abc-123_xyz.42"
    r = client.get("/cfde/api/bio/query/gene", headers={"X-Request-Id": good_id})
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    assert records[0].request_id == good_id


def test_access_log_client_ip_from_x_real_ip_truncated_to_24(access_log_records):
    client = TestClient(_make_app())
    client.get("/cfde/api/bio/query/gene?q=X",
               headers={"X-Real-IP": "203.0.113.7"})
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    # last octet zeroed for privacy (/24)
    assert records[0].client_ip == "203.0.113.0"


def test_access_log_client_ip_falls_back_to_leftmost_xff_truncated(access_log_records):
    client = TestClient(_make_app())
    client.get("/cfde/api/bio/query/gene?q=X",
               headers={"X-Forwarded-For": "198.51.100.4, 10.0.0.1"})
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    assert records[0].client_ip == "198.51.100.0"


def test_access_log_captures_user_agent(access_log_records):
    client = TestClient(_make_app())
    client.get("/cfde/api/bio/query/gene?q=X",
               headers={"User-Agent": "python-requests/2.31"})
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    assert records[0].user_agent == "python-requests/2.31"


def test_access_log_truncates_long_user_agent(access_log_records):
    client = TestClient(_make_app())
    client.get("/cfde/api/bio/query/gene?q=X",
               headers={"User-Agent": "x" * 500})
    records = [r for r in access_log_records.records if r.name == "bioindex.access"]
    assert len(records) == 1
    assert records[0].user_agent == "x" * 256
