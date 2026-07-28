import logging
import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware


@pytest.fixture(autouse=True)
def _registry():
    init_registry([PortalContext(name="p", config=object(), engine=object(), indexes={})])


@pytest.fixture
def records(caplog):
    caplog.set_level(logging.INFO, logger="bioindex.access")
    return lambda: [r for r in caplog.records if r.name == "bioindex.access"]


def _app():
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=("health",))

    @app.get("/api/bio/query/{index}")
    def query(index: str, q: str = ""):
        return {"index": index, "q": q}

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/api/bio/boom")
    def boom():
        raise RuntimeError("kaboom")

    return app


def _get(path, **kwargs):
    return TestClient(_app(), raise_server_exceptions=False).get(path, **kwargs)


def test_record_describes_the_request(records):
    resp = _get("/p/api/bio/query/gene?q=SLC30A8")
    assert resp.status_code == 200

    (rec,) = records()
    assert rec.portal == "p"
    assert rec.method == "GET"
    assert rec.route == "/api/bio/query/{index}"
    assert rec.path == "/api/bio/query/gene"
    assert rec.query == "q=SLC30A8"
    assert rec.status == 200
    assert rec.response_bytes > 0
    assert rec.latency_ms >= 0
    assert rec.request_id


def test_reserved_path_is_logged_without_a_portal(records):
    assert _get("/health").status_code == 200

    (rec,) = records()
    assert rec.portal is None
    assert rec.route == "/health"
    assert rec.path == "/health"


def test_unknown_portal_is_logged_with_the_path_it_asked_for(records):
    assert _get("/nope/api/bio/query/gene").status_code == 404

    (rec,) = records()
    assert rec.portal is None
    assert rec.status == 404
    assert rec.path == "/nope/api/bio/query/gene"


@pytest.mark.parametrize("param", ["token", "access_token"])
def test_secret_query_values_are_redacted(records, param):
    secret = "sensitive." * 20
    _get(f"/p/api/bio/query/gene?q=SLC30A8&{param}={secret}")

    (rec,) = records()
    assert rec.query == f"q=SLC30A8&{param}=<redacted>"
    assert "sensitive" not in rec.query


def test_well_formed_request_id_is_kept(records):
    _get("/p/api/bio/query/gene", headers={"X-Request-Id": "abc-123_xyz.42"})

    assert records()[0].request_id == "abc-123_xyz.42"


@pytest.mark.parametrize("bad", ["a" * 200, "has spaces", "semi;colon", ""])
def test_unusable_request_id_is_replaced_with_a_uuid(records, bad):
    _get("/p/api/bio/query/gene", headers={"X-Request-Id": bad})

    request_id = records()[0].request_id
    assert request_id != bad
    assert re.fullmatch(r"[0-9a-f]{32}", request_id)


@pytest.mark.parametrize("headers,expected", [
    ({"X-Real-IP": "203.0.113.7"}, "203.0.113.0"),
    ({"X-Forwarded-For": "198.51.100.4, 10.0.0.1"}, "198.51.100.0"),
    ({"X-Real-IP": "203.0.113.7", "X-Forwarded-For": "198.51.100.4"}, "203.0.113.0"),
    ({"X-Real-IP": "2001:db8:1234:5678::1"}, "2001:db8:1234::"),
])
def test_client_ip_is_coarsened(records, headers, expected):
    _get("/p/api/bio/query/gene", headers=headers)

    assert records()[0].client_ip == expected


def test_unparseable_client_address_is_dropped(records):
    # the test client's peer is the literal "testclient"
    _get("/p/api/bio/query/gene")

    assert records()[0].client_ip is None


def test_user_agent_is_recorded_and_capped(records):
    _get("/p/api/bio/query/gene", headers={"User-Agent": "curl/8.0"})
    assert records()[0].user_agent == "curl/8.0"

    _get("/p/api/bio/query/gene", headers={"User-Agent": "x" * 500})
    assert records()[-1].user_agent == "x" * 256


def test_handler_exception_is_logged_and_answered_with_a_500(records):
    resp = _get("/p/api/bio/boom")
    assert resp.status_code == 500

    (rec,) = records()
    assert rec.levelno == logging.ERROR
    assert rec.status == 500
    assert rec.portal == "p"
    assert rec.path == "/api/bio/boom"
    assert rec.exc_info is not None

    # the caller gets the id to quote back, and nothing else
    assert resp.json() == {"detail": "Internal server error", "request_id": rec.request_id}
