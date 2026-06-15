import fastapi
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api.raw import router as raw_router
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.lib.response_cache import ResponseCache
from bioindex.middleware.portal import PortalResolveMiddleware
import bioindex.api.raw as raw


# --- _etag_matches ---

def test_etag_matches_exact():
    assert raw._etag_matches('"v1"', '"v1"') is True


def test_etag_matches_none_and_empty():
    assert raw._etag_matches(None, '"v1"') is False
    assert raw._etag_matches("", '"v1"') is False


def test_etag_matches_wildcard():
    assert raw._etag_matches("*", '"v1"') is True


def test_etag_matches_weak_prefix():
    assert raw._etag_matches('W/"v1"', '"v1"') is True


def test_etag_matches_comma_list():
    assert raw._etag_matches('"a", "v1", "b"', '"v1"') is True


def test_etag_matches_no_match():
    assert raw._etag_matches('"other"', '"v1"') is False


# --- shared fixtures/helpers for _raw_response ---

@pytest.fixture
def fresh_raw_cache(monkeypatch):
    monkeypatch.setattr(raw, "_RAW_CACHE", ResponseCache(max_bytes=10_000_000))


def _install_s3(monkeypatch, etag, body, counter):
    def fake_head(bucket, path):
        return {"ETag": etag}

    def fake_get(bucket, path):
        counter["n"] += 1
        return body, etag

    monkeypatch.setattr(raw.s3, "head_object", fake_head)
    monkeypatch.setattr(raw.s3, "read_object_with_etag", fake_get)


# --- 200 MISS/HIT + headers ---

def test_raw_response_miss_then_hit(monkeypatch, fresh_raw_cache):
    counter = {"n": 0}
    _install_s3(monkeypatch, '"v1"', b"BODY", counter)

    r1 = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", None)
    r2 = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", None)

    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.body == b"BODY" and r2.body == b"BODY"
    assert r1.headers["X-Cache"] == "MISS"
    assert r2.headers["X-Cache"] == "HIT"
    assert counter["n"] == 1  # second served from LRU, no second GET


def test_raw_response_headers(monkeypatch, fresh_raw_cache):
    counter = {"n": 0}
    _install_s3(monkeypatch, '"v1"', b"BODY", counter)

    r = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", None)
    assert r.headers["Cache-Control"] == "public, no-cache, must-revalidate"
    assert r.headers["ETag"] == '"v1"'
    # .json.gz => application/json + Content-Encoding: gzip (mimetypes)
    assert r.headers["content-type"].startswith("application/json")
    assert r.headers["Content-Encoding"] == "gzip"


# --- 304 revalidation ---

def test_raw_response_304_when_if_none_match_matches(monkeypatch, fresh_raw_cache):
    counter = {"n": 0}
    _install_s3(monkeypatch, '"v1"', b"BODY", counter)

    r = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", '"v1"')

    assert r.status_code == 304
    assert r.body == b""                       # no body on 304
    assert r.headers["ETag"] == '"v1"'
    assert r.headers["Cache-Control"] == "public, no-cache, must-revalidate"
    assert counter["n"] == 0                    # never fetched the body


# --- changed ETag busts the cache (no stale) ---

def test_raw_response_new_etag_busts_cache(monkeypatch, fresh_raw_cache):
    state = {"etag": '"v1"', "body": b"OLD", "gets": 0}

    def fake_head(bucket, path):
        return {"ETag": state["etag"]}

    def fake_get(bucket, path):
        state["gets"] += 1
        return state["body"], state["etag"]

    monkeypatch.setattr(raw.s3, "head_object", fake_head)
    monkeypatch.setattr(raw.s3, "read_object_with_etag", fake_get)

    r1 = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", None)
    assert r1.body == b"OLD" and r1.headers["ETag"] == '"v1"'

    state["etag"], state["body"] = '"v2"', b"NEW"

    r2 = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", None)
    assert r2.status_code == 200
    assert r2.body == b"NEW"                     # fresh, not cached OLD
    assert r2.headers["ETag"] == '"v2"'
    assert r2.headers["X-Cache"] == "MISS"       # new key => miss
    assert state["gets"] == 2


# --- 404 + disabled cache ---

def test_raw_response_404_when_missing(monkeypatch, fresh_raw_cache):
    monkeypatch.setattr(raw.s3, "head_object", lambda bucket, path: None)

    with pytest.raises(fastapi.HTTPException) as exc:
        raw._raw_response("bkt", "raw/missing.json.gz", "missing.json.gz", None)
    assert exc.value.status_code == 404


def test_raw_response_disabled_cache_still_revalidates(monkeypatch):
    # max_bytes=0 disables the body cache; revalidation must still work.
    monkeypatch.setattr(raw, "_RAW_CACHE", ResponseCache(max_bytes=0))
    counter = {"n": 0}
    _install_s3(monkeypatch, '"v1"', b"BODY", counter)

    r1 = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", None)
    r2 = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", None)
    assert r1.headers["X-Cache"] == "MISS" and r2.headers["X-Cache"] == "MISS"
    assert counter["n"] == 2                      # no body cache => GET each time

    r3 = raw._raw_response("bkt", "raw/x.json.gz", "x.json.gz", '"v1"')
    assert r3.status_code == 304                  # 304 independent of LRU


# --- integration: route serves ETag + revalidation ---

class _FakeRawConfig:
    s3_bucket = "dig-test-bioindex"

    def s3_path(self, path):
        return path


def _raw_app():
    ctx = PortalContext(
        name="p", config=_FakeRawConfig(), engine=object(),
        portal=None, indexes={}, gql_schema=None,
    )
    init_registry([ctx])
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=("health", "ready"))
    app.include_router(raw_router, prefix="/api/raw", tags=["raw"])
    return app


def test_route_serves_and_revalidates(monkeypatch, fresh_raw_cache):
    counter = {"n": 0}
    _install_s3(monkeypatch, '"v1"', b"BODY", counter)
    client = TestClient(_raw_app(), raise_server_exceptions=False)

    r = client.get("/p/api/raw/file/single_cell/DS/fields.json")
    assert r.status_code == 200
    assert r.headers["ETag"] == '"v1"'
    assert r.headers["Cache-Control"] == "public, no-cache, must-revalidate"

    r304 = client.get(
        "/p/api/raw/file/single_cell/DS/fields.json",
        headers={"If-None-Match": '"v1"'},
    )
    assert r304.status_code == 304
