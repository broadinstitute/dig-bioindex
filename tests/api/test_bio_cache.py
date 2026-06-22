"""
TDD tests for Tasks 4 + 5:
  - Task 4: _match_keys mints ContState.generation from index_generation()
  - Task 5: /cont rejects tokens whose generation no longer matches
"""
import os
import types

import orjson
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import bioindex.api.bio as bio
from bioindex.api.bio import _match_keys, router as bio_router
from bioindex.lib import signed_tokens
from bioindex.lib.continuation import ContState
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _signing_key(monkeypatch):
    monkeypatch.setenv("BIOINDEX_TOKEN_SIGNING_KEY", "0" * 64)
    signed_tokens.signing_key.cache_clear()
    yield
    signed_tokens.signing_key.cache_clear()


def _stub_ctx(name):
    return PortalContext(
        name=name, config=object(), engine=object(),
        portal=None, indexes={}, gql_schema=None,
    )


def _make_app():
    init_registry([_stub_ctx("p")])
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware,
                       reserved_prefixes=("health", "ready"))
    app.include_router(bio_router, prefix="/api/bio", tags=["bio"])
    return app


# ---------------------------------------------------------------------------
# Task 4: _match_keys must bind index_generation into the minted token
# ---------------------------------------------------------------------------

class _FakeConfig:
    match_limit = 2


class _FakeCtx:
    name = "myportal"
    config = _FakeConfig()
    engine = None


def test_match_keys_mints_generation(monkeypatch):
    """
    _match_keys should embed the generation value it receives into the
    ContState that it encodes as the continuation token.
    """
    ctx = _FakeCtx()
    fake_index = types.SimpleNamespace(name="idx")
    # query.match (keyset) returns 3 keys >= match_limit (2), so a token is minted.
    monkeypatch.setattr(bio.query, "match", lambda *a, **k: ["key_a", "key_b", "key_c"])

    # _match_keys now returns a nonce-free body dict (finalised by _cached_response)
    data = _match_keys(ctx, fake_index, ["q"], None, generation="GENx")

    token = data["continuation"]
    assert token is not None, "Expected a continuation token (3 keys >= match_limit 2)"

    state = signed_tokens.decode(token, signed_tokens.signing_key())
    assert state.generation == "GENx", (
        f"Expected generation='GENx' in minted token, got {state.generation!r}"
    )


# ---------------------------------------------------------------------------
# Task 5: /cont must reject tokens whose generation has changed (409)
# ---------------------------------------------------------------------------

def test_cont_rejects_stale_generation(monkeypatch):
    """
    A continuation token minted with generation='G1' must be rejected with
    HTTP 409 when the current index_generation() returns 'G2'.
    """
    monkeypatch.setattr("bioindex.api.bio.index_generation",
                        lambda engine, index_name, **kw: "G2")

    state = ContState(
        type="fetch",
        index_name="idx",
        index_arity=1,
        qs=["x"],
        portal_name="p",
        generation="G1",   # stale
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    client = TestClient(_make_app(), raise_server_exceptions=False)
    r = client.get(f"/p/api/bio/cont?token={token}")

    assert r.status_code == 409, (
        f"Expected 409 for stale generation, got {r.status_code}: {r.text}"
    )
    detail = r.json().get("detail", "").lower()
    assert "stale" in detail or "rebuilt" in detail or "re-run" in detail, (
        f"Expected staleness hint in detail, got: {r.json()['detail']!r}"
    )


def test_cont_passes_generation_check_when_matching(monkeypatch):
    """
    When the token's generation matches the current index_generation(),
    the /cont handler must NOT return 409.  It will fail for other reasons
    (stub engine has no DB), but must proceed past the generation guard.
    """
    monkeypatch.setattr("bioindex.api.bio.index_generation",
                        lambda engine, index_name, **kw: "G1")

    state = ContState(
        type="fetch",
        index_name="idx",
        index_arity=1,
        qs=["x"],
        portal_name="p",
        generation="G1",   # matches
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    client = TestClient(_make_app(), raise_server_exceptions=False)
    r = client.get(f"/p/api/bio/cont?token={token}")

    assert r.status_code != 409, (
        f"Should NOT get 409 when generation matches; got {r.status_code}: {r.text}"
    )
    # The stub engine will blow up past the guard (400 or 500) — both are fine.
    assert r.status_code in (400, 500), (
        f"Expected 400 or 500 (post-generation-guard failure), got {r.status_code}"
    )


def test_cont_portal_binding_checked_before_generation(monkeypatch):
    """
    Cross-portal tokens must still fail with 403, NOT 409.
    The portal-binding check must run before the generation check.
    """
    monkeypatch.setattr("bioindex.api.bio.index_generation",
                        lambda engine, index_name, **kw: "G2")

    state = ContState(
        type="fetch",
        index_name="idx",
        index_arity=1,
        qs=["x"],
        portal_name="other_portal",   # different from "p"
        generation="G1",              # stale — but should never be reached
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    # Register both portals so the app doesn't 404 at routing
    init_registry([_stub_ctx("p"), _stub_ctx("other_portal")])
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=("health", "ready"))
    app.include_router(bio_router, prefix="/api/bio", tags=["bio"])

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get(f"/p/api/bio/cont?token={token}")

    assert r.status_code == 403, (
        f"Cross-portal token must 403 before reaching generation check; "
        f"got {r.status_code}: {r.text}"
    )


# ---------------------------------------------------------------------------
# Task 7: _cached_response helper — cache logic, X-Cache header, fresh nonce
# ---------------------------------------------------------------------------

def test_cached_response_produces_once_and_marks_hit_miss(monkeypatch):
    bio._RESP_CACHE = bio.ResponseCache(max_bytes=10_000)   # fresh cache
    calls = {"n": 0}

    def produce():
        calls["n"] += 1
        return {"data": [1, 2, 3], "continuation": None}

    r1 = bio._cached_response("k1", None, produce)
    r2 = bio._cached_response("k1", None, produce)
    assert calls["n"] == 1                                   # second served from cache
    assert r1.headers["X-Cache"] == "MISS" and r2.headers["X-Cache"] == "HIT"
    b1 = orjson.loads(r1.body)
    b2 = orjson.loads(r2.body)
    assert b1["data"] == b2["data"]
    assert b1["nonce"] != b2["nonce"]                        # fresh nonce per response


def test_cached_response_bypasses_when_restricted(monkeypatch):
    bio._RESP_CACHE = bio.ResponseCache(max_bytes=10_000)
    calls = {"n": 0}

    def produce():
        calls["n"] += 1
        return {"data": [1]}

    bio._cached_response("k", {"pheno": {"T2D"}}, produce)
    bio._cached_response("k", {"pheno": {"T2D"}}, produce)
    assert calls["n"] == 2                                   # restricted => never cached


def test_finalize_always_adds_nonce_and_cache_control():
    """Every _finalize call produces Cache-Control: no-store and a nonce."""
    r = bio._finalize({"data": []}, "MISS")
    assert r.headers["Cache-Control"] == "no-store"
    assert r.headers["X-Cache"] == "MISS"
    body = orjson.loads(r.body)
    assert "nonce" in body

    r2 = bio._finalize({"data": []}, "HIT")
    assert r2.headers["X-Cache"] == "HIT"


def test_cached_response_miss_has_cache_control_no_store():
    """Cache-Control: no-store must be present on every response, hit or miss."""
    bio._RESP_CACHE = bio.ResponseCache(max_bytes=10_000)

    r_miss = bio._cached_response("km", None, lambda: {"data": []})
    assert r_miss.headers["Cache-Control"] == "no-store"

    r_hit = bio._cached_response("km", None, lambda: {"data": []})
    assert r_hit.headers["Cache-Control"] == "no-store"


def test_cached_response_none_key_always_misses():
    """A None cache key means: compute fresh every time, always X-Cache: MISS."""
    bio._RESP_CACHE = bio.ResponseCache(max_bytes=10_000)
    calls = {"n": 0}

    def produce():
        calls["n"] += 1
        return {"data": []}

    bio._cached_response(None, None, produce)
    bio._cached_response(None, None, produce)
    assert calls["n"] == 2  # both are fresh computes


def test_nonce_not_stored_in_cache():
    """The body stored in the cache must NOT contain a nonce key."""
    bio._RESP_CACHE = bio.ResponseCache(max_bytes=10_000)
    bio._cached_response("knonce", None, lambda: {"data": [42]})
    cached = bio._RESP_CACHE.get("knonce")
    assert cached is not None
    assert "nonce" not in cached  # nonce injected at finalize, not stored


# ---------------------------------------------------------------------------
# Task 8: Integration test — generation bump invalidates query cache
# ---------------------------------------------------------------------------

def test_generation_bump_invalidates_query_cache():
    """
    Integration test: because `generation` is part of the _query_cache_key,
    an index rebuild (generation bump) produces a different key and therefore
    a guaranteed cache MISS, repopulating from fresh data.

    The /cont 409-on-rebuild path is separately covered by
    test_cont_rejects_stale_generation above.
    """
    bio._RESP_CACHE = bio.ResponseCache(max_bytes=100_000)   # fresh cache
    calls = {"n": 0}

    def produce():
        calls["n"] += 1
        return {"data": [calls["n"]], "continuation": None}

    # --- generation G1 ---
    k1 = bio._query_cache_key("p", "idx", 1, "row", "GEN1", ["x"])
    r1a = bio._cached_response(k1, None, produce)   # MISS (produce #1)
    r1b = bio._cached_response(k1, None, produce)   # HIT  (no produce)
    assert calls["n"] == 1, "produce() should have been called exactly once (cache hit on second call)"
    assert r1a.headers["X-Cache"] == "MISS", "First call must be a MISS"
    assert r1b.headers["X-Cache"] == "HIT",  "Second call with same key must be a HIT"

    # --- index rebuilt -> generation G2 -> different key ---
    k2 = bio._query_cache_key("p", "idx", 1, "row", "GEN2", ["x"])
    assert k2 != k1, "generation is part of the cache key — G2 key must differ from G1 key"

    r2 = bio._cached_response(k2, None, produce)    # MISS (produce #2) -> fresh data
    assert calls["n"] == 2, "After generation bump the new key must MISS and call produce()"
    assert r2.headers["X-Cache"] == "MISS", "New-generation key must be a MISS"

    # Verify the fresh response actually returned the updated data (not the old cached body)
    import orjson
    body2 = orjson.loads(r2.body)
    assert body2["data"] == [2], f"Expected fresh data=[2] after rebuild, got {body2['data']}"


# ---------------------------------------------------------------------------
# limit must be part of _query_cache_key
#
# /match and /query both accept a `limit` that bounds the page/budget. If limit
# is not part of the cache key, the first-cached variant for a given query is
# served to every later request regardless of its limit — e.g. a no-limit page
# of 100 keys answers a later ?limit=3 request (and vice-versa). The key must
# distinguish requests that differ only in `limit`.
# ---------------------------------------------------------------------------

def test_limit_is_part_of_query_cache_key():
    base = ("p", "idx", 1, "match", "GEN1", ["x"])

    k_none = bio._query_cache_key(*base)
    k_5 = bio._query_cache_key(*base, limit=5)
    k_50 = bio._query_cache_key(*base, limit=50)

    assert k_5 != k_none, "limit=5 must not collide with the no-limit (unbounded) key"
    assert k_5 != k_50, "requests differing only in limit must produce different keys"
    assert k_5 == bio._query_cache_key(*base, limit=5), "same inputs (incl. limit) must be stable"
    assert k_none == bio._query_cache_key(*base), "omitting limit must remain backwards-compatible/stable"
