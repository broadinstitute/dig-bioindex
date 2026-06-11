"""
TDD tests for Tasks 4 + 5:
  - Task 4: _match_keys mints ContState.generation from index_generation()
  - Task 5: /cont rejects tokens whose generation no longer matches
"""
import os
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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


def test_match_keys_mints_generation():
    """
    _match_keys should embed the generation value it receives into the
    ContState that it encodes as the continuation token.
    """
    ctx = _FakeCtx()
    # Provide enough keys to hit match_limit (2), so a continuation is minted.
    keys = iter(["key_a", "key_b", "key_c"])

    response = _match_keys(ctx, keys, "idx", ["q"], limit=None, generation="GENx")
    body = response.body  # ORJSONResponse stores raw bytes in .body
    import json
    data = json.loads(body)

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
