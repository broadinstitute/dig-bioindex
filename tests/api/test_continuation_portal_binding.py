"""
Regression test for the cross-portal continuation-token binding.

Phase 5 added portal_name to ContState so that a token issued under
portal A cannot be replayed against portal B. This test exercises the
403 path in api_cont when the portal in the request URL differs from
the portal that signed the token.
"""
import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api.bio import router as bio_router
from bioindex.lib import signed_tokens
from bioindex.lib.continuation import ContState
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware


@pytest.fixture(autouse=True)
def _signing_key(monkeypatch):
    # Set a deterministic key for the duration of the test.
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
    init_registry([_stub_ctx("portal_a"), _stub_ctx("portal_b")])
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware,
                       reserved_prefixes=("health", "ready"))
    app.include_router(bio_router, prefix="/api/bio", tags=["bio"])
    return app


def test_cont_rejects_token_from_different_portal():
    """A token issued under portal_a must return 403 when replayed at portal_b."""
    state = ContState(
        type="fetch",
        index_name="dummy",
        index_arity=1,
        qs=["x"],
        portal_name="portal_a",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    client = TestClient(_make_app())
    r = client.get(f"/portal_b/api/bio/cont?token={token}")
    assert r.status_code == 403
    assert "different portal" in r.json()["detail"].lower()


def test_cont_accepts_token_from_same_portal_but_fails_open_for_unknown_index():
    """
    Sanity check: when the portal matches, the token is accepted and the
    request proceeds past the binding check. The request will subsequently
    fail (HTTP 400) because the registered PortalContext stubs have an
    empty indexes dict, but that confirms we got past the 403 gate.
    """
    state = ContState(
        type="fetch",
        index_name="dummy",
        index_arity=1,
        qs=["x"],
        portal_name="portal_a",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    client = TestClient(_make_app(), raise_server_exceptions=False)
    r = client.get(f"/portal_a/api/bio/cont?token={token}")
    # Past the 403; we expect 400 from "index no longer present" or similar.
    # The stub engine has no .connect() so _refresh_indexes raises AttributeError
    # which Starlette surfaces as a 500 — that is still past the 403 gate.
    assert r.status_code != 403
    assert r.status_code in (400, 500)  # acceptable: post-403 failure
