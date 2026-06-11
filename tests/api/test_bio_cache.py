"""
TDD tests for Task 4:
  - _match_keys mints ContState.generation from the value passed by the caller
    (which is computed via index_generation() in the handler).
"""
import json

import pytest

from bioindex.api.bio import _match_keys
from bioindex.lib import signed_tokens


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _signing_key(monkeypatch):
    monkeypatch.setenv("BIOINDEX_TOKEN_SIGNING_KEY", "0" * 64)
    signed_tokens.signing_key.cache_clear()
    yield
    signed_tokens.signing_key.cache_clear()


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
    data = json.loads(body)

    token = data["continuation"]
    assert token is not None, "Expected a continuation token (3 keys >= match_limit 2)"

    state = signed_tokens.decode(token, signed_tokens.signing_key())
    assert state.generation == "GENx", (
        f"Expected generation='GENx' in minted token, got {state.generation!r}"
    )
