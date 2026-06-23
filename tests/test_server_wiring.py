"""
Integration smoke-test: multi-portal wiring in server.py.

Registry init runs in the lifespan handler, not at import, so importing
server.py has no side effects (no DB/S3).  Tests populate the registry
directly via the autouse fixture and drive the app with a plain TestClient
(no lifespan).
"""
import os
import types
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("BIOINDEX_TOKEN_SIGNING_KEY", "00" * 32)

from bioindex.lib.portal_context import PortalContext  # noqa: E402
from bioindex.lib.portal_registry import init_registry  # noqa: E402

_STUB_INDEX = types.SimpleNamespace(
    name="variants",
    built=True,
    schema=types.SimpleNamespace(
        arity=1,
        key_columns=["varId"],
        has_locus=False,
        __str__=lambda s: "varId",
    ),
    compressed=False,
)
_STUB_INDEXES = {("variants", 1): _STUB_INDEX}
_STUB_CTX = PortalContext(
    name="p",
    config=object(),
    engine=MagicMock(name="stub_engine"),
    indexes=dict(_STUB_INDEXES),
)

import bioindex.server as server  # noqa: E402

from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(server.app)


@pytest.fixture(autouse=True)
def _restore_registry():
    # Other test modules (test_portal_middleware) call init_registry with empty
    # stubs; restore our stub context before each test in this module.
    _STUB_CTX.indexes = dict(_STUB_INDEXES)
    init_registry([_STUB_CTX])


def test_known_portal_indexes_200():
    with patch("bioindex.api.bio._refresh_indexes", lambda ctx: None):
        resp = client.get("/p/api/bio/indexes")
    assert resp.status_code == 200
    body = resp.json()
    assert "data" in body
    names = [item["index"] for item in body["data"]]
    assert "variants" in names


def test_unknown_portal_404_with_valid_portals():
    resp = client.get("/nope/api/bio/indexes")
    assert resp.status_code == 404
    body = resp.json()
    assert "valid_portals" in body
    assert "p" in body["valid_portals"]
