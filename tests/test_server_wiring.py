"""
Integration smoke-test: multi-portal wiring in server.py.

Sets up env vars and patches build_portal_contexts before importing server.py
so no real DB/S3 is needed.  The patch is scoped to the import so it does not
corrupt the portal_loader module for other tests.
"""
import os
import types
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("BIOINDEX_TOKEN_SIGNING_KEY", "00" * 32)
os.environ["BIOINDEX_ENV"] = "test"
os.environ.setdefault("BIOINDEX_CONFIG_DIR", "/tmp/no-such-bioindex-dir")

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

with patch("bioindex.lib.portal_loader.build_portal_contexts", return_value=[_STUB_CTX]):
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
