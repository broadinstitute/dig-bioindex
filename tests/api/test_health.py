import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api.health import router as health_router
from bioindex.lib.portal_registry import init_registry
from bioindex.lib.portal_context import PortalContext


@pytest.fixture(autouse=True)
def _reset_registry():
    import bioindex.lib.portal_registry as pr
    pr._registry = None
    yield
    pr._registry = None


def _make_app(engines_ok=True):
    eng = MagicMock()
    if engines_ok:
        # Make engine.connect() yield a context manager whose __enter__
        # returns a conn with a no-op execute().
        conn = MagicMock()
        conn.execute = MagicMock(return_value=None)
        eng.connect.return_value.__enter__.return_value = conn
    else:
        eng.connect.side_effect = RuntimeError("db down")

    init_registry([
        PortalContext(name="cfde", config=object(), engine=eng,
                      portal=None, indexes={}, gql_schema=None),
    ])

    app = FastAPI()
    app.include_router(health_router)
    return app


def test_health_returns_200():
    r = TestClient(_make_app()).get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_ready_returns_200_when_portals_healthy():
    r = TestClient(_make_app(engines_ok=True)).get("/ready")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["portals"]["cfde"] == "ok"


def test_ready_returns_503_only_when_all_portals_fail():
    # one portal, db down — all-fail case
    r = TestClient(_make_app(engines_ok=False)).get("/ready")
    assert r.status_code == 503
    assert r.json()["portals"]["cfde"] != "ok"
