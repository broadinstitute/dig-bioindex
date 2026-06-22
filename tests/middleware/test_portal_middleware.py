import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware, get_portal_ctx


def _make_ctx(name):
    return PortalContext(name=name, config=object(), engine=object(), indexes={})


@pytest.fixture(autouse=True)
def _registry():
    init_registry([_make_ctx("p")])


def _app():
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=("health",))

    @app.get("/echo")
    def echo(req: Request):
        return {"portal": get_portal_ctx(req).name}

    @app.get("/health")
    def health():
        return {"ok": True}

    return app


def test_known_portal_strips_prefix_and_resolves():
    client = TestClient(_app())
    resp = client.get("/p/echo")
    assert resp.status_code == 200
    assert resp.json()["portal"] == "p"


def test_unknown_portal_returns_404_with_valid_portals():
    client = TestClient(_app())
    resp = client.get("/nope/echo")
    assert resp.status_code == 404
    body = resp.json()
    assert body["valid_portals"] == ["p"]


def test_reserved_prefix_bypasses_portal_lookup():
    client = TestClient(_app())
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
