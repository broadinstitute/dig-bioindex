import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import RedirectResponse

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


def test_redirect_keeps_the_portal_prefix():
    # routes only ever see the stripped path, so a trailing-slash redirect
    # would otherwise point at a URL with no portal on it
    client = TestClient(_app())
    resp = client.get("/p/echo/", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"].endswith("/p/echo")


def test_handler_redirect_is_not_double_prefixed():
    # a handler builds its own Location and already knows its portal
    app = _app()

    @app.get("/jump")
    def jump():
        return RedirectResponse("/p/echo", status_code=307)

    resp = TestClient(app).get("/p/jump", follow_redirects=False)
    assert resp.headers["location"] == "/p/echo"


def test_offsite_redirect_is_left_alone():
    app = _app()

    @app.get("/away")
    def away():
        return RedirectResponse("https://example.org/elsewhere", status_code=307)

    resp = TestClient(app).get("/p/away", follow_redirects=False)
    assert resp.headers["location"] == "https://example.org/elsewhere"
