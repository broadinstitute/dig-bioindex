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


def test_portal_root_without_slash_redirects():
    # the index page fetches ./api/bio/... — without the trailing slash those
    # resolve against the server root and miss the portal entirely
    client = TestClient(_app())
    resp = client.get("/p", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/p/"


def test_portal_root_redirect_preserves_the_query_string():
    client = TestClient(_app())
    resp = client.get("/p?q=SLC30A8&limit=5", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers["location"] == "/p/?q=SLC30A8&limit=5"


def test_portal_root_with_slash_is_served_not_redirected():
    app = _app()

    @app.get("/")
    def root():
        return {"ok": True}

    resp = TestClient(app).get("/p/", follow_redirects=False)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_unknown_portal_still_404s_rather_than_redirecting():
    # the redirect must sit behind the registry lookup, or a typo'd portal
    # would bounce to itself instead of reporting the valid names
    client = TestClient(_app())
    resp = client.get("/nope", follow_redirects=False)
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Unknown portal 'nope'"


def test_reserved_prefix_is_not_redirected():
    client = TestClient(_app())
    resp = client.get("/health", follow_redirects=False)
    assert resp.status_code == 200
