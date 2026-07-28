from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.responses import RedirectResponse

from bioindex.middleware.portal import PortalResolveMiddleware
from bioindex.lib.portal_registry import init_registry
from bioindex.lib.portal_context import PortalContext


def _make_app(reserved=("health", "ready")):
    init_registry([
        PortalContext(name="cfde", config=object(), engine=object(),
                      portal=None, indexes={}, gql_schema=None),
    ])
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=reserved)

    @app.get("/api/bio/ping")
    def ping(request: Request):
        return {"portal": request.state.portal_ctx.name, "path": request.url.path}

    @app.get("/health")
    def health():
        return {"ok": True}
    return app


def test_known_portal_prefix_resolves_and_strips():
    client = TestClient(_make_app())
    r = client.get("/cfde/api/bio/ping")
    assert r.status_code == 200
    body = r.json()
    assert body["portal"] == "cfde"
    # path seen by the route is /api/bio/ping (prefix stripped)
    assert body["path"].endswith("/api/bio/ping")


def test_unknown_portal_returns_404_with_valid_list():
    client = TestClient(_make_app())
    r = client.get("/nope/api/bio/ping")
    assert r.status_code == 404
    body = r.json()
    assert "cfde" in body["valid_portals"]


def test_reserved_prefix_bypasses_portal_resolution():
    client = TestClient(_make_app())
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_root_without_portal_returns_404():
    client = TestClient(_make_app())
    r = client.get("/")
    assert r.status_code == 404


def test_trailing_slash_redirect_keeps_the_portal_prefix():
    # the route only sees the stripped path, so Starlette builds the redirect
    # without the portal on it; following that would 404 as portal 'api'
    client = TestClient(_make_app())
    r = client.get("/cfde/api/bio/ping/", follow_redirects=False)
    assert r.status_code == 307
    assert urlsplit(r.headers["location"]).path == "/cfde/api/bio/ping"

    assert client.get("/cfde/api/bio/ping/").status_code == 200


def test_offsite_redirect_is_left_alone():
    app = _make_app()

    @app.get("/api/bio/away")
    def away():
        return RedirectResponse("https://example.org/elsewhere", status_code=307)

    r = TestClient(app).get("/cfde/api/bio/away", follow_redirects=False)
    assert r.headers["location"] == "https://example.org/elsewhere"


def test_handler_redirect_is_not_double_prefixed():
    # a handler builds its own Location and already knows its portal
    app = _make_app()

    @app.get("/api/bio/go")
    def go():
        return RedirectResponse("/cfde/api/bio/ping", status_code=307)

    r = TestClient(app).get("/cfde/api/bio/go", follow_redirects=False)
    assert r.headers["location"] == "/cfde/api/bio/ping"
