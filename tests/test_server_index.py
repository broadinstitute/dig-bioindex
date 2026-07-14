"""
Tests for the search page served at each portal root by ``bioindex.server``.

Importing ``bioindex.server`` builds the portal registry at import time, which
would otherwise need real DB engines — patch the loader just for the import.
"""
import os
from unittest import mock
from urllib.parse import urljoin

import pytest
from fastapi.testclient import TestClient

from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry


def _ctx(name="cfde"):
    return PortalContext(name=name, config=object(), engine=object(),
                         portal=None, indexes={}, gql_schema=None)


os.environ.setdefault("BIOINDEX_ENV", "qa")
with mock.patch("bioindex.lib.portal_loader.build_portal_contexts",
                return_value=[_ctx()]):
    from bioindex import server as srv


@pytest.fixture
def client():
    # conftest's autouse _reset_registry nulls the process-global registry
    # before every test, but the app was built at import — re-register here.
    init_registry([_ctx()])
    return TestClient(srv.app, follow_redirects=False)


def test_portal_root_serves_search_page(client):
    r = client.get("/cfde/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "BioIndex" in r.text


def test_portal_root_without_trailing_slash_redirects(client):
    """
    The page calls the API with relative URLs (./api/bio/...), which only
    resolve under /<portal>/ when the browser keeps the trailing slash.
    """
    r = client.get("/cfde")
    assert r.status_code == 307
    assert r.headers["location"] == "/cfde/"


def test_bare_portal_redirect_preserves_query_string(client):
    r = client.get("/cfde?q=rs123")
    assert r.status_code == 307
    assert r.headers["location"] == "/cfde/?q=rs123"


def test_bare_root_without_portal_is_not_found(client):
    """No portal to query, so the page would be useless — stays a 404."""
    r = client.get("/")
    assert r.status_code == 404


def test_unknown_portal_root_still_reports_unknown_portal(client):
    r = client.get("/nope/")
    assert r.status_code == 404
    assert "cfde" in r.json()["valid_portals"]


def test_api_still_reachable_under_portal_prefix(client):
    """The index route must not shadow the portal-scoped API paths."""
    r = client.get("/cfde/api/bio/indexes")
    assert r.status_code != 404


def test_served_page_relative_calls_resolve_under_portal(client):
    """
    Pins why the redirect exists: the page ships relative API URLs, and only
    the trailing-slash URL resolves them to the portal rather than the root.
    """
    page = client.get("/cfde/")
    assert "./api/bio/query" in page.text

    assert urljoin("/cfde/", "./api/bio/query") == "/cfde/api/bio/query"
    assert urljoin("/cfde", "./api/bio/query") == "/api/bio/query"
