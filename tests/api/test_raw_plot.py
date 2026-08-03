"""
The plot endpoints used to 500 on a plot that wasn't there.

They read with `s3.read_object` and guarded on `if content is None`, but
read_object is `get_object(...).get('Body')` - it raises NoSuchKey for a
missing key and never returns None, so the guard was unreachable. That was
36 real 500s in prod over 14 days, all on these routes.
"""
import types

import botocore.exceptions
import fastapi
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api import raw
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware

PNG = b'\x89PNG\r\n\x1a\n'


@pytest.fixture(autouse=True)
def _registry():
    ctx = PortalContext(name='p', config=types.SimpleNamespace(
        s3_bucket='bucket', s3_path=lambda k: k), engine=object(), indexes={})
    init_registry([ctx])


@pytest.fixture(autouse=True)
def _allowed(monkeypatch):
    """These tests are about the object, not about who may see it."""
    monkeypatch.setattr(raw, 'verify_permissions', lambda *a, **kw: True)


@pytest.fixture
def client():
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=('health',))
    app.include_router(raw.router, prefix='/api/raw')
    return TestClient(app)


# the two routes that are reachable through the router; the third handler is
# exercised directly below, because {file:path} is greedy and swallows the
# ancestry segment before its own route is ever tried
ROUTES = [
    '/p/api/raw/plot/dataset/ds1/manhattan.png',
    '/p/api/raw/plot/phenotype/T2D/manhattan.png',
]


@pytest.mark.parametrize('route', ROUTES)
def test_a_missing_plot_is_a_404(client, monkeypatch, route):
    monkeypatch.setattr(raw.s3, 'read_object_with_etag', lambda bucket, path: None)

    assert client.get(route).status_code == 404


@pytest.mark.parametrize('route', ROUTES)
def test_a_plot_that_is_there_is_served_as_a_png(client, monkeypatch, route):
    monkeypatch.setattr(raw.s3, 'read_object_with_etag', lambda bucket, path: (PNG, '"t"'))

    resp = client.get(route)

    assert resp.status_code == 200
    assert resp.content == PNG
    assert resp.headers['content-type'] == 'image/png'


@pytest.mark.parametrize('route', ROUTES)
def test_a_read_failure_is_not_disguised_as_a_missing_plot(client, monkeypatch, route):
    def boom(bucket, path):
        raise botocore.exceptions.ClientError(
            {'Error': {'Code': 'AccessDenied'}, 'ResponseMetadata': {'HTTPStatusCode': 403}},
            'GetObject')

    monkeypatch.setattr(raw.s3, 'read_object_with_etag', boom)

    assert client.get(route).status_code == 500


@pytest.mark.asyncio
async def test_a_missing_ancestry_plot_is_a_404(monkeypatch):
    # not reachable through the router, so call it the way test_raw_ctx does
    monkeypatch.setattr(raw.s3, 'read_object_with_etag', lambda bucket, path: None)

    req = types.SimpleNamespace(state=types.SimpleNamespace(portal_ctx=types.SimpleNamespace(
        name='p', config=types.SimpleNamespace(s3_bucket='bucket', s3_path=lambda k: k),
        portal=None)))

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await raw.api_raw_plot_phenotype_ancestry(
            phenotype='T2D', ancestry='EU', file='manhattan.png', req=req)

    assert exc_info.value.status_code == 404
