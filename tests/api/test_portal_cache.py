"""
Portal metadata is the same answer over and over - roughly 17k requests from
about 55 distinct responses on one route in qa - so it is worth a validator
and a freshness window.

The catch these pin down: the envelope regenerates a nonce and a query timing
on every response, so a tag taken over the whole body would differ every time
and quietly never match.
"""
import types

import orjson
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api import portal as portal_api
from bioindex.lib import http_cache
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware

GROUPS = [('t2d', 'T2D', 'diabetes', 1, 'main')]


class _Conn:
    """Enough of a connection to answer the metadata queries."""

    def __init__(self, rows):
        self.rows = rows

    def execute(self, *args, **kwargs):
        return iter(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def rows():
    return list(GROUPS)


@pytest.fixture
def client(rows):
    engine = types.SimpleNamespace(connect=lambda: _Conn(rows))
    ctx = PortalContext(
        name='p',
        config=types.SimpleNamespace(s3_bucket='b', s3_path=lambda k: k),
        engine=object(), indexes={}, portal=engine)
    init_registry([ctx])

    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=('health',))
    app.include_router(portal_api.router, prefix='/api/portal')
    return TestClient(app)


def test_a_metadata_response_carries_a_tag_and_a_lifetime(client):
    resp = client.get('/p/api/portal/groups')

    assert resp.status_code == 200
    assert resp.headers['etag']
    assert resp.headers['cache-control'] == 'public, max-age=300, must-revalidate'


def test_the_tag_survives_the_nonce_and_the_timing(client):
    # the whole point. Two identical requests differ in `nonce` and in
    # `profile.query`, so a tag over the raw body would never repeat.
    a = client.get('/p/api/portal/groups')
    b = client.get('/p/api/portal/groups')

    assert orjson.loads(a.content)['nonce'] != orjson.loads(b.content)['nonce']
    assert a.headers['etag'] == b.headers['etag']


def test_holding_the_tag_gets_a_304_with_no_body(client):
    etag = client.get('/p/api/portal/groups').headers['etag']

    resp = client.get('/p/api/portal/groups', headers={'If-None-Match': etag})

    assert resp.status_code == 304
    assert resp.content == b''
    # so the client starts a fresh window rather than asking again next time
    assert resp.headers['cache-control'] == 'public, max-age=300, must-revalidate'


def test_changed_data_gets_a_new_tag(client, rows):
    before = client.get('/p/api/portal/groups').headers['etag']

    rows.append(('cvd', 'CVD', 'cardio', 0, 'main'))
    after = client.get('/p/api/portal/groups')

    assert after.headers['etag'] != before
    # and a client holding the old tag is not told it is still current
    assert client.get('/p/api/portal/groups',
                      headers={'If-None-Match': before}).status_code == 200


def test_the_body_is_unchanged_by_the_wrapper(client):
    resp = client.get('/p/api/portal/groups')
    body = orjson.loads(resp.content)

    assert body['count'] == 1
    assert body['data'][0]['name'] == 't2d'
    assert body['data'][0]['default'] is True
    assert 'query' in body['profile']


def test_restrictions_is_never_stored(client, monkeypatch):
    # per-user: it answers off x-bioindex-access-token, so a shared cache
    # holding it would hand one user's restrictions to another
    monkeypatch.setattr(portal_api, 'restrictions', lambda engine, req: [])

    resp = client.get('/p/api/portal/restrictions')

    assert resp.headers['cache-control'] == 'private, no-store'
    assert 'etag' not in resp.headers


def test_query_parameters_still_reach_the_handler(client, monkeypatch):
    # the wrapper takes (req, *args, **kwargs); FastAPI reads the signature
    # through functools.wraps to know what to bind. If that ever stopped
    # working, `q` would silently arrive as None and every portal would be
    # served the unfiltered list.
    seen = {}

    def fake(portal, q=None):
        seen['q'] = q
        return [], 0.0

    monkeypatch.setattr(portal_api, 'query_phenotypes', fake)

    assert client.get('/p/api/portal/phenotypes?q=T2D').status_code == 200
    assert seen['q'] == 'T2D'


def test_every_public_metadata_route_is_tagged():
    # a route added later without the decorator would silently opt out of
    # all of this, so check the router rather than a list of paths
    untagged = []
    for route in portal_api.router.routes:
        tagged = getattr(route.endpoint, '__wrapped__', None) is not None
        if not tagged and route.path != '/restrictions':
            untagged.append(route.path)

    assert untagged == []


@pytest.mark.parametrize('header,expected', [
    ('"abc"', True),
    ('W/"abc"', True),
    ('"x", "abc"', True),
    ('*', True),
    ('"x"', False),
    ('', False),
    (None, False),
])
def test_the_header_grammar_is_honoured(header, expected):
    assert http_cache.if_none_match(header, '"abc"') is expected


def test_the_tag_ignores_only_the_volatile_fields():
    a = {'data': [1], 'count': 1, 'nonce': 'x', 'profile': {'query': 0.1}}
    b = {'data': [1], 'count': 1, 'nonce': 'y', 'profile': {'query': 9.9}}
    c = {'data': [2], 'count': 1, 'nonce': 'x', 'profile': {'query': 0.1}}

    assert http_cache.etag_for(a) == http_cache.etag_for(b)
    assert http_cache.etag_for(a) != http_cache.etag_for(c)
