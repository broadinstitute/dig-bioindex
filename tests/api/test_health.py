"""
/health and /ready are what the load balancer polls, so they must answer
without a portal prefix and must not be resolved as a portal name.
"""
import types
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import bioindex.server as server
from bioindex.lib.portal_registry import init_registry


def _ctx(name, engine):
    return types.SimpleNamespace(name=name, config=object(), engine=engine,
                                 indexes={}, portal=None, gql_schema=None)


def _engine(fails=False):
    engine = MagicMock()
    if fails:
        engine.connect.side_effect = OSError('connection refused')
    return engine


@pytest.fixture
def client():
    return TestClient(server.app)


def test_health_is_up_without_touching_the_registry(client):
    init_registry([])
    resp = client.get('/health')
    assert resp.status_code == 200
    assert resp.json() == {'status': 'ok'}


def test_ready_reports_every_portal(client):
    init_registry([_ctx('amp', _engine()), _ctx('cfde', _engine())])
    resp = client.get('/ready')
    assert resp.status_code == 200
    assert resp.json() == {'status': 'ok', 'portals': {'amp': 'ok', 'cfde': 'ok'}}


def test_ready_stays_in_rotation_when_one_portal_is_down(client):
    init_registry([_ctx('amp', _engine()), _ctx('cfde', _engine(fails=True))])
    resp = client.get('/ready')

    # the healthy portal is still servable, so the task keeps taking traffic
    assert resp.status_code == 200
    body = resp.json()
    assert body['portals']['amp'] == 'ok'
    assert body['portals']['cfde'].startswith('error:')


def test_ready_is_503_when_every_portal_is_down(client):
    init_registry([_ctx('amp', _engine(fails=True)), _ctx('cfde', _engine(fails=True))])
    resp = client.get('/ready')
    assert resp.status_code == 503
    assert resp.json()['status'] == 'unhealthy'


def test_probes_are_not_resolved_as_portal_names(client):
    # without the reserved prefixes the middleware would look for a portal
    # called 'health' and answer 404
    init_registry([_ctx('amp', _engine())])
    assert client.get('/health').status_code == 200
    assert client.get('/ready').status_code == 200
