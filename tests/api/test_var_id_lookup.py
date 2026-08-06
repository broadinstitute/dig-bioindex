"""
/varIdLookup used to 500 on an rsID that isn't in the table.

look_up_var_id returned `response['Items'][0]`, and DynamoDB answers a query
that matched nothing with an empty Items list rather than an error - so any
rsID the mapping doesn't cover raised IndexError out of the handler.
"""
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api import bio
from bioindex.lib import aws
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware

VARIANT = {'rsid': 'rs7903146', 'varid': '10:114758349:C:T'}


@pytest.fixture(autouse=True)
def _registry():
    ctx = PortalContext(
        name='p',
        config=types.SimpleNamespace(
            s3_bucket='bucket', s3_path=lambda k: k,
            variant_dynamodb_table='rsidmapping_v2'),
        engine=object(), indexes={})
    init_registry([ctx])


@pytest.fixture
def client():
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=('health',))
    app.include_router(bio.router, prefix='/api/bio')
    return TestClient(app)


def _dynamo(items):
    """A DynamoDB table whose query returns the given items."""
    return types.SimpleNamespace(query=lambda **kw: {'Items': list(items)})


def test_a_known_rsid_returns_its_variant(client, monkeypatch):
    monkeypatch.setattr(aws, 'dynamo_client',
                        types.SimpleNamespace(Table=lambda t: _dynamo([VARIANT])))

    resp = client.get('/p/api/bio/varIdLookup/rs7903146')

    assert resp.status_code == 200
    body = resp.json()
    assert body['data'] == VARIANT
    assert body['q'] == 'rs7903146'
    assert body['index'] == 'rsidmapping_v2'


def test_an_unknown_rsid_is_a_404(client, monkeypatch):
    # DynamoDB reports "no match" as an empty list, not as an error
    monkeypatch.setattr(aws, 'dynamo_client',
                        types.SimpleNamespace(Table=lambda t: _dynamo([])))

    resp = client.get('/p/api/bio/varIdLookup/rs00000000000')

    assert resp.status_code == 404
    assert 'rs00000000000' in resp.json()['detail']


def test_the_lookup_returns_none_rather_than_raising(monkeypatch):
    monkeypatch.setattr(aws, 'dynamo_client',
                        types.SimpleNamespace(Table=lambda t: _dynamo([])))

    assert aws.look_up_var_id('rs00000000000', 'rsidmapping_v2') is None


def test_the_lookup_returns_the_first_match(monkeypatch):
    other = {'rsid': 'rs7903146', 'varid': '10:114758349:C:G'}
    monkeypatch.setattr(aws, 'dynamo_client',
                        types.SimpleNamespace(Table=lambda t: _dynamo([VARIANT, other])))

    assert aws.look_up_var_id('rs7903146', 'rsidmapping_v2') == VARIANT
