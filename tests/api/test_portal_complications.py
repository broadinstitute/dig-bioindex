"""
/complications used to 500 for every value of ?q=.

It looked the disease group up with `portal.execute(...)`, and `portal` is an
Engine - which has no `.execute` in SQLAlchemy 2.x. The connection was already
open two lines above.

Behind that, a group the table doesn't know about fell through with an empty
group name and built `FIND_IN_SET(:, ...)`, which is neither a bind parameter
nor valid SQL - so fixing only the first left the route still failing.
"""
import types

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api import portal as portal_api
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware

# name, group, phenotype, withComplication
COMPLICATIONS = [
    ('T2D', 'diabetes', 'CKD', 1),
    ('T2D', 'diabetes', 'Retinopathy', 1),
]

DISEASE_GROUPS = {
    't2d': ('diabetes,metabolic',),
    'empty': ('',),
}


class _Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.queries.append((sql, params))

        if 'DiseaseGroups' in sql:
            return _Result(DISEASE_GROUPS.get(params['name']))

        return iter(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def conn():
    return _Conn(list(COMPLICATIONS))


@pytest.fixture
def client(conn):
    # a SimpleNamespace with no `execute`, because that is what an Engine is:
    # reaching for engine.execute is an AttributeError, not a query
    engine = types.SimpleNamespace(connect=lambda: conn)
    ctx = PortalContext(
        name='p',
        config=types.SimpleNamespace(s3_bucket='b', s3_path=lambda k: k),
        engine=object(), indexes={}, portal=engine)
    init_registry([ctx])

    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=('health',))
    app.include_router(portal_api.router, prefix='/api/portal')
    return TestClient(app)


def test_without_a_filter_every_complication_comes_back(client):
    resp = client.get('/p/api/portal/complications')

    assert resp.status_code == 200
    assert resp.json()['count'] == 1
    assert resp.json()['data'] == [
        {'name': 'T2D', 'phenotypes': {'CKD': 1, 'Retinopathy': 1}}]


def test_filtering_by_a_disease_group_works(client, conn):
    resp = client.get('/p/api/portal/complications?q=t2d')

    assert resp.status_code == 200
    assert resp.json()['count'] == 1

    # the group's own names reached the query as bound parameters
    union = [(sql, params) for sql, params in conn.queries if 'FIND_IN_SET' in sql]
    assert len(union) == 1
    assert union[0][1] == {'diabetes': 'diabetes', 'metabolic': 'metabolic'}


def test_an_unknown_disease_group_is_an_empty_answer(client, conn):
    resp = client.get('/p/api/portal/complications?q=zzz-not-a-group')

    assert resp.status_code == 200
    assert resp.json()['data'] == []
    assert resp.json()['count'] == 0

    # and nothing was asked of the complications tables at all
    assert not any('FIND_IN_SET' in sql for sql, _ in conn.queries)


def test_a_disease_group_with_no_groups_is_an_empty_answer(client, conn):
    # `groups` is a comma-separated set, and an empty one splits to [''] -
    # the value that builds `FIND_IN_SET(:, ...)`
    resp = client.get('/p/api/portal/complications?q=empty')

    assert resp.status_code == 200
    assert resp.json()['count'] == 0
    assert not any('FIND_IN_SET' in sql for sql, _ in conn.queries)


def test_the_group_lookup_runs_on_the_connection(client, conn):
    client.get('/p/api/portal/complications?q=t2d')

    lookups = [(sql, params) for sql, params in conn.queries if 'DiseaseGroups' in sql]
    assert lookups == [
        ('SELECT `groups` FROM DiseaseGroups WHERE `name` = :name', {'name': 't2d'})]


def test_an_empty_q_is_the_same_as_no_filter(client, conn):
    resp = client.get('/p/api/portal/complications?q=')

    assert resp.status_code == 200
    assert resp.json()['count'] == 1
    assert not any('DiseaseGroups' in sql for sql, _ in conn.queries)
