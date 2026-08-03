import gzip
import types

import botocore.exceptions
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bioindex.api import raw
from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry
from bioindex.middleware.portal import PortalResolveMiddleware

ETAG = '"a1b2c3"'
BODY = b'chr,pos\n1,100\n'


@pytest.fixture(autouse=True)
def _registry():
    ctx = PortalContext(name='p', config=types.SimpleNamespace(
        s3_bucket='bucket', s3_path=lambda k: k), engine=object(), indexes={})
    init_registry([ctx])


@pytest.fixture
def s3(monkeypatch):
    """Stub S3 with one object; a test can make it absent or change its tag."""
    state = types.SimpleNamespace(etag=ETAG, body=BODY, present=True, reads=0, heads=0)

    def head_object(bucket, path):
        state.heads += 1
        return {'ETag': state.etag} if state.present else None

    def read_object_with_etag(bucket, path):
        state.reads += 1
        return state.body, state.etag

    monkeypatch.setattr(raw.s3, 'head_object', head_object)
    monkeypatch.setattr(raw.s3, 'read_object_with_etag', read_object_with_etag)
    return state


@pytest.fixture
def client():
    app = FastAPI()
    app.add_middleware(PortalResolveMiddleware, reserved_prefixes=('health',))
    app.include_router(raw.router, prefix='/api/raw')
    return TestClient(app)


def test_a_first_read_returns_the_body_and_its_tag(s3, client):
    resp = client.get('/p/api/raw/file/data.csv')

    assert resp.status_code == 200
    assert resp.content == BODY
    assert resp.headers['etag'] == ETAG
    assert resp.headers['cache-control'] == 'public, no-cache, must-revalidate'


def test_a_client_holding_the_tag_gets_a_304_with_no_body(s3, client):
    resp = client.get('/p/api/raw/file/data.csv', headers={'If-None-Match': ETAG})

    assert resp.status_code == 304
    assert resp.content == b''
    assert resp.headers['etag'] == ETAG
    # the point of the whole exercise: the object was never fetched
    assert s3.reads == 0


def test_a_stale_tag_gets_the_new_body(s3, client):
    resp = client.get('/p/api/raw/file/data.csv', headers={'If-None-Match': '"old"'})

    assert resp.status_code == 200
    assert resp.content == BODY
    assert s3.reads == 1


def test_a_changed_object_is_not_served_from_a_stale_tag(s3, client):
    assert client.get('/p/api/raw/file/data.csv').headers['etag'] == ETAG

    s3.etag, s3.body = '"changed"', b'new bytes\n'
    resp = client.get('/p/api/raw/file/data.csv', headers={'If-None-Match': ETAG})

    assert resp.status_code == 200
    assert resp.content == b'new bytes\n'


@pytest.mark.parametrize('header', [
    f'W/{ETAG}',                    # weak validator
    f'"other", {ETAG}',             # a list, ours second
    '*',                            # wildcard matches anything present
])
def test_the_header_grammar_is_honoured(s3, client, header):
    assert client.get('/p/api/raw/file/data.csv',
                      headers={'If-None-Match': header}).status_code == 304


@pytest.mark.parametrize('header', ['', '"nope"', 'W/"nope"'])
def test_a_non_matching_header_is_not_a_304(s3, client, header):
    assert client.get('/p/api/raw/file/data.csv',
                      headers={'If-None-Match': header}).status_code == 200


def test_the_tag_returned_is_the_one_the_body_came_at(s3, client, monkeypatch):
    # the object is replaced between the HEAD and the GET, so the tag HEAD
    # saw no longer describes the bytes being sent. Labelling them with it
    # would have the client cache new content under the old version.
    monkeypatch.setattr(raw.s3, 'read_object_with_etag',
                        lambda bucket, path: (b'newer bytes\n', '"newer"'))

    resp = client.get('/p/api/raw/file/data.csv')

    assert resp.status_code == 200
    assert resp.content == b'newer bytes\n'
    assert resp.headers['etag'] == '"newer"'


def test_a_missing_object_is_a_404(s3, client):
    s3.present = False

    assert client.get('/p/api/raw/file/gone.csv').status_code == 404
    assert s3.reads == 0


def test_an_object_deleted_between_the_head_and_the_get_is_a_404(s3, client, monkeypatch):
    # HEAD found it, then it was deleted before we read it. That is a real
    # race and the honest answer is 404 - not the 500 an unhandled
    # NoSuchKey would produce.
    monkeypatch.setattr(raw.s3, 'read_object_with_etag', lambda bucket, path: None)

    assert client.get('/p/api/raw/file/racy.csv').status_code == 404


def test_a_read_failure_is_not_disguised_as_a_404(s3, client, monkeypatch):
    # "we are not allowed to read it" is not "it isn't there". The access
    # middleware turns the raised error into a 500, which is the point: a
    # 404 here would read as a data problem and hide a broken deployment.
    def boom(bucket, path):
        raise botocore.exceptions.ClientError(
            {'Error': {'Code': 'AccessDenied'}, 'ResponseMetadata': {'HTTPStatusCode': 403}},
            'GetObject')

    monkeypatch.setattr(raw.s3, 'read_object_with_etag', boom)

    assert client.get('/p/api/raw/file/denied.csv').status_code == 500


@pytest.mark.parametrize('name,content_type', [
    ('data.csv', 'text/csv'),
    ('notes.txt', 'text/plain'),
    ('blob.unknownext', 'application/octet-stream'),
])
def test_the_content_type_still_comes_from_the_name(s3, client, name, content_type):
    resp = client.get(f'/p/api/raw/file/{name}')

    assert resp.headers['content-type'].split(';')[0] == content_type


def test_a_gzipped_name_keeps_its_content_encoding(s3, client):
    # the object is stored gzipped; the header says so and the bytes go out
    # untouched, so the client is the one that inflates them
    s3.body = gzip.compress(BODY)

    resp = client.get('/p/api/raw/file/data.csv.gz')

    assert resp.headers['content-encoding'] == 'gzip'
    assert resp.content == BODY
