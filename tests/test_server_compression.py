"""
Response compression on the real app wiring in server.py.

Cloudflare asks the origin for gzip; a JSON body that crosses that hop
uncompressed is a cost every time. The contract here is that a body worth
compressing is compressed when asked, and that nothing which must not be
touched - already-encoded objects, empty bodies, tiny bodies - is.

httpx sends Accept-Encoding by itself and decodes transparently, so the
tests name the header explicitly either way and read the wire bytes raw.
"""
import gzip
import os
import types
from unittest.mock import MagicMock, patch

import orjson
import pytest

os.environ.setdefault("BIOINDEX_TOKEN_SIGNING_KEY", "00" * 32)

from bioindex.api import raw  # noqa: E402
from bioindex.lib.portal_context import PortalContext  # noqa: E402
from bioindex.lib.portal_registry import init_registry  # noqa: E402

import bioindex.server as server  # noqa: E402

from fastapi.testclient import TestClient  # noqa: E402

GZIP = {'Accept-Encoding': 'gzip'}
IDENTITY = {'Accept-Encoding': 'identity'}

# the raw route serves this back as-is: it is already gzip on S3
RAW_ETAG = '"a1b2c3"'
RAW_BODY = gzip.compress(b'chr,pos\n' + b'1,100\n' * 500)


def _stub_index(name):
    return types.SimpleNamespace(
        name=name,
        built=True,
        schema=types.SimpleNamespace(
            arity=1, key_columns=['varId'], has_locus=False),
        compressed=False,
    )


# enough indexes that the listing is well past any sensible minimum size
_MANY_INDEXES = {(f'index-{n:03d}', 1): _stub_index(f'index-{n:03d}') for n in range(60)}

_STUB_CTX = PortalContext(
    name='p',
    config=types.SimpleNamespace(s3_bucket='bucket', s3_path=lambda k: k),
    engine=MagicMock(name='stub_engine'),
    indexes={('variants', 1): _stub_index('variants')},
)

client = TestClient(server.app)


@pytest.fixture(autouse=True)
def _registry():
    _STUB_CTX.indexes = {('variants', 1): _stub_index('variants')}
    init_registry([_STUB_CTX])


@pytest.fixture
def many_indexes():
    with patch('bioindex.api.bio._load_indexes', lambda ctx: dict(_MANY_INDEXES)):
        yield


@pytest.fixture
def raw_s3(monkeypatch):
    monkeypatch.setattr(raw.s3, 'head_object', lambda bucket, path: {'ETag': RAW_ETAG})
    monkeypatch.setattr(raw.s3, 'read_object_with_etag', lambda bucket, path: (RAW_BODY, RAW_ETAG))


def _wire(method, url, headers):
    """The response as it left the server: status, headers, undecoded body."""
    with client.stream(method, url, headers=headers) as resp:
        body = b''.join(resp.iter_raw())
        return resp.status_code, resp.headers, body


def test_a_large_json_body_is_gzipped_when_the_client_asks(many_indexes):
    plain = client.get('/p/api/bio/indexes', headers=IDENTITY)
    status, headers, body = _wire('GET', '/p/api/bio/indexes', GZIP)

    assert status == 200
    assert headers['content-encoding'] == 'gzip'
    assert 'accept-encoding' in headers['vary'].lower()
    assert int(headers['content-length']) == len(body) < len(plain.content)

    # the same answer, only smaller; the nonce is the one field free to differ
    decoded = orjson.loads(gzip.decompress(body))
    expected = plain.json()
    decoded.pop('nonce')
    expected.pop('nonce')
    assert decoded == expected


def test_a_client_that_does_not_ask_gets_the_body_as_is(many_indexes):
    status, headers, body = _wire('GET', '/p/api/bio/indexes', IDENTITY)

    assert status == 200
    assert 'content-encoding' not in headers
    assert int(headers['content-length']) == len(body)
    assert body.startswith(b'{')


def test_a_small_body_is_not_worth_encoding():
    status, headers, body = _wire('GET', '/health', GZIP)

    assert status == 200
    assert 'content-encoding' not in headers
    assert body.startswith(b'{')


def test_an_object_that_is_already_gzip_is_passed_through_untouched(raw_s3):
    status, headers, body = _wire('GET', '/p/api/raw/file/data.csv.gz', GZIP)

    assert status == 200
    # exactly one encoding, the object's own - never gzip-of-gzip
    assert headers.get_list('content-encoding') == ['gzip']
    assert body == RAW_BODY
    # the tag is over the stored bytes, and those are what was sent
    assert headers['etag'] == RAW_ETAG


def test_the_raw_tag_does_not_depend_on_the_encoding_asked_for(raw_s3):
    asked = _wire('GET', '/p/api/raw/file/data.csv.gz', GZIP)[1]['etag']
    plain = _wire('GET', '/p/api/raw/file/data.csv.gz', IDENTITY)[1]['etag']

    assert asked == plain == RAW_ETAG


def test_a_304_stays_empty_and_unencoded(raw_s3):
    status, headers, body = _wire(
        'GET', '/p/api/raw/file/data.csv.gz', {**GZIP, 'If-None-Match': RAW_ETAG})

    assert status == 304
    assert body == b''
    assert 'content-encoding' not in headers
    assert headers['etag'] == RAW_ETAG


def test_a_head_query_keeps_its_declared_length():
    reader = types.SimpleNamespace(bytes_total=123456789)
    with patch('bioindex.api.bio.query.fetch', lambda *a, **k: reader):
        status, headers, body = _wire('HEAD', '/p/api/bio/query/variants?q=rs123', GZIP)

    assert status == 200
    assert body == b''
    assert headers['content-length'] == '123456789'
    assert 'content-encoding' not in headers
