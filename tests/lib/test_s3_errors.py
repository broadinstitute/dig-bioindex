"""
An object that is absent and an object we are not allowed to read are
different answers, and S3 reports them as the same kind of exception. These
pin the distinction, because collapsing it turns a broken deployment into a
quiet 404 on every file.
"""
import types

import botocore.exceptions
import pytest

from bioindex.lib import s3


def _client_error(code, status, operation='HeadObject'):
    return botocore.exceptions.ClientError(
        {
            'Error': {'Code': code, 'Message': 'testing'},
            'ResponseMetadata': {'HTTPStatusCode': status},
        },
        operation,
    )


@pytest.fixture
def raises(monkeypatch):
    """Make the next s3 call fail with a given error."""
    def _raises(error):
        def boom(**kwargs):
            raise error
        monkeypatch.setattr(s3.s3_client, 'head_object', boom)
        monkeypatch.setattr(s3.s3_client, 'get_object', boom)
    return _raises


# A missing object looks different depending on how you asked. HEAD replies
# with no body, so botocore has no error code to parse and falls back to the
# status; GET replies with one. Keying off the code alone would miss the HEAD
# case entirely - and that is the common one here, since we HEAD first.
@pytest.mark.parametrize('code', ['404', 'NoSuchKey'])
def test_head_of_a_missing_object_is_none(raises, code):
    raises(_client_error(code, 404))

    assert s3.head_object('bucket', 'gone.csv') is None


@pytest.mark.parametrize('code,status', [
    ('AccessDenied', 403),
    ('SlowDown', 503),
    ('InternalError', 500),
])
def test_head_does_not_report_a_failure_as_a_missing_object(raises, code, status):
    # if the task role loses s3:GetObject, every file would 404 and look like
    # a data problem rather than a permissions one
    raises(_client_error(code, status))

    with pytest.raises(botocore.exceptions.ClientError):
        s3.head_object('bucket', 'present.csv')


def test_read_of_an_object_that_vanished_is_none(raises):
    raises(_client_error('NoSuchKey', 404, operation='GetObject'))

    assert s3.read_object_with_etag('bucket', 'gone.csv') is None


def test_read_does_not_report_a_failure_as_a_missing_object(raises):
    raises(_client_error('AccessDenied', 403, operation='GetObject'))

    with pytest.raises(botocore.exceptions.ClientError):
        s3.read_object_with_etag('bucket', 'present.csv')


def test_a_successful_read_returns_the_body_and_the_tag(monkeypatch):
    monkeypatch.setattr(s3.s3_client, 'get_object', lambda **kwargs: {
        'Body': types.SimpleNamespace(read=lambda: b'bytes'),
        'ETag': '"tag"',
    })

    assert s3.read_object_with_etag('bucket', 'here.csv') == (b'bytes', '"tag"')
