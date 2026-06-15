import botocore.exceptions

from bioindex.lib import s3


class _FakeClient:
    def __init__(self, head=None, raise_code=None, get=None):
        self._head = head
        self._raise_code = raise_code
        self._get = get

    def head_object(self, Bucket, Key):
        if self._raise_code is not None:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": self._raise_code, "Message": "x"}}, "HeadObject"
            )
        return self._head

    def get_object(self, Bucket, Key):
        return self._get


def test_head_object_returns_response(monkeypatch):
    monkeypatch.setattr(s3, "s3_client", _FakeClient(head={"ETag": '"abc"'}))
    meta = s3.head_object("bkt", "raw/x.json.gz")
    assert meta["ETag"] == '"abc"'


def test_head_object_returns_none_when_missing(monkeypatch):
    monkeypatch.setattr(s3, "s3_client", _FakeClient(raise_code="404"))
    assert s3.head_object("bkt", "raw/missing.json.gz") is None


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


def test_read_object_with_etag_returns_bytes_and_etag(monkeypatch):
    fake = _FakeClient(get={"Body": _Body(b"PAYLOAD"), "ETag": '"v1"'})
    monkeypatch.setattr(s3, "s3_client", fake)
    body, etag = s3.read_object_with_etag("bkt", "raw/x.json.gz")
    assert body == b"PAYLOAD"
    assert etag == '"v1"'
