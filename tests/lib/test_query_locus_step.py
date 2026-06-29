from unittest.mock import MagicMock

from bioindex.lib import query
from bioindex.lib.index import Index


class _FakeResult:
    def fetchall(self):
        return []


class _FakeConn:
    """Captures the bound params _run_query builds; yields no rows."""
    def __init__(self, sink):
        self._sink = sink

    def execute(self, statement, params=None):
        self._sink['params'] = params or {}
        return _FakeResult()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _params_for(schema_str, varid):
    idx = Index("vb", "VB", "associations/variant/", schema_str, "2026-01-01", True)
    sink = {}
    engine = MagicMock()
    engine.connect.return_value = _FakeConn(sink)
    query._run_query(MagicMock(), engine, idx, (varid,), None)
    return sink['params']


def test_default_step_buckets_at_20000():
    p = _params_for("varId=$chr:$pos", "1:45123:A:G")
    assert p['start_pos'] == 40000
    assert p['end_pos'] == 40000


def test_custom_step_buckets_at_250():
    p = _params_for("varId=$chr:$pos;locus_step=250", "1:45123:A:G")
    assert p['start_pos'] == 45000
    assert p['end_pos'] == 45000
