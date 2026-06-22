"""
query.match uses keyset pagination: it orders by the BINARY (byte) value of the
matched column, resumes strictly after a given key via `BINARY col > :__after`,
and bounds the page with LIMIT. BINARY (not the column's case-insensitive
collation) makes the order total and case-sensitive so /cont resumes without
duplicate keys. These are white-box guards on the generated SQL (the real
collation behaviour only reproduces against MySQL, which unit tests can't run).
"""
from unittest.mock import MagicMock

from bioindex.lib import query


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    """Captures the SQL text + params query.match builds; yields no rows."""
    def __init__(self, sink):
        self._sink = sink

    def execute(self, statement, params=None):
        self._sink['sql'] = str(statement)
        self._sink['params'] = params or {}
        return _FakeResult([])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _capture(key_columns, q, after=None, limit=None):
    sink = {}
    engine = MagicMock()
    engine.connect.return_value = _FakeConn(sink)
    index = MagicMock()
    index.built = True
    index.table = "my_table"
    index.schema.key_columns = key_columns
    query.match(MagicMock(), engine, index, q, after=after, limit=limit)
    return sink


def test_match_orders_by_binary_value():
    assert "ORDER BY BINARY" in _capture(["name"], ["A"])['sql'].upper()


def test_match_orders_by_binary_with_leading_keys():
    assert "ORDER BY BINARY" in _capture(["phenotype", "gene"], ["T2D", "TCF"])['sql'].upper()


def test_match_keyset_cursor_compares_binary():
    sink = _capture(["name"], ["A"], after="AASS")
    sql = sink['sql'].upper()
    assert "BINARY `NAME` > :__AFTER" in sql, sql
    assert sink['params'].get('__after') == "AASS"


def test_match_no_cursor_without_after():
    assert ":__AFTER" not in _capture(["name"], ["A"])['sql'].upper()


def test_match_applies_limit():
    assert "LIMIT 50" in _capture(["name"], ["A"], limit=50)['sql'].upper()
