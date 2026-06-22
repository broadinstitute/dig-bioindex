"""
Regression: query.match must ORDER BY the BINARY (byte) value of the matched
column. The stateless /cont resume skips already-returned keys with a Python
`dropwhile(k <= last_key)` — a case-sensitive, codepoint comparison. If the SQL
orders by the column's default (case-insensitive) collation — or, with no
ORDER BY, the implicit index-scan order — the two orderings disagree and page 2+
re-return keys already seen (duplicate keys). Ordering by BINARY makes the SQL
order match the Python cursor.

This is a white-box guard on the generated SQL (the actual duplication only
reproduces against a real MySQL collation, which unit tests can't exercise).
"""
from unittest.mock import MagicMock

from bioindex.lib import query


class _FakeConn:
    """Captures the SQL text query.match builds, yields no rows."""
    def __init__(self, sink):
        self._sink = sink

    def execution_options(self, **kwargs):
        return self

    def execute(self, statement, params=None):
        self._sink['sql'] = str(statement)
        return iter(())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _capture_match_sql(key_columns, q):
    sink = {}
    engine = MagicMock()
    engine.connect.return_value = _FakeConn(sink)
    index = MagicMock()
    index.built = True
    index.table = "my_table"
    index.schema.key_columns = key_columns
    # consume the generator so the SQL is built + "executed"
    list(query.match(MagicMock(), engine, index, q))
    return sink['sql']


def test_match_orders_by_binary_value():
    sql = _capture_match_sql(["name"], ["A"]).upper()
    assert "ORDER BY BINARY" in sql, sql


def test_match_orders_by_binary_with_leading_keys():
    # multi-column key: leading columns are exact, last is the matched column
    sql = _capture_match_sql(["phenotype", "gene"], ["T2D", "TCF"]).upper()
    assert "ORDER BY BINARY" in sql, sql
