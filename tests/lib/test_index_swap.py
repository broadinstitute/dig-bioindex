"""
What swap_into refuses to do.

A swap repoints a name that is already being served at a different table, so
everything it declines to do is a production index it declines to break. The
cutover itself needs a real database and lives in test_index_swap_mysql.py.
"""
import datetime

import pytest
import sqlalchemy.dialects.mysql
from click.testing import CliRunner

from bioindex.lib.index import Index, _is_built


def _row(id, name, table, schema, built='2026-08-03 12:00:00'):
    return {
        'id': id,
        'table': table,
        'prefix': f'{name}/',
        'schema': schema,
        'built': datetime.datetime.fromisoformat(built) if built else built,
        'compressed': 0,
        # derived the way the (name, arity) unique index derives it
        'arity': schema.count(',') + 1,
    }


class _Engine:
    """
    Answers the row lookup and records anything that would have been written.
    """

    def __init__(self, rows):
        self.rows = rows
        self.written = []

    def _ctx(self):
        engine = self

        class _Ctx:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, statement, params=None):
                sql = str(statement)
                if sql.lstrip().startswith('SELECT'):
                    return _Result(
                        [r for r in engine.rows if r['name'] == params['name']])
                engine.written.append((sql, params))

        return _Ctx()

    connect = _ctx
    begin = _ctx


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


def _engine(*rows):
    return _Engine(list(rows))


def _named(row, name):
    return {**row, 'name': name}


def test_a_name_that_does_not_exist_is_refused():
    engine = _engine(_named(_row(1, 'gene', 'genes', 'name'), 'gene'))

    with pytest.raises(KeyError):
        Index.swap_into(engine, 'gene-tmp', 'gene')

    assert engine.written == []


def test_swapping_a_name_into_itself_is_refused():
    # every statement in the cutover reads as a no-op against a single name
    # until the last, which deletes it: the index ends up unpublished, its
    # keys gone, and its live table returned to the caller to drop
    engine = _engine(_named(_row(1, 'assoc', 'assoc_live', 'phenotype'), 'assoc'))

    with pytest.raises(ValueError, match='into itself'):
        Index.swap_into(engine, 'assoc', 'assoc')

    assert engine.written == []


def test_a_multi_arity_name_is_refused():
    # `gene` is indexed both by name and by name and build. A swap names no
    # arity and its statements match on name alone, so run against a name
    # like this it reads one row arbitrarily and writes every one of them.
    engine = _engine(
        _named(_row(1, 'gene', 'genes_1', 'name'), 'gene'),
        _named(_row(2, 'gene', 'genes_2', 'name,build'), 'gene'),
        _named(_row(3, 'gene-tmp', 'genes_tmp', 'name'), 'gene-tmp'),
    )

    with pytest.raises(ValueError, match='more than one arity'):
        Index.swap_into(engine, 'gene-tmp', 'gene')

    assert engine.written == []


def test_an_unbuilt_temp_index_is_refused():
    engine = _engine(
        _named(_row(1, 'gene', 'genes', 'name'), 'gene'),
        _named(_row(2, 'gene-tmp', 'genes_tmp', 'name', built=None), 'gene-tmp'),
    )

    with pytest.raises(ValueError, match='never been built'):
        Index.swap_into(engine, 'gene-tmp', 'gene')

    assert engine.written == []


def test_a_temp_index_left_at_the_zero_date_is_refused():
    # rows written while create() set `built` = 0 hold the zero date rather
    # than NULL, and the driver hands it back as a string: not None, and not
    # falsy. Testing `is None` alone promotes an index that was never built.
    engine = _engine(
        _named(_row(1, 'gene', 'genes', 'name'), 'gene'),
        _named({**_row(2, 'gene-tmp', 'genes_tmp', 'name'),
                'built': '0000-00-00 00:00:00'}, 'gene-tmp'),
    )

    with pytest.raises(ValueError, match='never been built'):
        Index.swap_into(engine, 'gene-tmp', 'gene')

    assert engine.written == []


def test_swapping_in_a_different_arity_is_refused():
    # the canonical row takes the temp's schema, so this would silently
    # change how many arguments the name accepts and orphan every caller
    engine = _engine(
        _named(_row(1, 'gene', 'genes', 'name'), 'gene'),
        _named(_row(2, 'gene-tmp', 'genes_tmp', 'name,build'), 'gene-tmp'),
    )

    with pytest.raises(ValueError, match='query argument'):
        Index.swap_into(engine, 'gene-tmp', 'gene')

    assert engine.written == []


def test_the_writes_are_keyed_by_id_not_by_name():
    # matching on name would widen these to a row added at another arity
    # between the checks and the cutover - rows the checks never saw
    engine = _engine(
        _named(_row(1, 'gene', 'genes', 'name'), 'gene'),
        _named(_row(2, 'gene-tmp', 'genes_tmp', 'name'), 'gene-tmp'),
    )

    Index.swap_into(engine, 'gene-tmp', 'gene')

    indexes = [(sql, params) for sql, params in engine.written if '__Indexes' in sql]
    assert len(indexes) == 2
    for sql, params in indexes:
        assert 'WHERE `id` = :id' in sql
        assert 'name' not in params

    assert [params['id'] for _sql, params in indexes] == [1, 2]


def test_the_old_table_is_returned_for_the_caller_to_drop():
    # DDL commits implicitly in MySQL, so the drop cannot be part of the
    # cutover; swap_into hands back the name instead of running it
    engine = _engine(
        _named(_row(1, 'gene', 'genes_old', 'name'), 'gene'),
        _named(_row(2, 'gene-tmp', 'genes_new', 'name'), 'gene-tmp'),
    )

    assert Index.swap_into(engine, 'gene-tmp', 'gene') == 'genes_old'
    assert not any('DROP' in sql for sql, _ in engine.written)


def test_the_canonical_keys_go_before_the_temp_keys_are_renamed():
    # __Keys is unique on (index, key) and the temp index covers the same
    # keys, so the other order collides on every one of them
    engine = _engine(
        _named(_row(1, 'gene', 'genes', 'name'), 'gene'),
        _named(_row(2, 'gene-tmp', 'genes_tmp', 'name'), 'gene-tmp'),
    )

    Index.swap_into(engine, 'gene-tmp', 'gene')

    keys = [sql for sql, _ in engine.written if '__Keys' in sql]
    assert keys[0].startswith('DELETE')
    assert keys[1].startswith('UPDATE')


@pytest.mark.parametrize('built,expected', [
    (None, False),
    ('0000-00-00 00:00:00', False),
    (datetime.datetime(2026, 8, 3, 12, 0), True),
    ('2026-08-03 12:00:00', True),
])
def test_is_built(built, expected):
    assert _is_built(built) is expected


def test_the_dropped_table_name_is_quoted_by_the_dialect(monkeypatch):
    """
    A table name is an identifier, so it cannot be bound as a parameter and
    has to be interpolated. Anything the dialect would need to escape has to
    reach the statement escaped, or the drop is malformed - and for a name
    holding a backtick, malformed in a way that names a different table.
    """
    import bioindex.main as main

    dropped = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, statement):
            dropped.append(str(statement))

    class _Engine:
        dialect = sqlalchemy.dialects.mysql.dialect()

        def begin(self):
            return _Conn()

    monkeypatch.setattr(main.migrate, 'migrate', lambda cfg: _Engine())
    monkeypatch.setattr(main.index.Index, 'swap_into',
                        staticmethod(lambda *a: 'weird`name'))

    result = CliRunner().invoke(main.cli_swap, ['tmp', 'canonical', '--yes'], obj=None)

    assert result.exit_code == 0, result.output
    assert dropped == ['DROP TABLE IF EXISTS `weird``name`']
