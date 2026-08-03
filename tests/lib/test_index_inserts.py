"""
The two bulk-load paths, and the CSV they hand to LOAD DATA.

insert_records takes a sequence, insert_records_iter takes an iterator and
never builds one. Both go through _load_csv, so both must produce the same
file for the same records - a difference here would be a silent data
difference between the local and the batch build paths.
"""
import csv
import glob
import os
import re
import tempfile
import types

import pytest
import sqlalchemy.exc

from bioindex.lib.index import Index

RECORDS = [
    {'chromosome': '1', 'position': 100, 'value': 'a'},
    {'chromosome': '1', 'position': 200, 'value': 'b'},
    {'chromosome': '2', 'position': 300, 'value': 'c'},
]


class _Engine:
    """
    Captures what LOAD DATA was asked to load, reading the CSV back while it
    still exists. Optionally fails the first few attempts.
    """

    def __init__(self, errors=()):
        self.errors = list(errors)
        self.statements = []
        self.loaded = None

    def begin(self):
        engine = self

        class _Ctx:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, statement):
                sql = str(statement)
                engine.statements.append(sql)

                if engine.errors:
                    raise engine.errors.pop(0)

                path = re.search(r"LOAD DATA LOCAL INFILE '([^']+)'", sql).group(1)
                with open(path) as fp:
                    engine.loaded = list(csv.DictReader(fp))

        return _Ctx()


def _mysql_error(errno, message):
    """
    An OperationalError shaped the way the driver really raises one: the
    MySQL error number is on the wrapped DBAPI exception, not on `.code`
    (which is SQLAlchemy's own 'e3q8' for this class, whatever went wrong).
    """
    orig = Exception(errno, message)
    orig.args = (errno, message)

    return sqlalchemy.exc.OperationalError('LOAD DATA ...', {}, orig)


def _deadlock():
    return _mysql_error(1213, 'Deadlock found when trying to get lock')


@pytest.fixture
def slept(monkeypatch):
    """Record the backoff instead of actually waiting."""
    waits = []
    monkeypatch.setattr('bioindex.lib.index.time.sleep', waits.append)
    return waits


@pytest.fixture
def index(slept):
    idx = Index.__new__(Index)
    idx.table = types.SimpleNamespace(name='test_table')
    return idx


@pytest.fixture(autouse=True)
def _no_stray_temp_files():
    """Any CSV left behind by these tests is a leak."""
    before = set(glob.glob(os.path.join(tempfile.gettempdir(), 'tmp*')))
    yield
    leaked = set(glob.glob(os.path.join(tempfile.gettempdir(), 'tmp*'))) - before
    assert not leaked, f'temp files left behind: {leaked}'


def test_the_iterator_path_loads_every_record(index):
    engine = _Engine()

    index.insert_records_iter(engine, iter(RECORDS))

    assert engine.loaded == [{k: str(v) for k, v in r.items()} for r in RECORDS]


def test_both_paths_produce_the_same_csv(index):
    from_list = _Engine()
    from_iter = _Engine()

    index.insert_records(from_list, list(RECORDS))
    index.insert_records_iter(from_iter, iter(RECORDS))

    assert from_iter.loaded == from_list.loaded
    # and the same column list, in the same order
    columns = lambda e: re.search(r'\(([^)]*)\)\s*$', e.statements[0]).group(1)
    assert columns(from_iter) == columns(from_list)


def test_the_iterator_is_never_materialized(index):
    # a generator that refuses to be listed: len() and slicing both fail on
    # it, so anything that tries to build a sequence blows up here
    consumed = []

    def stream():
        for r in RECORDS:
            consumed.append(r['position'])
            yield r

    engine = _Engine()
    index.insert_records_iter(engine, stream())

    assert consumed == [100, 200, 300]
    assert len(engine.loaded) == 3


def test_an_empty_iterator_loads_nothing(index):
    engine = _Engine()

    index.insert_records_iter(engine, iter([]))

    assert engine.statements == []


def test_a_deadlock_is_retried(index):
    # parallel builds lock each other out of the table; 1213 is transient
    engine = _Engine(errors=[_deadlock(), _deadlock()])

    index.insert_records_iter(engine, iter(RECORDS))

    assert len(engine.statements) == 3
    assert len(engine.loaded) == 3


def test_a_persistent_failure_is_raised(index):
    engine = _Engine(errors=[_deadlock()] * 5)

    with pytest.raises(sqlalchemy.exc.OperationalError):
        index.insert_records_iter(engine, iter(RECORDS))

    assert len(engine.statements) == 5


def test_the_csv_is_removed_when_the_load_fails(index):
    # covered by the autouse leak check, but state it directly
    engine = _Engine(errors=[_deadlock()] * 5)

    with pytest.raises(sqlalchemy.exc.OperationalError):
        index.insert_records_iter(engine, iter(RECORDS))


def test_the_csv_is_removed_when_a_record_cannot_be_written(index):
    # a record with a field the header doesn't have makes DictWriter raise
    # mid-write, after the temp file exists
    engine = _Engine()
    bad = iter([RECORDS[0], {'chromosome': '1', 'unexpected': 'x'}])

    with pytest.raises(ValueError):
        index.insert_records_iter(engine, bad)

    assert engine.statements == []


def test_a_deadlock_actually_backs_off(index, slept):
    # `error.code` is SQLAlchemy's own 'e3q8' for this class, never a MySQL
    # errno, so keying the backoff off it means five instant retries into a
    # lock that is still held - no wait at all where the wait is the point
    engine = _Engine(errors=[_deadlock(), _deadlock()])

    index.insert_records_iter(engine, iter(RECORDS))

    assert slept == [1, 1]


def test_a_non_deadlock_failure_does_not_back_off(index, slept):
    engine = _Engine(errors=[_mysql_error(1146, "Table 'x' doesn't exist")] * 5)

    with pytest.raises(sqlalchemy.exc.OperationalError):
        index.insert_records_iter(engine, iter(RECORDS))

    assert slept == []
