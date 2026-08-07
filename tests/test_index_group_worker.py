import importlib.util, pathlib
import os
from unittest.mock import MagicMock
import pytest

_spec = importlib.util.spec_from_file_location(
    'index_group', pathlib.Path('batch-index-files/index_group.py'))
index_group = importlib.util.module_from_spec(_spec)


@pytest.fixture(autouse=True)
def _isolate_bioindex_env(monkeypatch):
    """index_group.main() writes BIOINDEX_* straight into os.environ.

    monkeypatch does not track those direct assignments, so without this the values
    outlive the test and later tests that build a Config from the environment pick up
    a bogus bucket/secret. Naming each key here makes monkeypatch record its original
    state and restore it on teardown, whatever main() does in between.
    """
    for key in ('BIOINDEX_S3_BUCKET', 'BIOINDEX_RDS_SECRET',
                'BIOINDEX_BIO_SCHEMA', 'BIOINDEX_S3_SUBDIR'):
        monkeypatch.setenv(key, os.environ.get(key, ''))
        monkeypatch.delenv(key, raising=False)


def _objs(keys):
    return [{'Key': k, 'Size': 10, 'ETag': '"v"'} for k in keys]


def test_select_chunk_picks_the_right_slice():
    _spec.loader.exec_module(index_group)
    objs = _objs([f'p/part-{i}.json.gz' for i in range(5)])
    chunk = index_group.select_chunk(objs, chunk_index=1, chunk_count=3,
                                     group_size=2, group_max_bytes=10 ** 9, expected_total=5)
    assert [o['Key'] for o in chunk] == ['p/part-2.json.gz', 'p/part-3.json.gz']


def test_select_chunk_drift_guard_on_total_mismatch():
    _spec.loader.exec_module(index_group)
    objs = _objs([f'p/part-{i}.json.gz' for i in range(4)])
    with pytest.raises(AssertionError, match='listing drift'):
        index_group.select_chunk(objs, chunk_index=0, chunk_count=2,
                                 group_size=2, group_max_bytes=10 ** 9, expected_total=5)


def test_select_chunk_drift_guard_on_chunk_count_mismatch():
    _spec.loader.exec_module(index_group)
    objs = _objs([f'p/part-{i}.json.gz' for i in range(4)])  # group_size=2 -> 2 chunks
    with pytest.raises(AssertionError, match='chunk-count drift'):
        index_group.select_chunk(objs, chunk_index=0, chunk_count=3,
                                 group_size=2, group_max_bytes=10 ** 9, expected_total=4)


def test_select_chunk_drift_guard_on_chunk_index_out_of_range():
    _spec.loader.exec_module(index_group)
    objs = _objs([f'p/part-{i}.json.gz' for i in range(4)])  # group_size=2 -> 2 chunks
    with pytest.raises(AssertionError, match='out of range'):
        index_group.select_chunk(objs, chunk_index=5, chunk_count=2,
                                 group_size=2, group_max_bytes=10 ** 9, expected_total=4)


def test_index_chunk_skips_current_and_indexes_others():
    _spec.loader.exec_module(index_group)
    idx = MagicMock()
    idx.index_object.side_effect = lambda engine, bucket, obj: (obj['Key'], iter([{'x': 1}]))
    db_keys = {'p/a.json.gz': {'id': 1, 'version': 'v'}}
    chunk = [{'Key': 'p/a.json.gz', 'Size': 10, 'ETag': '"v"'},
             {'Key': 'p/b.json.gz', 'Size': 10, 'ETag': '"v"'}]
    index_group.index_chunk(idx, MagicMock(), 'bkt', db_keys, chunk)
    assert idx.index_object.call_count == 1
    assert idx.insert_records_iter.call_count == 1
    assert idx.index_object.call_args[0][2]['Key'] == 'p/b.json.gz'


def test_main_sets_subdir_env_and_lists_qualified_prefix(monkeypatch):
    _spec.loader.exec_module(index_group)
    monkeypatch.delenv('BIOINDEX_S3_SUBDIR', raising=False)
    captured = {}

    class FakeConfig:
        def s3_path(self, path):
            sub = os.environ.get('BIOINDEX_S3_SUBDIR')
            return f'{sub}/{path}' if sub else path

    monkeypatch.setattr(index_group, 'Config', lambda: FakeConfig())
    monkeypatch.setattr(index_group, 'migrate', lambda config: object())
    fake_index = MagicMock()
    fake_index.lookup_keys.return_value = {}
    fake_index.index_object.side_effect = lambda engine, bucket, obj: (obj['Key'], iter([]))
    monkeypatch.setattr(
        index_group.Index, 'lookup', staticmethod(lambda engine, name, arity: fake_index)
    )

    def fake_list(bucket, s3_path, prefer_compressed):
        captured['s3_path'] = s3_path
        return [{'Key': f'{s3_path}part-0.json.gz', 'Size': 10, 'ETag': '"v"'}]

    monkeypatch.setattr(index_group, 'list_index_objects', fake_list)
    monkeypatch.setattr(index_group, '_chunk_objects', lambda objs, gs, gmb: [objs])

    index_group.main.callback(
        index_name='idx', arity='1', bucket='bkt', rds_secret='sec', rds_schema='sch',
        s3_subdir='bioindex', prefix='pre/', prefer_compressed=1, chunk_index=0, chunk_count=1,
        group_size=2, group_max_bytes=1000000, expected_total=1,
    )
    assert os.environ.get('BIOINDEX_S3_SUBDIR') == 'bioindex'
    assert captured['s3_path'] == 'bioindex/pre/'


def test_main_sentinel_subdir_leaves_env_unset_and_lists_bare_prefix(monkeypatch):
    # Non-subdir portals arrive as the GROUP_NO_SUBDIR sentinel (Batch can't carry ''); the
    # worker must NOT set the env, so Config.s3_path stays bare and lookup_keys filters bare.
    from bioindex.lib.aws import GROUP_NO_SUBDIR
    _spec.loader.exec_module(index_group)
    monkeypatch.delenv('BIOINDEX_S3_SUBDIR', raising=False)
    captured = {}

    class FakeConfig:
        def s3_path(self, path):
            sub = os.environ.get('BIOINDEX_S3_SUBDIR')
            return f'{sub}/{path}' if sub else path

    monkeypatch.setattr(index_group, 'Config', lambda: FakeConfig())
    monkeypatch.setattr(index_group, 'migrate', lambda config: object())
    fake_index = MagicMock()
    fake_index.lookup_keys.return_value = {}
    fake_index.index_object.side_effect = lambda engine, bucket, obj: (obj['Key'], iter([]))
    monkeypatch.setattr(
        index_group.Index, 'lookup', staticmethod(lambda engine, name, arity: fake_index)
    )

    def fake_list(bucket, s3_path, prefer_compressed):
        captured['s3_path'] = s3_path
        return [{'Key': f'{s3_path}part-0.json.gz', 'Size': 10, 'ETag': '"v"'}]

    monkeypatch.setattr(index_group, 'list_index_objects', fake_list)
    monkeypatch.setattr(index_group, '_chunk_objects', lambda objs, gs, gmb: [objs])

    index_group.main.callback(
        index_name='idx', arity='1', bucket='bkt', rds_secret='sec', rds_schema='sch',
        s3_subdir=GROUP_NO_SUBDIR, prefix='pre/', prefer_compressed=1, chunk_index=0, chunk_count=1,
        group_size=2, group_max_bytes=1000000, expected_total=1,
    )
    assert os.environ.get('BIOINDEX_S3_SUBDIR') is None
    assert captured['s3_path'] == 'pre/'
