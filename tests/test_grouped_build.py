from unittest.mock import MagicMock, patch

import bioindex.lib.index as index_mod
import pytest
from bioindex.lib.index import Index, _chunk_objects


def _objs(sizes):
    return [{'Key': f'p/part-{i}.json.gz', 'Size': s, 'ETag': '"v"'} for i, s in enumerate(sizes)]


def test_chunk_objects_bounds_by_count_and_bytes():
    objs = _objs([10] * 10)
    # max 3 files per group -> 4 groups (3,3,3,1)
    groups = _chunk_objects(objs, max_files=3, max_bytes=10 ** 9)
    assert [len(g) for g in groups] == [3, 3, 3, 1]
    # max 25 bytes per group -> groups of <=2 files (10+10=20, +10 would be 30>25)
    groups = _chunk_objects(objs, max_files=99, max_bytes=25)
    # every multi-file group must respect the byte bound; a lone oversized file may exceed it
    for g in groups:
        if len(g) > 1:
            assert sum(o['Size'] for o in g) <= 25
    # 10-byte files, 25-byte budget -> at most 2 per group
    assert all(len(g) <= 2 for g in groups)


def test_index_objects_grouped_submits_coordinates_and_sets_built_flags(monkeypatch):
    calls = []

    def fake_submit(index, arity, bucket, rds_secret, rds_schema, s3_subdir, prefix,
                    prefer_compressed, chunk_index, chunk_count, group_size,
                    group_max_bytes, expected_total):
        calls.append({'chunk_index': chunk_index, 'chunk_count': chunk_count,
                      'expected_total': expected_total, 'prefix': prefix,
                      'prefer_compressed': prefer_compressed, 's3_subdir': s3_subdir})
        return {'status': 'SUCCEEDED'}

    monkeypatch.setattr(index_mod, 'start_and_wait_for_group_indexer_job', fake_submit)

    idx = Index.__new__(Index)
    idx.name = 'i'
    idx.s3_prefix = 'p/'
    idx.schema = MagicMock(arity=1)
    built = []
    idx.set_key_built_flag = lambda engine, key: built.append(key)

    cfg = MagicMock(s3_bucket='b', rds_secret='sec', bio_schema='sch', s3_subdir='sub')
    objs = _objs([10] * 5)
    idx.index_objects_grouped(cfg, MagicMock(), objs, prefer_compressed=True,
                              group_size=2, group_max_bytes=10 ** 9)

    assert len(calls) == 3
    assert all(c['chunk_count'] == 3 for c in calls)
    assert sorted(c['chunk_index'] for c in calls) == [0, 1, 2]
    assert all(c['expected_total'] == 5 for c in calls)
    assert all(c['prefix'] == 'p/' and c['prefer_compressed'] is True for c in calls)
    assert all(c['s3_subdir'] == 'sub' for c in calls)
    assert sorted(built) == sorted(o['Key'] for o in objs)


def test_index_objects_grouped_raises_on_failed_chunk(monkeypatch):
    monkeypatch.setattr(index_mod, 'start_and_wait_for_group_indexer_job',
                        lambda *a, **k: {'status': 'FAILED', 'statusReason': 'boom'})
    idx = Index.__new__(Index)
    idx.name = 'i'
    idx.s3_prefix = 'p/'
    idx.schema = MagicMock(arity=1)
    idx.set_key_built_flag = lambda engine, key: None
    cfg = MagicMock(s3_bucket='b', rds_secret='sec', bio_schema='sch')
    with pytest.raises(RuntimeError, match=r'boom'):
        idx.index_objects_grouped(cfg, MagicMock(), _objs([10] * 2), prefer_compressed=True,
                                  group_size=2, group_max_bytes=10 ** 9)


def _listed(keys):
    return [{'Key': k, 'Size': 1, 'ETag': '"v"'} for k in keys]


def test_list_index_objects_gz_only():
    from bioindex.lib.index import list_index_objects
    def fake_list(bucket, prefix, only=None):
        return iter(_listed(['p/a.json.gz', 'p/b.json.gz']) if only == '*.json.gz' else [])
    with patch('bioindex.lib.index.list_objects', side_effect=fake_list):
        objs = list_index_objects('b', 'p/', prefer_compressed=True)
    assert [o['Key'] for o in objs] == ['p/a.json.gz', 'p/b.json.gz']


def test_list_index_objects_mixed_without_prefer_raises():
    from bioindex.lib.index import list_index_objects
    def fake_list(bucket, prefix, only=None):
        return iter(_listed(['p/a.json']) if only == '*.json' else _listed(['p/a.json.gz']))
    with patch('bioindex.lib.index.list_objects', side_effect=fake_list):
        with pytest.raises(ValueError):
            list_index_objects('b', 'p/', prefer_compressed=False)


def test_list_index_objects_mixed_with_prefer_drops_json():
    from bioindex.lib.index import list_index_objects
    def fake_list(bucket, prefix, only=None):
        return iter(_listed(['p/a.json']) if only == '*.json' else _listed(['p/a.json.gz']))
    with patch('bioindex.lib.index.list_objects', side_effect=fake_list):
        objs = list_index_objects('b', 'p/', prefer_compressed=True)
    assert [o['Key'] for o in objs] == ['p/a.json.gz']


def test_list_index_objects_json_only():
    from bioindex.lib.index import list_index_objects
    def fake_list(bucket, prefix, only=None):
        return iter(_listed(['p/a.json', 'p/b.json']) if only == '*.json' else [])
    with patch('bioindex.lib.index.list_objects', side_effect=fake_list):
        objs = list_index_objects('b', 'p/', prefer_compressed=False)
    assert [o['Key'] for o in objs] == ['p/a.json', 'p/b.json']


class _FakeBatch:
    """Records submit_job params; reports SUCCEEDED immediately (no polling sleep)."""

    def __init__(self, captured):
        self._captured = captured

    def submit_job(self, **kwargs):
        self._captured['parameters'] = kwargs['parameters']
        return {'jobId': 'j1'}

    def describe_jobs(self, jobs):
        return {'jobs': [{'status': 'SUCCEEDED'}]}


def _run_submit(monkeypatch, s3_subdir):
    import bioindex.lib.aws as aws_mod
    captured = {}
    monkeypatch.setattr(aws_mod.boto3, 'client', lambda *a, **k: _FakeBatch(captured))
    aws_mod.start_and_wait_for_group_indexer_job(
        index='i', arity=1, bucket='b', rds_secret='sec', rds_schema='sch',
        s3_subdir=s3_subdir, prefix='p/', prefer_compressed=True, chunk_index=0,
        chunk_count=1, group_size=2, group_max_bytes=10 ** 9, expected_total=5,
    )
    return captured['parameters']


def test_submit_omits_empty_s3_subdir(monkeypatch):
    # Batch SubmitJob rejects empty parameter values; s3-subdir must be omitted, not ''.
    params = _run_submit(monkeypatch, s3_subdir='')
    assert 's3-subdir' not in params
    # and no other parameter is ever submitted empty
    assert all(v != '' for v in params.values())


def test_submit_includes_nonempty_s3_subdir(monkeypatch):
    params = _run_submit(monkeypatch, s3_subdir='bioindex')
    assert params['s3-subdir'] == 'bioindex'


def test_key_is_current_matches_version():
    from bioindex.lib.index import _key_is_current
    db_keys = {'p/a.json.gz': {'id': 1, 'version': 'v'}}
    assert _key_is_current(db_keys, {'Key': 'p/a.json.gz', 'ETag': '"v"'}) is True
    assert _key_is_current(db_keys, {'Key': 'p/a.json.gz', 'ETag': '"w"'}) is False
    assert _key_is_current(db_keys, {'Key': 'p/new.json.gz', 'ETag': '"v"'}) is False
    assert _key_is_current({'p/a.json.gz': {'id': 1, 'version': None}},
                           {'Key': 'p/a.json.gz', 'ETag': '"v"'}) is False
