from unittest.mock import MagicMock
import bioindex.lib.index as index_mod
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


def test_index_objects_grouped_submits_one_job_per_chunk_and_sets_built_flags(monkeypatch):
    submitted = []

    def fake_submit(keys_uri, index, arity, bucket, rds_secret, rds_schema):
        submitted.append(keys_uri)
        return {'status': 'SUCCEEDED'}

    monkeypatch.setattr(index_mod, 'start_and_wait_for_group_indexer_job', fake_submit)
    monkeypatch.setattr(index_mod, '_write_keys_manifest',
                        lambda bucket, index, keys: f's3://{bucket}/__manifest/{index}/{len(keys)}.txt')

    idx = Index.__new__(Index)
    idx.name = 'i'
    idx.s3_prefix = 'p/'
    idx.schema = MagicMock(arity=1)
    built = []
    idx.set_key_built_flag = lambda engine, key: built.append(key)

    cfg = MagicMock(s3_bucket='b', rds_secret='sec', bio_schema='sch')
    objs = _objs([10] * 5)
    idx.index_objects_grouped(cfg, MagicMock(), objs, group_size=2, group_max_bytes=10 ** 9)

    assert len(submitted) == 3                      # chunks of 2,2,1
    assert sorted(built) == sorted(o['Key'] for o in objs)  # every key marked built
