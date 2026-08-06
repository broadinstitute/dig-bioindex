from unittest.mock import MagicMock
import bioindex.lib.index as index_mod
from bioindex.lib.index import Index


def _make_index(monkeypatch, json_keys, gz_keys):
    """Build an Index whose S3 listing returns the given keys; stub out all DB/index work."""
    def fake_list_objects(bucket, path, only=None):
        if only == '*.json':
            return [{'Key': k, 'Size': 10, 'ETag': '"v"'} for k in json_keys]
        if only == '*.json.gz':
            return [{'Key': k, 'Size': 10, 'ETag': '"v"'} for k in gz_keys]
        return []
    monkeypatch.setattr(index_mod, 'list_objects', fake_list_objects)

    idx = Index.__new__(Index)
    idx.name = 'i'
    idx.s3_prefix = 'p/'
    idx.schema = MagicMock(arity=1)
    idx.table = MagicMock()
    # Record what gets indexed; short-circuit everything after object selection.
    idx.delete_stale_keys = lambda config, engine, objects, console=None: objects
    idx.index_objects_local = MagicMock()
    idx.index_objects_remote = MagicMock()
    idx.set_built_flag = MagicMock()
    return idx


def test_build_raises_on_mixed_by_default(monkeypatch):
    idx = _make_index(monkeypatch, json_keys=['p/a.json'], gz_keys=['p/a.json.gz'])
    cfg = MagicMock(s3_bucket='b'); cfg.s3_path = lambda p: p
    import pytest
    with pytest.raises(ValueError):
        idx.build(cfg, MagicMock())


def test_build_prefers_gz_when_prefer_compressed(monkeypatch):
    idx = _make_index(monkeypatch, json_keys=['p/a.json'], gz_keys=['p/a.json.gz'])
    cfg = MagicMock(s3_bucket='b'); cfg.s3_path = lambda p: p
    captured = {}
    idx.delete_stale_keys = lambda config, engine, objects, console=None: captured.setdefault('objects', objects) or objects
    idx.build(cfg, MagicMock(), prefer_compressed=True)
    keys = [o['Key'] for o in captured['objects']]
    assert keys == ['p/a.json.gz']  # the .json original is ignored, not indexed


def test_index_objects_remote_fails_on_failed_batch_child():
    """A batch indexer job that returns FAILED must NOT have its key marked built, and
    the build must fail loudly instead of silently skipping the file."""
    import concurrent.futures
    import pytest

    idx = Index.__new__(Index)
    idx.set_key_built_flag = MagicMock()

    def run_function(config, obj):
        status = 'FAILED' if 'bad' in obj['Key'] else 'SUCCEEDED'
        return {'status': status, 'statusReason': 'CannotPullContainerError',
                'parameters': {'file': obj['Key'], 'file-size': '10'}}

    objects = [{'Key': 'p/good.json.gz', 'Size': 10},
               {'Key': 'p/bad.json.gz', 'Size': 10}]
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    with pytest.raises(Exception) as exc:
        idx.index_objects_remote(MagicMock(), MagicMock(), pool, objects, run_function)

    built = [c.args[1] for c in idx.set_key_built_flag.call_args_list]
    assert 'p/bad.json.gz' not in built     # failed job's key must not be marked built
    assert 'p/good.json.gz' in built         # succeeded job still marked built
    assert 'bad.json.gz' in str(exc.value)   # failure names the offending file
