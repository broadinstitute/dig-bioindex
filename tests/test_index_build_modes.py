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
