import importlib.util, pathlib
from unittest.mock import MagicMock

_spec = importlib.util.spec_from_file_location(
    'index_group', pathlib.Path('batch-index-files/index_group.py'))
index_group = importlib.util.module_from_spec(_spec)


def test_read_manifest_parses_keys(monkeypatch):
    _spec.loader.exec_module(index_group)
    body = b'p/a.json.gz\np/b.json.gz\n\n'  # trailing/blank lines ignored
    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = {'Body': MagicMock(read=lambda: body)}
    monkeypatch.setattr(index_group.boto3, 'client', lambda svc: fake_s3)
    keys = index_group.read_manifest('s3://bucket/__sync_manifests/i/abc.txt')
    assert keys == ['p/a.json.gz', 'p/b.json.gz']


def test_index_keys_calls_index_object_and_streams_each(monkeypatch):
    _spec.loader.exec_module(index_group)
    idx = MagicMock()
    idx.index_object.side_effect = lambda engine, bucket, obj: (obj['Key'], iter([{'x': 1}]))
    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {'ContentLength': 5, 'ETag': '"v"'}
    monkeypatch.setattr(index_group.boto3, 'client', lambda svc: fake_s3)
    index_group.index_keys(idx, MagicMock(), 'bkt', ['p/a.json.gz', 'p/b.json.gz'])
    assert idx.index_object.call_count == 2
    assert idx.insert_records_iter.call_count == 2  # streamed, not list()
