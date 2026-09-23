"""Unit tests for batch-compression-management/compress_json_array.py.

The worker lives outside the package (it is copied into the bgzip Docker image), so it is
loaded by path. Its sibling `utils` module is resolved by putting the script's directory on
sys.path, exactly as the image's working directory does.
"""
import importlib.util
import pathlib
import sys

import pytest

_DIR = pathlib.Path('batch-compression-management')
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))

_spec = importlib.util.spec_from_file_location('compress_json_array', _DIR / 'compress_json_array.py')
worker = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(worker)


class _FakeS3:
    """Answers list_objects_v2 with a fixed set of keys; records delete_object calls."""

    def __init__(self, keys):
        self.keys = set(keys)
        self.deleted = []

    def list_objects_v2(self, Bucket, Prefix):
        found = sorted(k for k in self.keys if k.startswith(Prefix))
        return {'Contents': [{'Key': k} for k in found]} if found else {}

    def delete_object(self, Bucket, Key):
        self.deleted.append(Key)
        self.keys.discard(Key)


def test_array_index_from_env_reads_batch_variable():
    assert worker.array_index_from_env({'AWS_BATCH_JOB_ARRAY_INDEX': '7'}) == 7


def test_array_index_defaults_to_zero():
    # A plain (non-array) submit has no AWS_BATCH_JOB_ARRAY_INDEX; it is child 0 of 1.
    assert worker.array_index_from_env({}) == 0


def test_select_files_takes_every_nth_of_sorted_keys():
    keys = ['p/c.json', 'p/a.json', 'p/d.json', 'p/b.json', 'p/e.json']
    assert worker.select_files(keys, 0, 2) == ['p/a.json', 'p/c.json', 'p/e.json']
    assert worker.select_files(keys, 1, 2) == ['p/b.json', 'p/d.json']


def test_select_files_one_per_child_when_size_equals_count():
    keys = [f'p/part-{i:03d}.json' for i in range(5)]
    for i in range(5):
        assert worker.select_files(keys, i, 5) == [f'p/part-{i:03d}.json']


def test_select_files_beyond_file_count_is_empty():
    # --array-size bigger than the number of files: surplus children own nothing.
    assert worker.select_files(['p/a.json', 'p/b.json'], 4, 10) == []


def test_select_files_rejects_out_of_range_index():
    with pytest.raises(AssertionError, match='out of range'):
        worker.select_files(['p/a.json'], 3, 3)
    with pytest.raises(AssertionError, match='out of range'):
        worker.select_files(['p/a.json'], -1, 3)


def test_compressed_pair_exists_when_gz_and_gzi_present():
    s3 = _FakeS3(['p/a.json', 'p/a.json.gz', 'p/a.json.gz.gzi'])
    assert worker.compressed_pair_exists(s3, 'bkt', 'p/a.json') is True


def test_compressed_pair_requires_both_gz_and_gzi():
    # A crashed run can leave .gz without .gzi; that file must be recompressed.
    assert worker.compressed_pair_exists(_FakeS3(['p/a.json', 'p/a.json.gz']), 'bkt', 'p/a.json') is False
    assert worker.compressed_pair_exists(_FakeS3(['p/a.json']), 'bkt', 'p/a.json') is False


def test_compressed_pair_ignores_other_keys_sharing_the_prefix():
    # 'p/a.json.gz' is a prefix of 'p/a.json.gz.bak' etc.; only the exact pair counts.
    s3 = _FakeS3(['p/a.json', 'p/a.json.gz.gzi', 'p/a.json.gzx'])
    assert worker.compressed_pair_exists(s3, 'bkt', 'p/a.json') is False


class _Result:
    def __init__(self, returncode, stderr=''):
        self.returncode = returncode
        self.stderr = stderr


class _FakeRun:
    """Stands in for subprocess.run: records argv and kwargs, answers from a script."""

    def __init__(self, returncodes):
        self.returncodes = list(returncodes)
        self.calls = []

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        return _Result(self.returncodes.pop(0), stderr='[E::bgzf] boom' if self.returncodes else '')


def test_compress_one_skips_when_pair_exists(monkeypatch):
    run = _FakeRun([])
    monkeypatch.setattr(worker.subprocess, 'run', run)
    s3 = _FakeS3(['p/a.json', 'p/a.json.gz', 'p/a.json.gz.gzi'])
    assert worker.compress_one(s3, 'bkt', 'p/a.json', threads=4) == worker.SKIPPED
    assert run.calls == []


def test_compress_one_runs_threaded_bgzip_then_validates_without_timeout(monkeypatch):
    run = _FakeRun([0, 0])
    monkeypatch.setattr(worker.subprocess, 'run', run)
    assert worker.compress_one(_FakeS3(['p/a.json']), 'bkt', 'p/a.json', threads=4) == worker.COMPRESSED
    argv = [c for c, _ in run.calls]
    assert argv == [
        ['bgzip', '-@', '4', '-i', 's3://bkt/p/a.json'],
        ['bgzip', '-t', 's3://bkt/p/a.json.gz'],
    ]
    for _, kwargs in run.calls:
        assert 'timeout' not in kwargs, 'the array worker must not impose a per-file timeout'


def test_compress_one_reports_failure_when_bgzip_fails(monkeypatch, capsys):
    run = _FakeRun([2, 0])
    monkeypatch.setattr(worker.subprocess, 'run', run)
    assert worker.compress_one(_FakeS3(['p/a.json']), 'bkt', 'p/a.json', threads=2) == worker.FAILED
    assert len(run.calls) == 1, 'validation must not run after a failed compression'
    out = capsys.readouterr().out
    assert 'exited 2' in out and '[E::bgzf] boom' in out


def test_compress_one_reports_failure_when_validation_fails(monkeypatch):
    run = _FakeRun([0, 1])
    monkeypatch.setattr(worker.subprocess, 'run', run)
    assert worker.compress_one(_FakeS3(['p/a.json']), 'bkt', 'p/a.json', threads=2) == worker.FAILED
    assert len(run.calls) == 2


def test_compress_one_deletes_partial_outputs_when_validation_fails(monkeypatch):
    # bgzip -i succeeded (both outputs may exist) but bgzip -t failed: the pair is corrupt
    # and must not survive to be picked up as "already compressed" on a retry.
    run = _FakeRun([0, 1])
    monkeypatch.setattr(worker.subprocess, 'run', run)
    s3 = _FakeS3(['p/a.json'])
    assert worker.compress_one(s3, 'bkt', 'p/a.json', threads=2) == worker.FAILED
    assert set(s3.deleted) == {'p/a.json.gz', 'p/a.json.gz.gzi'}


def test_compress_one_deletes_partial_outputs_when_compression_fails(monkeypatch):
    # bgzip -i itself failed; delete is still issued (idempotent even if nothing was written).
    run = _FakeRun([2, 0])
    monkeypatch.setattr(worker.subprocess, 'run', run)
    s3 = _FakeS3(['p/a.json'])
    assert worker.compress_one(s3, 'bkt', 'p/a.json', threads=2) == worker.FAILED
    assert set(s3.deleted) == {'p/a.json.gz', 'p/a.json.gz.gzi'}


def test_log_flushes_every_call(monkeypatch):
    # stdout is a pipe under awslogs and Python full-buffers it; a child killed by the
    # attempt timeout would otherwise leave an empty log. Every worker message must flush.
    calls = []

    def recorder(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(worker, 'print', recorder, raising=False)
    worker.log('x')
    assert calls == [(('x',), {'flush': True})]


def _patch_main(monkeypatch, keys, outcomes):
    """Wire main() to fake listing/creds/session/client and a scripted compress_one.

    Returns (listed, compressed, calls) where `calls` records the order in which the task
    role's credentials are resolved and the bgzip user's creds are exported.
    """
    calls = []

    class _FakeSession:
        def get_credentials(self):
            calls.append('get_credentials')
            return object()  # sentinel; real code never inspects the return value

        def client(self, name):
            return object()

    monkeypatch.setattr(worker.boto3, 'Session', lambda: _FakeSession())
    monkeypatch.setattr(worker.utils, 'set_bgzip_creds', lambda: calls.append('set_bgzip_creds'))
    listed = {}

    def fake_list(bucket, path, only=None):
        listed['args'] = (bucket, path, only)
        return [{'Key': k, 'Size': 10} for k in keys]

    monkeypatch.setattr(worker.s3, 'list_objects', fake_list)
    compressed = []

    def fake_compress_one(boto_s3, bucket, key, threads):
        compressed.append((key, threads))
        return outcomes.get(key, worker.COMPRESSED)

    monkeypatch.setattr(worker, 'compress_one', fake_compress_one)
    return listed, compressed, calls


def test_main_processes_only_this_childs_stride(monkeypatch):
    monkeypatch.setenv('AWS_BATCH_JOB_ARRAY_INDEX', '1')
    keys = [f'p/part-{i}.json' for i in range(4)]
    listed, compressed, _ = _patch_main(monkeypatch, keys, {})
    worker.main.callback(index='idx', bucket='bkt', path='p/', array_size=2, threads=3)
    assert listed['args'] == ('bkt', 'p/', '*.json')
    assert compressed == [('p/part-1.json', 3), ('p/part-3.json', 3)]


def test_main_with_no_files_exits_zero(monkeypatch):
    # array-size larger than the file count: this child owns nothing and must succeed.
    monkeypatch.setenv('AWS_BATCH_JOB_ARRAY_INDEX', '9')
    _, compressed, _ = _patch_main(monkeypatch, ['p/a.json'], {})
    worker.main.callback(index='idx', bucket='bkt', path='p/', array_size=10, threads=4)
    assert compressed == []


def test_main_continues_after_failure_and_exits_one(monkeypatch, capsys):
    monkeypatch.delenv('AWS_BATCH_JOB_ARRAY_INDEX', raising=False)
    keys = ['p/a.json', 'p/b.json', 'p/c.json']
    _, compressed, _ = _patch_main(monkeypatch, keys, {'p/a.json': worker.FAILED, 'p/b.json': worker.SKIPPED})
    with pytest.raises(SystemExit) as exc:
        worker.main.callback(index='idx', bucket='bkt', path='p/', array_size=1, threads=4)
    assert exc.value.code == 1
    assert [k for k, _ in compressed] == keys, 'one failure must not stop the remaining files'
    assert 'compressed 1, skipped 1, failed 1' in capsys.readouterr().out


def test_main_resolves_task_role_credentials_before_exporting_bgzip_creds(monkeypatch):
    # utils.set_bgzip_creds() exports the bgzip user's keys (no DeleteObject) into the env;
    # the task role's credentials must be pinned on the session before that happens so a
    # later boto client built from the session can still delete partial outputs on failure.
    monkeypatch.delenv('AWS_BATCH_JOB_ARRAY_INDEX', raising=False)
    _, _, calls = _patch_main(monkeypatch, ['p/a.json'], {})
    worker.main.callback(index='idx', bucket='bkt', path='p/', array_size=1, threads=4)
    assert calls == ['get_credentials', 'set_bgzip_creds']
