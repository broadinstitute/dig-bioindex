"""`compress-array` CLI: validates the index, sizes the array from the listing, exits on FAILED."""
from click.testing import CliRunner

import bioindex.main as main


class _Cfg:
    s3_bucket = 'bkt'

    def s3_path(self, prefix):
        return f'sub/{prefix}'


def _wire(monkeypatch, n_files, valid=True, status='SUCCEEDED', summary=None):
    calls = {}
    monkeypatch.setattr(main, 'is_index_prefix_valid', lambda cfg, idx, prefix: valid)
    monkeypatch.setattr(main, 'list_objects',
                        lambda bucket, path, only=None: iter([{'Key': f'{path}{i}.json', 'Size': 1}
                                                             for i in range(n_files)]))

    def fake_start(bucket, index_name, s3_path, array_size, threads=4):
        calls['start'] = (bucket, index_name, s3_path, array_size, threads)
        return 'job-9'

    def fake_wait(job_id, poll_seconds=30):
        calls['wait'] = job_id
        return status, (summary or {})

    monkeypatch.setattr(main.aws, 'start_compress_array_job', fake_start)
    monkeypatch.setattr(main.aws, 'wait_for_array_job', fake_wait)
    return calls


def test_default_array_size_is_the_file_count(monkeypatch):
    calls = _wire(monkeypatch, n_files=5)
    result = CliRunner().invoke(main.cli_compress_array, ['idx', 'p/'], obj=_Cfg())
    assert result.exit_code == 0, result.output
    assert calls['start'] == ('bkt', 'idx', 'sub/p/', 5, 4)
    assert calls['wait'] == 'job-9'
    assert 'job-9' in result.output


def test_explicit_array_size_and_threads(monkeypatch):
    calls = _wire(monkeypatch, n_files=5)
    result = CliRunner().invoke(main.cli_compress_array,
                                ['idx', 'p/', '--array-size', '2', '--threads', '8'], obj=_Cfg())
    assert result.exit_code == 0, result.output
    assert calls['start'] == ('bkt', 'idx', 'sub/p/', 2, 8)


def test_array_size_is_capped_at_batch_maximum(monkeypatch):
    calls = _wire(monkeypatch, n_files=main.aws.BATCH_ARRAY_MAX_SIZE + 5)
    result = CliRunner().invoke(main.cli_compress_array, ['idx', 'p/'], obj=_Cfg())
    assert result.exit_code == 0, result.output
    assert calls['start'][3] == main.aws.BATCH_ARRAY_MAX_SIZE


def test_no_files_submits_nothing(monkeypatch):
    calls = _wire(monkeypatch, n_files=0)
    result = CliRunner().invoke(main.cli_compress_array, ['idx', 'p/'], obj=_Cfg())
    assert result.exit_code == 0, result.output
    assert 'start' not in calls
    assert 'nothing to do' in result.output


def test_invalid_index_exits_one_without_submitting(monkeypatch):
    calls = _wire(monkeypatch, n_files=5, valid=False)
    result = CliRunner().invoke(main.cli_compress_array, ['idx', 'p/'], obj=_Cfg())
    assert result.exit_code == 1
    assert 'start' not in calls


def test_no_wait_returns_after_submit(monkeypatch):
    calls = _wire(monkeypatch, n_files=5)
    result = CliRunner().invoke(main.cli_compress_array, ['idx', 'p/', '--no-wait'], obj=_Cfg())
    assert result.exit_code == 0, result.output
    assert 'start' in calls and 'wait' not in calls


def test_failed_parent_exits_one_and_prints_summary(monkeypatch):
    _wire(monkeypatch, n_files=5, status='FAILED', summary={'SUCCEEDED': 4, 'FAILED': 1})
    result = CliRunner().invoke(main.cli_compress_array, ['idx', 'p/'], obj=_Cfg())
    assert result.exit_code == 1
    assert 'FAILED' in result.output and "'FAILED': 1" in result.output
