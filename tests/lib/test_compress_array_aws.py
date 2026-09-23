"""start_compress_array_job / wait_for_array_job: payload shape and polling behaviour."""
import pytest

import bioindex.lib.aws as aws_mod


class _FakeBatch:
    def __init__(self, describe_responses=None):
        self.submitted = []
        self.describe_responses = list(describe_responses or [])
        self.describe_calls = 0

    def submit_job(self, **kwargs):
        self.submitted.append(kwargs)
        return {'jobId': 'job-1'}

    def describe_jobs(self, jobs):
        self.describe_calls += 1
        return self.describe_responses.pop(0)


def _install(monkeypatch, fake):
    monkeypatch.setattr(aws_mod.boto3, 'client', lambda *a, **k: fake)
    return fake


def test_start_submits_array_with_stringified_parameters(monkeypatch):
    fake = _install(monkeypatch, _FakeBatch())
    job_id = aws_mod.start_compress_array_job('bkt', 'variant-dataset-associations',
                                              'associations/variant/', 1000, threads=4)
    assert job_id == 'job-1'
    (call,) = fake.submitted
    assert call['jobQueue'] == 'bgzip-job-queue'
    assert call['jobDefinition'] == 'bgzip-array-job'
    assert call['arrayProperties'] == {'size': 1000}
    assert call['parameters'] == {
        'index': 'variant-dataset-associations', 'bucket': 'bkt', 'path': 'associations/variant/',
        'array-size': '1000', 'threads': '4',
    }
    assert call['jobName'].startswith('bgzip-array-job-')


def test_start_size_one_is_plain_submit(monkeypatch):
    # Batch rejects arrayProperties.size < 2; a single file goes as an ordinary job.
    fake = _install(monkeypatch, _FakeBatch())
    aws_mod.start_compress_array_job('bkt', 'idx', 'p/', 1)
    (call,) = fake.submitted
    assert 'arrayProperties' not in call
    assert call['parameters']['array-size'] == '1'


def test_start_rejects_sizes_outside_batch_limits(monkeypatch):
    _install(monkeypatch, _FakeBatch())
    with pytest.raises(AssertionError):
        aws_mod.start_compress_array_job('bkt', 'idx', 'p/', 0)
    with pytest.raises(AssertionError):
        aws_mod.start_compress_array_job('bkt', 'idx', 'p/', aws_mod.BATCH_ARRAY_MAX_SIZE + 1)


def test_start_sanitises_job_name(monkeypatch):
    # Batch job names allow only letters, digits, '-' and '_' (max 128 chars).
    fake = _install(monkeypatch, _FakeBatch())
    aws_mod.start_compress_array_job('bkt', 'weird name/with.dots', 'p/', 2)
    name = fake.submitted[0]['jobName']
    assert name == 'bgzip-array-job-weird-name-with-dots'
    assert len(name) <= 128


def test_wait_returns_status_and_child_summary(monkeypatch):
    fake = _install(monkeypatch, _FakeBatch([
        {'jobs': [{'status': 'RUNNING'}]},
        {'jobs': [{'status': 'FAILED',
                   'arrayProperties': {'statusSummary': {'SUCCEEDED': 998, 'FAILED': 2}}}]},
    ]))
    status, summary = aws_mod.wait_for_array_job('job-1', poll_seconds=0)
    assert status == 'FAILED'
    assert summary == {'SUCCEEDED': 998, 'FAILED': 2}
    assert fake.describe_calls == 2


def test_wait_tolerates_empty_describe_then_terminal(monkeypatch):
    # DescribeJobs can return no rows for a moment right after SubmitJob.
    fake = _install(monkeypatch, _FakeBatch([
        {'jobs': []},
        {'jobs': [{'status': 'SUCCEEDED', 'arrayProperties': {'statusSummary': {'SUCCEEDED': 3}}}]},
    ]))
    assert aws_mod.wait_for_array_job('job-1', poll_seconds=0) == ('SUCCEEDED', {'SUCCEEDED': 3})
    assert fake.describe_calls == 2


def test_wait_plain_job_has_empty_summary(monkeypatch):
    _install(monkeypatch, _FakeBatch([{'jobs': [{'status': 'SUCCEEDED'}]}]))
    assert aws_mod.wait_for_array_job('job-1', poll_seconds=0) == ('SUCCEEDED', {})
