"""
A Batch child that dies must not be recorded as built.

start_and_wait_for_indexer_job returns the job description for FAILED just as
it does for SUCCEEDED - it waits on `status in ['SUCCEEDED', 'FAILED']` and
returns either - so a failed child arrives at the collector looking exactly
like a finished one, and the key gets its built flag set for a file that was
never indexed. The build then reports success with data missing.
"""
import concurrent.futures
import types

import pytest

from bioindex.lib.index import Index


def _batch_result(key, status, size=100):
    """What describe_jobs gives back once a job reaches a terminal state."""
    return {
        'status': status,
        'statusReason': None if status == 'SUCCEEDED' else 'Essential container exited',
        'parameters': {'file': key, 'file-size': str(size)},
    }


def _lambda_result(key, records=5, size=100):
    return {'key': key, 'records': records, 'size': size}


@pytest.fixture
def index(monkeypatch):
    """An Index that records which keys it was told to mark built."""
    idx = Index.__new__(Index)
    idx.name = 'test-index'
    idx.built = []
    monkeypatch.setattr(Index, 'set_key_built_flag',
                        lambda self, engine, key: self.built.append(key))
    return idx


def _run(index, results):
    """Drive index_objects_remote over a set of canned job results."""
    config = types.SimpleNamespace()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        index.index_objects_remote(
            config, engine=object(), pool=pool,
            objects=results,
            run_function=lambda cfg, obj: obj,
        )


def test_a_succeeded_batch_job_marks_its_key_built(index):
    _run(index, [_batch_result('a.json', 'SUCCEEDED')])

    assert index.built == ['a.json']


def test_a_failed_batch_job_does_not_mark_its_key_built(index):
    with pytest.raises(RuntimeError):
        _run(index, [_batch_result('a.json', 'FAILED')])

    assert index.built == []


def test_the_build_fails_rather_than_reporting_a_partial_success(index):
    with pytest.raises(RuntimeError) as exc_info:
        _run(index, [_batch_result('bad.json', 'FAILED')])

    assert 'bad.json' in str(exc_info.value)


def test_one_failure_does_not_hide_the_others(index):
    # every job is waited on before the build gives up, so the operator sees
    # the whole list rather than whichever one happened to finish first
    results = [_batch_result(f'{n}.json', 'FAILED') for n in 'abc']

    with pytest.raises(RuntimeError) as exc_info:
        _run(index, results)

    message = str(exc_info.value)
    assert '3 indexer job(s) failed' in message
    for name in ('a.json', 'b.json', 'c.json'):
        assert name in message


def test_the_good_keys_in_a_mixed_batch_are_still_recorded(index):
    # a failed sibling must not cost the files that did index
    results = [
        _batch_result('ok1.json', 'SUCCEEDED'),
        _batch_result('bad.json', 'FAILED'),
        _batch_result('ok2.json', 'SUCCEEDED'),
    ]

    with pytest.raises(RuntimeError):
        _run(index, results)

    assert sorted(index.built) == ['ok1.json', 'ok2.json']


def test_lambda_results_are_unaffected(index):
    # the lambda path reports differently - it has no 'status' - and raises
    # on failure rather than returning, so it must not be caught by the check
    _run(index, [_lambda_result('a.json'), _lambda_result('b.json')])

    assert sorted(index.built) == ['a.json', 'b.json']


def test_progress_still_advances_past_a_failure(index):
    # otherwise the bar stalls short of the total and the run looks hung
    advanced = []
    progress = types.SimpleNamespace(
        advance=lambda overall, advance: advanced.append(advance))

    config = types.SimpleNamespace()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        with pytest.raises(RuntimeError):
            index.index_objects_remote(
                config, engine=object(), pool=pool,
                objects=[_batch_result('bad.json', 'FAILED', size=42)],
                run_function=lambda cfg, obj: obj,
                progress=progress, overall='bar',
            )

    assert advanced == [42]
