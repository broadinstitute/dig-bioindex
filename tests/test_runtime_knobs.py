"""
The container sets BIOINDEX_WORKERS / BIOINDEX_THREAD_POOL in its
environment, so the server has to read them or the deployment silently
runs on the defaults.
"""
import contextlib

import anyio.to_thread
import pytest
from click.testing import CliRunner

import bioindex.main as main
import bioindex.server as server


@pytest.fixture
def uvicorn_options(monkeypatch):
    """Run `serve` far enough to capture what it would hand uvicorn."""
    captured = {}
    monkeypatch.setattr(main.uvicorn, 'run', lambda app, **kw: captured.update(app=app, **kw))
    monkeypatch.setenv('BIOINDEX_TOKEN_SIGNING_KEY', '0' * 64)

    def run(*args):
        result = CliRunner().invoke(main.cli_serve, args)
        assert result.exit_code == 0, result.output
        return captured

    return run


def test_workers_comes_from_the_environment(uvicorn_options, monkeypatch):
    monkeypatch.setenv('BIOINDEX_WORKERS', '4')

    assert uvicorn_options()['workers'] == 4


def test_workers_defaults_to_one(uvicorn_options, monkeypatch):
    monkeypatch.delenv('BIOINDEX_WORKERS', raising=False)

    assert uvicorn_options()['workers'] == 1


def test_the_flag_beats_the_environment(uvicorn_options, monkeypatch):
    monkeypatch.setenv('BIOINDEX_WORKERS', '4')

    assert uvicorn_options('--workers', '2')['workers'] == 2


def test_concurrency_is_capped_when_asked(uvicorn_options, monkeypatch):
    monkeypatch.setenv('BIOINDEX_LIMIT_CONCURRENCY', '64')

    assert uvicorn_options()['limit_concurrency'] == 64


def test_no_concurrency_cap_is_left_unset_rather_than_zero(uvicorn_options, monkeypatch):
    monkeypatch.delenv('BIOINDEX_LIMIT_CONCURRENCY', raising=False)

    # uvicorn has no sentinel for "no limit"; 0 would reject every request
    assert 'limit_concurrency' not in uvicorn_options()


def test_shutdown_waits_for_in_flight_requests(uvicorn_options):
    assert uvicorn_options()['timeout_graceful_shutdown'] == 30


@contextlib.contextmanager
def _restoring_thread_limit():
    # the limiter only exists inside a running loop, which is why the server
    # sizes it from lifespan rather than at import
    limiter = anyio.to_thread.current_default_thread_limiter()
    original = limiter.total_tokens
    try:
        yield limiter
    finally:
        limiter.total_tokens = original


@pytest.mark.asyncio
async def test_thread_pool_is_sized_from_the_environment(monkeypatch):
    monkeypatch.setenv('BIOINDEX_THREAD_POOL', '75')

    with _restoring_thread_limit() as limiter:
        server._configure_thread_pool()
        assert limiter.total_tokens == 75


@pytest.mark.asyncio
async def test_thread_pool_defaults_to_the_anyio_default(monkeypatch):
    monkeypatch.delenv('BIOINDEX_THREAD_POOL', raising=False)

    with _restoring_thread_limit() as limiter:
        server._configure_thread_pool()
        assert limiter.total_tokens == 40
