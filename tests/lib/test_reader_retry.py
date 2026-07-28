import subprocess
import types

import pytest

from bioindex.lib import reader as reader_mod
from bioindex.lib.reader import RecordReader, RecordSource

CONTENT = b"".join(b'{"i":%d}\n' % i for i in range(6))
RECORD_BYTES = 8
ALL_RECORDS = [{"i": i} for i in range(6)]


@pytest.fixture
def bgzf_path(tmp_path):
    raw = tmp_path / "data.json"
    raw.write_bytes(CONTENT)
    subprocess.run(["bgzip", "-k", str(raw)], check=True)
    gz = tmp_path / "data.json.gz"
    subprocess.run(["bgzip", "-r", str(gz)], check=True)
    return gz


@pytest.fixture
def no_backoff(monkeypatch):
    monkeypatch.setattr(reader_mod.time, "sleep", lambda _: None)


@pytest.fixture
def bgzip(monkeypatch, bgzf_path):
    """
    Run the real bgzip against a local bgzf file, but let a test say which
    attempts should fail. A failing attempt still streams `truncate_after`
    records before dying, which is what a dropped S3 connection looks like.
    """
    real_popen = subprocess.Popen
    calls = []

    def install(fail_attempts=(), truncate_after=0):
        def wrapper(cmd, *args, **kwargs):
            cmd = [str(bgzf_path) if str(a).startswith("s3://") else a for a in cmd]
            calls.append(cmd)

            if len(calls) - 1 in fail_attempts:
                # emit some records, then exit non-zero
                script = (f"{_quote(cmd)} | head -c {truncate_after * RECORD_BYTES}; "
                          f"echo 'read failed: SlowDown' >&2; exit 1")
                return real_popen(["sh", "-c", script], *args, **kwargs)

            return real_popen(cmd, *args, **kwargs)

        monkeypatch.setattr("bioindex.lib.reader.subprocess.Popen", wrapper)
        return calls

    return install


def _quote(cmd):
    return " ".join(f"'{c}'" for c in cmd)


def _reader():
    config = types.SimpleNamespace(s3_bucket="test-bucket")
    index = types.SimpleNamespace(compressed=True)
    source = RecordSource(key="data.json.gz", start=0, end=len(CONTENT), bounded=True)
    return RecordReader(config, [source], index)


def test_a_clean_read_runs_bgzip_once(bgzip):
    calls = bgzip()
    assert list(_reader().records) == ALL_RECORDS
    assert len(calls) == 1


def test_bgzip_is_launched_with_the_resolved_credentials(bgzip, monkeypatch):
    frozen = types.SimpleNamespace(access_key="AKIA", secret_key="shh", token="tok")
    monkeypatch.setattr(reader_mod._session, "get_credentials",
                        lambda: types.SimpleNamespace(get_frozen_credentials=lambda: frozen))
    bgzip()

    launched = []
    real = reader_mod.subprocess.Popen
    monkeypatch.setattr("bioindex.lib.reader.subprocess.Popen",
                        lambda cmd, *a, **kw: (launched.append(kw), real(cmd, *a, **kw))[1])

    list(_reader().records)

    # htslib reads credentials from the environment and nowhere else
    assert launched[0]["env"]["AWS_ACCESS_KEY_ID"] == "AKIA"
    assert launched[0]["env"]["AWS_SESSION_TOKEN"] == "tok"


def test_a_failed_read_resumes_without_repeating_records(bgzip, no_backoff):
    # the first attempt dies after handing back two records
    calls = bgzip(fail_attempts=(0,), truncate_after=2)

    assert list(_reader().records) == ALL_RECORDS

    # the retry seeks past what the first attempt already yielded
    assert len(calls) == 2
    assert calls[0][1:3] == ["-b", "0"]
    assert calls[1][1:3] == ["-b", str(2 * RECORD_BYTES)]


def test_it_gives_up_after_the_configured_number_of_retries(bgzip, no_backoff, monkeypatch):
    monkeypatch.setattr(reader_mod, "BGZIP_MAX_RETRIES", 2)
    calls = bgzip(fail_attempts=(0, 1, 2), truncate_after=0)

    with pytest.raises(subprocess.CalledProcessError):
        list(_reader().records)

    assert len(calls) == 3  # the first try plus two retries


def test_bgzip_stderr_is_logged_when_a_read_fails(bgzip, no_backoff, caplog):
    bgzip(fail_attempts=(0,), truncate_after=1)

    with caplog.at_level("WARNING"):
        list(_reader().records)

    assert "SlowDown" in caplog.text


def test_credentials_are_handed_to_the_subprocess(monkeypatch):
    frozen = types.SimpleNamespace(access_key="AKIA", secret_key="shh", token="tok")
    monkeypatch.setattr(reader_mod._session, "get_credentials",
                        lambda: types.SimpleNamespace(get_frozen_credentials=lambda: frozen))

    env = reader_mod._bgzip_env()
    assert env["AWS_ACCESS_KEY_ID"] == "AKIA"
    assert env["AWS_SECRET_ACCESS_KEY"] == "shh"
    assert env["AWS_SESSION_TOKEN"] == "tok"


def test_a_session_token_is_omitted_when_there_is_none(monkeypatch):
    frozen = types.SimpleNamespace(access_key="AKIA", secret_key="shh", token=None)
    monkeypatch.setattr(reader_mod._session, "get_credentials",
                        lambda: types.SimpleNamespace(get_frozen_credentials=lambda: frozen))
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)

    assert "AWS_SESSION_TOKEN" not in reader_mod._bgzip_env()


def test_the_ambient_environment_survives_when_boto3_has_no_credentials(monkeypatch):
    monkeypatch.setattr(reader_mod._session, "get_credentials", lambda: None)
    monkeypatch.setenv("SOME_UNRELATED_VAR", "kept")

    env = reader_mod._bgzip_env()
    assert env["SOME_UNRELATED_VAR"] == "kept"
