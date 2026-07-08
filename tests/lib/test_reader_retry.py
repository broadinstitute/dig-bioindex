import logging
import subprocess
from unittest.mock import MagicMock

import orjson
import pytest

import bioindex.lib.reader as reader_mod
from bioindex.lib.reader import RecordReader, RecordSource


# Four physical 8-byte JSON-lines records, at cumulative UNCOMPRESSED byte
# offsets 0, 8, 16, 24 (each record is 8 bytes: b'{"v":N}\n'). A faithful fake
# bgzip must return only the records whose start offset is >= the requested
# ``-b`` offset, exactly like htslib random access. That behavior is what lets
# these tests actually prove resume-correctness rather than just count calls.
PHYSICAL_LINES = [b'{"v":1}\n', b'{"v":2}\n', b'{"v":3}\n', b'{"v":4}\n']
LINE_OFFSETS = [0, 8, 16, 24]
STDERR_MARKER = b"[E::hts_open] 503 SlowDown"


class _FakeStderr:
    """Stand-in for proc.stderr — only ``.read()`` (returning bytes) is used."""

    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


def make_fake_popen(scripts):
    """Build a fake ``subprocess.Popen`` that simulates bgzip seek-by-offset.

    ``scripts`` is a list of per-call ``(mode, n)`` tuples:
      - ("fail", n): emit the first ``n`` offset-filtered lines, then exit 1
        with a 503 SlowDown on stderr.
      - ("ok", None): emit ALL offset-filtered lines, then exit 0.
    If more calls happen than scripts are provided, the last script repeats.
    """

    class FakePopen:
        calls = []  # argv of every call, in order

        def __init__(self, command, stdout=None, stderr=None, env=None):
            FakePopen.calls.append(list(command))
            call_no = len(FakePopen.calls) - 1

            # Simulate htslib random access: ``-b <offset>`` yields only the
            # records whose start offset is >= the requested offset. If the
            # reader ever restarted a source instead of resuming, this fake
            # would re-emit already-yielded records and the duplicate check
            # in test_retry_resumes_without_duplicates would fail.
            seek_start = int(command[command.index("-b") + 1])
            available = [ln for off, ln in zip(LINE_OFFSETS, PHYSICAL_LINES)
                         if off >= seek_start]

            mode, n = scripts[call_no] if call_no < len(scripts) else scripts[-1]
            if mode == "fail":
                self._emit = available[:n]
                self._returncode = 1
                self._stderr = STDERR_MARKER
            else:
                self._emit = available
                self._returncode = 0
                self._stderr = b""

            self.stdout = iter(self._emit)
            self.stderr = _FakeStderr(self._stderr)
            self.returncode = None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def wait(self):
            self.returncode = self._returncode

    return FakePopen


def _make_reader(monkeypatch, fake_popen, bounded=True, end=10_000):
    # Avoid boto3/network and any real backoff sleep.
    monkeypatch.setattr(reader_mod, "_aws_env_for_htslib", lambda: {})
    monkeypatch.setattr(reader_mod.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(reader_mod.subprocess, "Popen", fake_popen)

    cfg = MagicMock()
    cfg.s3_bucket = "b"
    idx = MagicMock()
    idx.compressed = True  # exercise the compressed / bgzip branch
    # bounded controls whether source.end is an UNCOMPRESSED boundary (True,
    # SQL-derived) or the object's COMPRESSED size (False, /all path). For the
    # unbounded case end is a compressed size, so callers pass a small value.
    src = RecordSource(key="k", start=0, end=end, bounded=bounded)
    return RecordReader(cfg, [src], idx)


def _seek_offset(argv):
    """Return the ``-b`` offset argument (as a str) from a recorded argv."""
    return argv[argv.index("-b") + 1]


def test_retry_resumes_without_duplicates(monkeypatch, caplog):
    # First bgzip call (-b 0) emits records 1 & 2 then fails; the retry must
    # RESUME at byte 16, not restart, so no record is delivered twice.
    fake_popen = make_fake_popen([("fail", 2), ("ok", None)])
    r = _make_reader(monkeypatch, fake_popen)

    with caplog.at_level(logging.WARNING):
        collected = [rec["v"] for rec in r.records]

    # Each record delivered exactly once — NOT [1, 2, 1, 2, 3, 4].
    assert collected == [1, 2, 3, 4]
    assert r.count == 4
    assert len(fake_popen.calls) == 2
    # The heart of the test: the second call seeks to byte 16 (len of records
    # 1 + 2), proving the retry resumed from self._source_byte_offset.
    assert _seek_offset(fake_popen.calls[1]) == "16"
    # The bgzip stderr reached the logs at WARNING.
    assert "503 SlowDown" in caplog.text
    assert any(rec.levelno == logging.WARNING for rec in caplog.records)


def test_gives_up_after_max_retries_logs_stderr(monkeypatch, caplog):
    monkeypatch.setattr(reader_mod, "BGZIP_MAX_RETRIES", 2)
    # Always fails, emitting nothing, so the offset never advances.
    fake_popen = make_fake_popen([("fail", 0)])
    r = _make_reader(monkeypatch, fake_popen)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(subprocess.CalledProcessError):
            list(r.records)

    # 1 initial attempt + 2 retries = 3 Popen calls.
    assert len(fake_popen.calls) == 3
    # The final give-up logs the bgzip stderr at ERROR before re-raising.
    assert "503 SlowDown" in caplog.text
    assert any(rec.levelno == logging.ERROR for rec in caplog.records)


def test_success_first_try_no_retry(monkeypatch, caplog):
    fake_popen = make_fake_popen([("ok", None)])
    r = _make_reader(monkeypatch, fake_popen)

    with caplog.at_level(logging.WARNING):
        collected = [rec["v"] for rec in r.records]

    assert collected == [1, 2, 3, 4]
    assert r.count == 4
    assert len(fake_popen.calls) == 1
    # No retry path taken → nothing logged at WARNING or above.
    assert [rec for rec in caplog.records if rec.levelno >= logging.WARNING] == []


def test_unbounded_retry_resumes_without_truncation(monkeypatch, caplog):
    # UNBOUNDED source (/all path): source.end is the object's COMPRESSED size,
    # NOT an uncompressed boundary. Here end=12 is smaller than the uncompressed
    # resume offset (16, after records 1 & 2), which is exactly the realistic
    # case — compression shrinks the bytes. The buggy per-attempt guard computed
    # cur_seek_length = source.end - cur_seek_start = 12 - 16 = -4 and treated
    # <= 0 as "fully consumed", breaking out of the retry loop and silently
    # dropping records 3 & 4 (a 200 with incomplete data). The fix must NOT
    # derive any length/early-break from source.end for unbounded sources.
    #
    # (The task suggested end "like 20", but with records 1 & 2 = 16 uncompressed
    # bytes the buggy break only fires when end <= 16; 20 would NOT reproduce the
    # bug. end=12 makes the uncompressed offset genuinely EXCEED the compressed
    # end, which is the condition described, so this test truly fails pre-fix.)
    fake_popen = make_fake_popen([("fail", 2), ("ok", None)])
    r = _make_reader(monkeypatch, fake_popen, bounded=False, end=12)

    with caplog.at_level(logging.WARNING):
        collected = [rec["v"] for rec in r.records]

    # The retry RESUMED and delivered the whole stream — no truncation.
    assert collected == [1, 2, 3, 4]
    assert len(fake_popen.calls) == 2
    # Second call resumes at byte 16 (len of records 1 + 2).
    assert _seek_offset(fake_popen.calls[1]) == "16"
    # Unbounded sources must NEVER pass -s (source.end is compressed bytes).
    for argv in fake_popen.calls:
        assert "-s" not in argv


def make_malformed_popen():
    """Fake bgzip that emits one valid then one MALFORMED line, then exits 0.

    The malformed line makes orjson.loads raise a JSONDecodeError from inside
    the read loop — a non-CalledProcessError. Retry is scoped strictly to
    CalledProcessError, so this must propagate immediately: no retry, no
    WARNING, exactly one Popen call.
    """

    class FakePopen:
        calls = []

        def __init__(self, command, stdout=None, stderr=None, env=None):
            FakePopen.calls.append(list(command))
            self.stdout = iter([b'{"v":1}\n', b'not-json\n'])
            self.stderr = _FakeStderr(b"")
            self.returncode = None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def wait(self):
            self.returncode = 0

    return FakePopen


def test_non_calledprocesserror_propagates_without_retry(monkeypatch, caplog):
    # A malformed line raises orjson.JSONDecodeError (a ValueError, NOT a
    # CalledProcessError). The retry loop must not catch or re-run it.
    fake_popen = make_malformed_popen()
    r = _make_reader(monkeypatch, fake_popen)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(orjson.JSONDecodeError):
            list(r.records)

    # Exactly one Popen call — no retry was attempted.
    assert len(fake_popen.calls) == 1
    # Nothing logged at WARNING/ERROR (no retry, no give-up path taken).
    assert [rec for rec in caplog.records if rec.levelno >= logging.WARNING] == []
