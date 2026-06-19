import subprocess
import types

import pytest

from bioindex.lib.reader import RecordReader, RecordSource

# Record at index 1 is terminated with a Windows CRLF (\r\n) instead of \n.
# On disk: rec0=8 bytes, rec1=9 bytes (CRLF), rec2 starts at byte 17.
CONTENT = b'{"i":0}\n{"i":1}\r\n{"i":2}\n{"i":3}\n'
REC2_OFFSET = 17


@pytest.fixture
def bgzf_path(tmp_path):
    raw = tmp_path / "data.json"
    raw.write_bytes(CONTENT)
    subprocess.run(["bgzip", "-k", str(raw)], check=True)
    gz = tmp_path / "data.json.gz"
    subprocess.run(["bgzip", "-r", str(gz)], check=True)  # .gzi for random access
    return gz


@pytest.fixture
def local_bgzip(monkeypatch, bgzf_path):
    """Run the real bgzip subprocess, but rewrite the s3:// URL the reader builds
    to our local bgzf file. The real Popen(text=...) decoding is what we're
    testing, so only the object location is faked."""
    real_popen = subprocess.Popen

    def wrapper(cmd, *args, **kwargs):
        cmd = [str(bgzf_path) if str(a).startswith("s3://") else a for a in cmd]
        return real_popen(cmd, *args, **kwargs)

    monkeypatch.setattr("bioindex.lib.reader.subprocess.Popen", wrapper)


def _reader(start_source_index=0, start_byte_offset=0):
    config = types.SimpleNamespace(s3_bucket="test-bucket")
    index = types.SimpleNamespace(compressed=True)
    source = RecordSource(key="data.json.gz", start=0, end=len(CONTENT), bounded=True)
    return RecordReader(config, [source], index,
                        start_source_index=start_source_index,
                        start_byte_offset=start_byte_offset)


def test_byte_offset_counts_true_bytes_through_crlf(local_bgzip):
    r = _reader()
    assert list(r.records) == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]
    # Must equal the real byte length (CRLF counted as 2 bytes), not 32.
    assert r._source_byte_offset == len(CONTENT)


def test_resume_after_crlf_record_lands_on_record_boundary(local_bgzip):
    # Page 1: read the first two records; the 2nd is CRLF-terminated.
    r1 = _reader()
    it = iter(r1.records)
    assert next(it) == {"i": 0}
    assert next(it) == {"i": 1}
    # The resume offset must be rec2's true start (17), not 16 (the stranded \n).
    assert r1._source_byte_offset == REC2_OFFSET

    # Page 2: resuming there must read rec2 cleanly, not choke on a lone newline.
    r2 = _reader(start_source_index=r1._source_index,
                 start_byte_offset=r1._source_byte_offset)
    assert next(iter(r2.records)) == {"i": 2}


# --- Uncompressed (--skip-compress) path -----------------------------------
# rec0 carries a 2-byte UTF-8 char (è, \xc3\xa8): as a str it is 1 char shorter
# than its byte length, so char-based counting under-counts it. The byte offsets
# stored in __Keys are written by index.py with len(line.encode('utf-8')) + 1,
# so the reader must use the identical accounting or resume lands mid-record.
# rec0 = 11 bytes on disk -> rec1 starts at byte 11.
UNCOMPRESSED = b'{"v":"\xc3\xa8"}\n{"v":"x"}\n'
REC1_OFFSET = 11


def _fake_read_lined_object(bucket, key, offset=None, length=None):
    """Slice the in-memory buffer like the real read_lined_object: honor the
    physical byte range, then decode utf-8 and drop the trailing newline."""
    start = offset or 0
    end = start + length if length is not None else len(UNCOMPRESSED)
    lines = UNCOMPRESSED[start:end].split(b'\n')
    if lines and lines[-1] == b'':
        lines = lines[:-1]
    return (l.decode('utf-8') for l in lines)


@pytest.fixture
def patch_uncompressed(monkeypatch):
    monkeypatch.setattr("bioindex.lib.reader.read_lined_object",
                        _fake_read_lined_object)


def _ureader(start_source_index=0, start_byte_offset=0):
    config = types.SimpleNamespace(s3_bucket="test-bucket")
    index = types.SimpleNamespace(compressed=False)
    source = RecordSource(key="data.json", start=0, end=len(UNCOMPRESSED), bounded=True)
    return RecordReader(config, [source], index,
                        start_source_index=start_source_index,
                        start_byte_offset=start_byte_offset)


def test_uncompressed_byte_offset_matches_writer_through_multibyte(patch_uncompressed):
    r = _ureader()
    it = iter(r.records)
    assert next(it) == {"v": "è"}
    # Must equal the writer's accounting (index.py: len(encode)+1) = 11, not 10.
    assert r._source_byte_offset == REC1_OFFSET


def test_uncompressed_resume_after_multibyte_lands_on_record_boundary(patch_uncompressed):
    r2 = _ureader(start_source_index=0, start_byte_offset=REC1_OFFSET)
    assert next(iter(r2.records)) == {"v": "x"}


# --- Multi-source resume: an index spans several S3 files -------------------
# Resume must pick up at the correct source_index — neither re-reading earlier
# sources nor skipping records across a file boundary. This is the real
# production shape: a built index has many .json objects under its prefix.
SRC_A = b'{"i":0}\n{"i":1}\n'  # source 0: two 8-byte lines
SRC_B = b'{"i":2}\n{"i":3}\n'  # source 1: two 8-byte lines
_MULTI_BUFS = {"a.json": SRC_A, "b.json": SRC_B}


def _fake_multi_read(bucket, key, offset=None, length=None):
    buf = _MULTI_BUFS[key]
    start = offset or 0
    end = start + length if length is not None else len(buf)
    lines = buf[start:end].split(b'\n')
    if lines and lines[-1] == b'':
        lines = lines[:-1]
    return (l.decode('utf-8') for l in lines)


@pytest.fixture
def patch_multi(monkeypatch):
    monkeypatch.setattr("bioindex.lib.reader.read_lined_object", _fake_multi_read)


def _multireader(start_source_index=0, start_byte_offset=0):
    config = types.SimpleNamespace(s3_bucket="test-bucket")
    index = types.SimpleNamespace(compressed=False)
    sources = [
        RecordSource(key="a.json", start=0, end=len(SRC_A), bounded=True),
        RecordSource(key="b.json", start=0, end=len(SRC_B), bounded=True),
    ]
    return RecordReader(config, sources, index,
                        start_source_index=start_source_index,
                        start_byte_offset=start_byte_offset)


def test_resume_at_second_source_no_dup_or_gap(patch_multi):
    # Baseline: a straight read across both files.
    assert list(_multireader().records) == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]

    # Page 1: read all of source 0 plus the first record of source 1.
    r1 = _multireader()
    it = iter(r1.records)
    assert [next(it) for _ in range(3)] == [{"i": 0}, {"i": 1}, {"i": 2}]
    # Cursor is now inside source 1, just past rec2 (one 8-byte line).
    assert r1._source_index == 1
    assert r1._source_byte_offset == len(b'{"i":2}\n')  # 8

    # Page 2: resuming at (source 1, offset 8) yields only the remaining
    # record — source 0 is not re-read and nothing is skipped.
    r2 = _multireader(start_source_index=r1._source_index,
                      start_byte_offset=r1._source_byte_offset)
    assert list(r2.records) == [{"i": 3}]
