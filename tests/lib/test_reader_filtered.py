from unittest.mock import MagicMock

import bioindex.lib.reader as reader_mod
from bioindex.lib.reader import RecordReader, RecordSource


def test_filtered_count_counts_rejected_records(monkeypatch):
    lines = ['{"v": 1}', '{"v": 2}', '{"v": 3}']
    monkeypatch.setattr(reader_mod, "read_lined_object", lambda *a, **k: iter(lines))

    cfg = MagicMock()
    cfg.s3_bucket = "b"
    idx = MagicMock()
    idx.compressed = False

    src = RecordSource(key="k", start=0, end=1000, bounded=True)
    r = RecordReader(cfg, [src], idx, record_filter=lambda rec: rec["v"] != 2)

    kept = list(r.records)
    assert [g["v"] for g in kept] == [1, 3]
    assert r.count == 2
    assert r.filtered_count == 1


def test_matched_bytes_counts_only_kept_records(monkeypatch):
    lines = ['{"v": 1}', '{"v": 2}', '{"v": 3}']  # equal-length lines
    monkeypatch.setattr(reader_mod, "read_lined_object", lambda *a, **k: iter(lines))

    cfg = MagicMock()
    cfg.s3_bucket = "b"
    idx = MagicMock()
    idx.compressed = False

    src = RecordSource(key="k", start=0, end=1000, bounded=True)
    r = RecordReader(cfg, [src], idx, record_filter=lambda rec: rec["v"] != 2)
    list(r.records)

    per_line = len('{"v": 1}'.encode("utf-8")) + 1  # matches reader's byte accounting
    # bytes_read counts every line decompressed; matched_bytes only the kept ones
    assert r.bytes_read == per_line * 3
    assert r.matched_bytes == per_line * 2
