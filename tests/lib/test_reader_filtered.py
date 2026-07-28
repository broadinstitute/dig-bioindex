import types

import pytest

from bioindex.lib.reader import MultiRecordReader, RecordReader, RecordSource

# ten records, of which the filter below keeps two
RECORDS = b"".join(b'{"i":%d}\n' % i for i in range(10))
KEPT = {3, 7}
RECORD_BYTES = 8  # '{"i":N}' + newline


@pytest.fixture
def patch_uncompressed(monkeypatch):
    def fake_read_lined_object(bucket, key, offset=0, length=None):
        body = RECORDS[offset:offset + length if length else None]
        return iter(body.decode().splitlines())

    monkeypatch.setattr("bioindex.lib.reader.read_lined_object", fake_read_lined_object)


def _reader(record_filter=None):
    config = types.SimpleNamespace(s3_bucket="test-bucket")
    index = types.SimpleNamespace(compressed=False)
    source = RecordSource(key="data.json", start=0, end=len(RECORDS), bounded=True)
    return RecordReader(config, [source], index, record_filter=record_filter)


def _keeps_two(record):
    return record["i"] in KEPT


def test_filter_rejects_are_tallied_separately_from_keeps(patch_uncompressed):
    r = _reader(_keeps_two)
    assert [rec["i"] for rec in r.records] == sorted(KEPT)

    assert r.count == 2
    assert r.filtered_count == 8
    assert r.count + r.filtered_count == 10


def test_matched_bytes_counts_only_what_was_returned(patch_uncompressed):
    r = _reader(_keeps_two)
    list(r.records)

    # every record was decompressed, but only two were handed back
    assert r.bytes_read == 10 * RECORD_BYTES
    assert r.matched_bytes == 2 * RECORD_BYTES


def test_an_unfiltered_read_matches_every_byte_it_reads(patch_uncompressed):
    r = _reader()
    list(r.records)

    assert r.filtered_count == 0
    assert r.matched_bytes == r.bytes_read == 10 * RECORD_BYTES


def test_the_filter_is_applied_once_per_record(patch_uncompressed):
    # it used to run inline during the read AND again over the resulting
    # iterator, so every record was tested twice
    seen = []

    def counting_filter(record):
        seen.append(record["i"])
        return _keeps_two(record)

    list(_reader(counting_filter).records)

    assert seen == list(range(10))


def test_multi_reader_totals_its_readers(patch_uncompressed):
    readers = [_reader(_keeps_two), _reader(_keeps_two)]
    multi = MultiRecordReader(readers)
    list(multi.records)

    assert multi.filtered_count == 16
    assert multi.matched_bytes == 4 * RECORD_BYTES
