import types

import pytest

from bioindex.api.bio import _take_page
from bioindex.lib.reader import RecordReader, RecordSource

# 100 records, every one the same size so byte counts are easy to reason about
RECORDS = b"".join(b'{"i":"%02d"}\n' % i for i in range(100))
RECORD_BYTES = 11  # '{"i":"NN"}' plus the newline
RESPONSE_LIMIT = 5 * RECORD_BYTES


@pytest.fixture(autouse=True)
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


def _one_in_ten(record):
    # the keeper is the LAST of each group of ten, so nine records are read
    # and discarded before the page gets its first one
    return int(record["i"]) % 10 == 9


def test_an_over_reading_query_still_fills_a_page():
    reader = _reader(_one_in_ten)

    page = list(_take_page(reader, RESPONSE_LIMIT))

    # finding even the first record costs 10x RESPONSE_LIMIT in bytes read, so
    # a page bounded on bytes_read would have stopped right there
    assert reader.bytes_read > RESPONSE_LIMIT
    assert len(page) == 6


def test_a_page_of_unfiltered_records_is_still_bounded_by_the_limit():
    reader = _reader()

    page = list(_take_page(reader, RESPONSE_LIMIT))

    # one past the limit: the record that crosses it is kept, not dropped
    assert len(page) == 6
    assert reader.matched_bytes == 6 * RECORD_BYTES


def test_a_short_source_ends_the_page_without_reaching_the_limit():
    reader = _reader(lambda r: int(r["i"]) < 3)

    page = list(_take_page(reader, RESPONSE_LIMIT))

    assert [int(r["i"]) for r in page] == [0, 1, 2]
    assert reader.at_end


def test_the_next_page_resumes_the_bound_from_what_is_already_matched():
    reader = _reader()

    first = list(_take_page(reader, RESPONSE_LIMIT))
    second = list(_take_page(reader, RESPONSE_LIMIT))

    assert len(first) == len(second) == 6
    # the second page continued rather than restarting
    assert [int(r["i"]) for r in second] == list(range(6, 12))
