import concurrent.futures
import io
import json

from .locus import *
from .s3 import *


def query(redis_client, key, chromosome, start, stop):
    """
    Query redis db for all objects that a region overlaps.
    """
    results = redis_client.query_records(key, chromosome, start, stop)

    # mapping of table info and all coalesced ranges
    tables = {}
    ranges = []

    # sort result ranges and fetch table location
    for table_id in results.keys():
        tables[table_id] = redis_client.get_table(table_id)

        # table_id may be represented once per coalesced range
        for r in coalesce_ranges(results[table_id]):
            ranges.append((table_id, r))

    # download the record from the table
    def read_records(coalesced_range):
        tid, (offset, length) = coalesced_range
        table = tables[tid]

        # parse all the records from the table
        stream = io.BytesIO(s3_read_object(table.bucket, table.path, offset, length).read())
        records = map(json.loads, stream.readlines())

        # parse the table locus into columns
        locus_cols = parse_locus_columns(table.locus)

        # tests to see if the record is overlapped by the region
        def overlapped(r):
            return Locus.from_record(r, *locus_cols). \
                overlaps(chromosome, start, stop)

        # final record list
        return filter(overlapped, records)

    # create a thread pool to load records in parallel
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=20)

    # each job (downloaded range of records) returns a record iterator
    for record_list in ex.map(read_records, ranges):
        for record in record_list:
            yield record


def coalesce_ranges(ranges):
    """
    Assuming all the ranges are to the same s3 object, sort the ranges and
    then coalesce nearby ranges together into larger ranges so that reads
    are faster.
    """
    ranges.sort()

    # handle degenerate case
    if len(ranges) == 0:
        return ranges

    # take the first item in the range
    coalesced = [ranges[0]]

    # attempt to merge next range with the previous
    for r in ranges[1:]:
        offset, length = coalesced[-1]

        if r[0] - (offset + length) < 1024:
            coalesced[-1] = (offset, r[0] + r[1] - offset)
        else:
            coalesced.append(r)

    return coalesced


def locus_overlaps(record, locus_cols, chromosome, start, stop):
    """
    Returns true if the record column(s) are overlapped by the region.
    """
    return Locus.from_record(record, *locus_cols). \
        overlaps(chromosome, start, stop)
