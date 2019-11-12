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

        # lookup table info
        table = tables[tid]
        table_bucket = table[b'bucket'].decode('utf-8')
        table_key = table[b'key'].decode('utf-8')
        table_locus = table[b'locus'].decode('utf-8')

        # parse the table locus into columns
        locus_cols = parse_locus_columns(table_locus)

        # return the locus columns and the s3 body
        return locus_cols, s3_read_object(table_bucket, table_key, offset, length)

    # create a thread pool to load records in parallel
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=10)

    # fetch the records from s3 in parallel
    for locus_cols, s3_body in ex.map(read_records, ranges):
        text = io.BytesIO(s3_body.read())

        for record in text:
            record = json.loads(record)

            # Due to region buckets and coalescing, not all regions returned
            # by the query and read are guaranteed to be overlapped by the
            # input region.

            if Locus.from_record(record, *locus_cols).overlaps(chromosome, start, stop):
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
