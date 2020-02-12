import botocore.exceptions
import json
import logging

import lib.locus
import lib.s3
import lib.schema

from lib.profile import profile


def fetch(engine, bucket, table_name, schema, q):
    """
    Use the table schema to determine the type of query to execute.
    """
    if isinstance(schema, lib.schema.LocusSchema):
        chromosome, start, stop = lib.locus.parse(q, allow_ens_lookup=True)
        cursor = _run_locus_query(engine, table_name, chromosome, start, stop)

        def overlaps(row):
            return schema.locus_of_row(row).overlaps(chromosome, start, stop)

        # only keep records that overlap the queried region
        yield from filter(overlaps, _read_records(bucket, cursor))
    else:
        cursor = _run_value_query(engine, table_name, q)

        # read the records from the query results
        yield from _read_records(bucket, cursor)


def count(engine, bucket, table_name, schema, q):
    """
    Estimate the number of records that will be returned by a query.
    """
    if isinstance(schema, lib.schema.LocusSchema):
        chromosome, start, stop = lib.locus.parse(q, allow_ens_lookup=True)
        cursor = _run_locus_query(engine, table_name, chromosome, start, stop)

        return _count_records(bucket, cursor)
    else:
        cursor = _run_value_query(engine, table_name, q)
        return _count_records(bucket, cursor)


def keys(engine, table_name, schema):
    """
    Assumes schema is a ValueSchema and asserts if not. If so, it
    fetches all the distinct values available that can be queried
    from the table.
    """
    assert isinstance(schema, lib.schema.ValueSchema)

    sql = (
        f'SELECT DISTINCT `value` '
        f'FROM `{table_name}` '
        f'ORDER BY `value` ASC '
    )

    # fetch all the results
    cursor, query_ms = profile(engine.execute, sql)
    logging.info('Query %s (distinct values) took %d ms', table_name, query_ms)

    # yield all the results
    for r in cursor:
        yield r[0]


def _run_locus_query(engine, table_name, chromosome, start, stop):
    """
    Run a SQL query and return a results cursor for a region query.
    """
    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{table_name}` '
        f'WHERE `chromosome` = %s AND `position` BETWEEN %s AND (%s - 1) '
        f'GROUP BY `path` '
        f'ORDER BY `path` ASC '
    )

    # fetch all the results
    return engine.execute(sql, chromosome, start, stop)


def _run_value_query(engine, table_name, q):
    """
    Run a SQL query and return a results cursor for a value query.
    """
    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{table_name}` '
        f'WHERE `value` = %s '
        f'GROUP BY `path` '
        f'ORDER BY `path` ASC '
    )

    # fetch all the results
    return engine.execute(sql, q)


def _read_records(bucket, cursor):
    """
    Read the records from all the S3 objects in the cursor.
    """
    for path, start_offset, end_offset in cursor:
        yield from _read_records_from_s3(bucket, path, start_offset, end_offset)


def _read_records_from_s3(bucket, path, start_offset, end_offset):
    """
    Returns a generator that reads  all the records from a given object in
    S3 from a start to end byte offset.
    """
    length = end_offset - start_offset

    try:
        content = lib.s3.read_object(bucket, path, offset=start_offset, length=length)

        for line in content.iter_lines():
            yield json.loads(line)

    except botocore.exceptions.ClientError:
        logging.error('Failed to read table %s; some records missing', path)


def _count_records(bucket, cursor):
    """
    Read the first 100 records from S3, getting their average length in size.
    Use that length to extrapolate what the estimated number of records is.
    """
    total_bytes = 0
    lengths = []

    # collect all the files that need read
    record_sets = cursor.fetchall()

    # loop over all the record sets
    for path, start, end in record_sets:
        if len(lengths) < 100:
            for _, r in zip(range(100), _read_records_from_s3(bucket, path, start, end)):
                lengths.append(len(json.dumps(r)))

        # calculate the total number of bytes across all record sets
        total_bytes += end - start

    # it's an exact count if less than 100 records
    if len(lengths) < 100:
        return len(lengths)

    # get the average length per record
    avg_len = sum(lengths) / len(lengths)

    # return the estimated count
    return int(total_bytes / avg_len)
