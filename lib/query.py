import botocore.exceptions
import json
import logging

import lib.locus
import lib.s3
import lib.schema

from lib.profile import profile


def fetch(engine, bucket, table_name, schema, q):
    """
    Use the table schema to determine the type of query to execute. The
    query should be either a tuple of parameters, where each parameter
    is a column or locus in the schema's index.
    """
    if len(q) != schema.arity:
        raise ValueError(f'Arity mismatch of query parameters for index: {schema}')

    # execute the query and fetch the records from s3
    cursor, locus_filter = _run_query(engine, bucket, table_name, schema, q)
    records = _read_records(bucket, cursor)

    # apply the locus filter
    if locus_filter:
        yield from filter(locus_filter, records)
    else:
        yield from records


def fetch_all(bucket, s3_prefix):
    """
    Scans for all the S3 files in the schema and creates a dummy cursor
    to read all the records from all the files.
    """
    for s3_obj in lib.s3.list_objects(bucket, s3_prefix):
        yield from _read_records_from_s3(bucket, s3_obj['Key'], None, None)


def count(engine, bucket, table_name, schema, q):
    """
    Estimate the number of records that will be returned by a query.
    """
    cursor, _ = _run_query(engine, bucket, table_name, schema, q)

    # estimate the count
    return _count_records(bucket, cursor)


def keys(engine, table_name, schema, q):
    """
    Returns all the unique keys within an index. If it's a compound
    index, then for every query parameter present, the keys for those
    parameters will be returned instead.

    If the final column being indexed is a locus, it is an error and
    no keys will be returned.
    """
    if len(q) >= len(schema.key_columns):
        raise ValueError(f'Too many key parameters for index: {schema}')

    # which column will be returned?
    distinct_column = schema.key_columns[len(q)]

    # filter query parameters
    tests = [f'{k} = %s ' for k in schema.key_columns[:len(q)]]

    # build the SQL statement
    sql = f'SELECT DISTINCT `{distinct_column}` FROM `{table_name}` '

    # if there are any keys provided, add the conditionals
    if len(tests) > 0:
        sql += f'WHERE {"AND".join(tests)} '

    # order the results
    sql += f'ORDER BY `{distinct_column}` ASC'

    # fetch all the results
    cursor, query_ms = profile(engine.execute, sql, *q)
    logging.info('Query %s (distinct values) took %d ms', table_name, query_ms)

    # yield all the results
    for r in cursor:
        yield r[0]


def _run_query(engine, bucket, table_name, schema, q):
    """
    Construct a SQL query to fetch S3 objects and byte offsets. Run it and
    return a cursor to the results along with an optional filter function
    if the schema contains a locus.
    """
    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{table_name}` '
        f'WHERE {schema.sql_filters} '
        f'GROUP BY `path` '
        f'ORDER BY `path` ASC'
    )

    # if the schema has a locus, parse the query parameter
    if schema.has_locus:
        chromosome, start, stop = lib.locus.parse(q[-1], allow_ens_lookup=True)

        # positions are stepped, and need to be between stepped ranges
        step_start = (start // lib.locus.Locus.LOCUS_STEP) * lib.locus.Locus.LOCUS_STEP
        step_stop = (stop // lib.locus.Locus.LOCUS_STEP) * lib.locus.Locus.LOCUS_STEP

        # replace the last query parameter with the locus
        q = [*q[:-1], chromosome, step_start, step_stop]

        # don't return rows that fail to overlap the locus
        def overlaps(row):
            return schema.locus_of_row(row).overlaps(chromosome, start, stop)

        # execute the query
        return engine.execute(sql, *q), overlaps

    # execute the query
    return engine.execute(sql, *q), None


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
    length = end_offset
    if length and start_offset:
        length -= start_offset

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
