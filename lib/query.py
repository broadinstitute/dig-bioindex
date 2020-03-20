import logging

import lib.locus
import lib.reader
import lib.s3
import lib.schema

from lib.profile import profile


def fetch(engine, bucket, index, q):
    """
    Use the table schema to determine the type of query to execute. Returns
    a RecordReader of all the results.
    """
    if len(q) != index.schema.arity:
        raise ValueError(f'Arity mismatch of query parameters for index: {index.schema}')

    # execute the query and fetch the records from s3
    return _run_query(engine, bucket, index, q)


def fetch_all(bucket, s3_prefix):
    """
    Scans for all the S3 files in the schema and creates a dummy cursor
    to read all the records from all the files. Returns a RecordReader
    of the results.
    """
    s3_objects = lib.s3.list_objects(bucket, s3_prefix)

    # create a RecordSource for each object
    sources = [lib.reader.RecordSource.from_s3_object(obj) for obj in s3_objects]

    # create the reader object, begin reading the records
    return lib.reader.RecordReader(bucket, sources)


def count(engine, bucket, index, q):
    """
    Estimate the number of records that will be returned by a query.
    """
    reader = _run_query(engine, bucket, index, q)

    # read a couple hundred records to get the total bytes read
    for _ in zip(range(500), reader.records):
        pass

    # if less than N records read, that's how many there are exactly
    if reader.at_end:
        return reader.count

    # get the % of bytes read to estimate the total number of records
    return int(reader.count * reader.bytes_total / reader.bytes_read)


def keys(engine, index, q):
    """
    Returns all the unique keys within an index. If it's a compound
    index, then for every query parameter present, the keys for those
    parameters will be returned instead.

    If the final column being indexed is a locus, it is an error and
    no keys will be returned.
    """
    if len(q) >= len(index.schema.key_columns):
        raise ValueError(f'Too many key parameters for index: {index.schema}')

    # which column will be returned?
    distinct_column = index.schema.key_columns[len(q)]

    # filter query parameters
    tests = [f'{k} = %s ' for k in index.schema.key_columns[:len(q)]]

    # build the SQL statement
    sql = f'SELECT DISTINCT `{distinct_column}` FROM `{index.table}` '

    # if there are any keys provided, add the conditionals
    if len(tests) > 0:
        sql += f'WHERE {"AND".join(tests)} '

    # order the results
    sql += f'ORDER BY `{distinct_column}` ASC'

    # fetch all the results
    cursor, query_ms = profile(engine.execute, sql, *q)
    logging.info('Query %s (distinct values) took %d ms', index.table, query_ms)

    # yield all the results
    for r in cursor:
        yield r[0]


def _run_query(engine, bucket, index, q):
    """
    Construct a SQL query to fetch S3 objects and byte offsets. Run it and
    return a RecordReader to the results.
    """
    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{index.table}` '
        f'WHERE {index.schema.sql_filters} '
        f'GROUP BY `path` '
        f'ORDER BY `path` ASC'
    )

    record_filter = None

    # if the schema has a locus, parse the query parameter
    if index.schema.has_locus:
        chromosome, start, stop = lib.locus.parse(q[-1], allow_ens_lookup=True)

        # positions are stepped, and need to be between stepped ranges
        step_start = (start // lib.locus.Locus.LOCUS_STEP) * lib.locus.Locus.LOCUS_STEP
        step_stop = (stop // lib.locus.Locus.LOCUS_STEP) * lib.locus.Locus.LOCUS_STEP

        # replace the last query parameter with the locus
        q = [*q[:-1], chromosome, step_start, step_stop]

        # don't return rows that fail to overlap the locus
        def overlaps(row):
            return index.schema.locus_of_row(row).overlaps(chromosome, start, stop)

        # filter records read by locus
        record_filter = overlaps

    # execute the query
    cursor = engine.execute(sql, *q)
    rows = cursor.fetchall()

    # create a RecordSource for each entry in the database
    sources = [lib.reader.RecordSource(*row) for row in rows]

    # create the reader
    return lib.reader.RecordReader(bucket, sources, record_filter=record_filter)
