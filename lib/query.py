import logging
import re

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
        raise ValueError(f'Arity mismatch for index schema "{index.schema}"')

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


def match(engine, index, q):
    """
    Returns a subset of unique keys that match the query.

    If the final column being indexed is a locus, it is an error and
    no keys will be returned.
    """
    if not (0 < len(q) <= len(index.schema.key_columns)):
        raise ValueError(f'Too many/few keys for index schema "{index.schema}"')

    # ensure the index is built
    if not index.built:
        raise ValueError(f'Index "{index.name}" is not built')

    # which column will be returned?
    distinct_column = index.schema.key_columns[len(q) - 1]

    # exact query parameters and match parameter
    tests = [f'`{k}` = %s' for k in index.schema.key_columns[:len(q) - 1]]

    # allow for wildcard to match all
    if q[len(q) - 1] != '*':
        tests.append(f'`{distinct_column}` >= %s')

    # build the SQL statement
    sql = (
        f'SELECT `{distinct_column}` FROM `{index.table}` '
        f'USE INDEX (`schema_idx`) '
    )

    # add match conditionals
    if len(tests) > 0:
        sql += f'WHERE {"AND".join(tests)} '

    # fetch all the results
    with engine.connect() as conn:
        cursor, query_ms = profile(conn.execution_options(stream_results=True).execute, sql, *q)
        match_string = q[len(q) - 1].lower()

        # output query performance
        logging.info('Match %s.%s took %d ms', index.table, distinct_column, query_ms)

        # yield all the results until no more matches
        for r in cursor:
            if not r[0].lower().startswith(match_string):
                break
            yield r[0]

        # no more matches
        cursor.close()


def _run_query(engine, bucket, index, q):
    """
    Construct a SQL query to fetch S3 objects and byte offsets. Run it and
    return a RecordReader to the results.
    """
    if not index.built:
        raise ValueError(f'Index "{index.name}" is not built')

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
