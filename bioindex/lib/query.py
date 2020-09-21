import re

from .locus import Locus, parse_locus
from .reader import RecordReader, RecordSource
from .s3 import list_objects


def fetch(engine, bucket, index, q, restricted=None):
    """
    Use the table schema to determine the type of query to execute. Returns
    a RecordReader of all the results.
    """
    if len(q) != index.schema.arity:
        raise ValueError(f'Arity mismatch for index schema "{index.schema}"')

    # execute the query and fetch the records from s3
    return _run_query(engine, bucket, index, q, restricted)


def fetch_all(bucket, s3_prefix, restricted=None):
    """
    Scans for all the S3 files in the schema and creates a dummy cursor
    to read all the records from all the files. Returns a RecordReader
    of the results.
    """
    s3_objects = list_objects(bucket, s3_prefix)

    # create a RecordSource for each object
    sources = [RecordSource.from_s3_object(obj) for obj in s3_objects]

    # create the reader object, begin reading the records
    return RecordReader(bucket, sources, restricted=restricted)


def count(engine, bucket, index, q):
    """
    Estimate the number of records that will be returned by a query.
    """
    reader = fetch_all(bucket, index.s3_prefix) if len(q) == 0 else _run_query(engine, bucket, index, q, None)

    # read a couple hundred records to get the total bytes read
    records = list(zip(range(500), reader.records))

    # if less than N records read, that's how many there are exactly
    if reader.at_end:
        return reader.count

    # get the % of bytes read to estimate the total number of records
    return int(len(records) * reader.bytes_total / reader.bytes_read)


def match(engine, index, q):
    """
    Returns a subset of unique keys that match the query.

    If the final column being indexed is a locus, it is an error and
    no keys will be returned.
    """
    if not 0 < len(q) <= len(index.schema.key_columns):
        raise ValueError(f'Too few/many keys for index schema "{index.schema}"')

    # ensure the index is built
    if not index.built:
        raise ValueError(f'Index "{index.name}" is not built')

    # which column will be returned?
    distinct_column = index.schema.key_columns[len(q) - 1]

    # exact query parameters and match parameter
    tests = [f'`{k}` = %s' for k in index.schema.key_columns[:len(q) - 1]]

    # append the matching query
    tests.append(f'`{distinct_column}` LIKE %s')

    # build the SQL statement
    sql = (
        f'SELECT `{distinct_column}` FROM `{index.table}` '
        f'USE INDEX (`schema_idx`) '
    )

    # add match conditionals
    if len(tests) > 0:
        sql += f'WHERE {" AND ".join(tests)} '

    # create the match pattern
    pattern = '%' if q[-1] in ['_', '*'] else  re.sub(r'_|%|$', lambda m: f'%{m.group(0)}', q[-1])
    prev_key = None

    # fetch all the results
    with engine.connect() as conn:
        cursor = conn.execution_options(stream_results=True).execute(sql, *q[:-1], pattern)

        # yield all the results until no more matches
        for r in cursor:
            if r[0] != prev_key:
                yield r[0]

            # don't return this key again
            prev_key = r[0]


def _run_query(engine, bucket, index, q, restricted):
    """
    Construct a SQL query to fetch S3 objects and byte offsets. Run it and
    return a RecordReader to the results.
    """
    if not index.built:
        raise ValueError(f'Index "{index.name}" is not built')

    sql = (
        f'SELECT `__Keys`.`key`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{index.table}` '
        f'INNER JOIN `__Keys` '
        f'ON `__Keys`.`id` = `{index.table}`.`key` '
        f'WHERE {index.schema.sql_filters} '
        f'GROUP BY `key` '
        f'ORDER BY `key` ASC'
    )

    record_filter = None

    # if the schema has a locus, parse the query parameter
    if index.schema.has_locus:
        chromosome, start, stop = parse_locus(q[-1], gene_lookup_engine=engine)

        # positions are stepped, and need to be between stepped ranges
        step_start = (start // Locus.LOCUS_STEP) * Locus.LOCUS_STEP
        step_stop = (stop // Locus.LOCUS_STEP) * Locus.LOCUS_STEP

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
    sources = [RecordSource(*row) for row in rows]

    # create the reader
    return RecordReader(
        bucket,
        sources,
        record_filter=record_filter,
        restricted=restricted,
    )
