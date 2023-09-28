import concurrent.futures
import re

from sqlalchemy import text

from .locus import Locus, parse_region_string
from .reader import MultiRecordReader, RecordReader, RecordSource
from .s3 import list_objects


def fetch(config, engine, index, q, restricted=None):
    """
    Use the table schema to determine the type of query to execute. Returns
    a RecordReader of all the results.
    """
    if len(q) != index.schema.arity:
        raise ValueError(f'Arity mismatch for index schema "{index.schema}"')

    # execute the query and fetch the records from s3
    return _run_query(config, engine, index, q, restricted)


def fetch_multi(executor, config, engine, index, queries, restricted=None):
    """
    Run multiple queries in parallel and chain the readers returned
    into a single reader.
    """
    jobs = [executor.submit(fetch, config, engine, index, q, restricted) for q in queries]

    # wait for them to complete and get the readers for each
    done = concurrent.futures.as_completed(jobs)
    readers = [d.result() for d in done]

    # chain the records together
    return MultiRecordReader(readers)


def fetch_all(config, index, restricted=None, key_limit=None):
    """
    Scans for all the S3 files in the schema and creates a dummy cursor
    to read all the records from all the files. Returns a RecordReader
    of the results.
    """
    s3_objects = list_objects(config.s3_bucket, index.s3_prefix, max_keys=key_limit)

    # arbitrarily limit the number of keys
    if key_limit:
        s3_objects = [o[1] for o in zip(range(key_limit), s3_objects)]

    # create a RecordSource for each object
    sources = [RecordSource.from_s3_object(obj) for obj in s3_objects]

    # create the reader object, begin reading the records
    return RecordReader(config, sources, index, restricted=restricted)


def count(config, engine, index, q):
    """
    Estimate the number of records that will be returned by a query.
    """
    reader = fetch_all(config, index) if len(q) == 0 else _run_query(config, engine, index, q, None)

    # read a couple hundred records to get the total bytes read
    records = list(zip(range(500), reader.records))

    # if less than N records read, that's how many there are exactly
    if reader.at_end:
        return reader.count

    # get the % of bytes read to estimate the total number of records
    return int(len(records) * reader.bytes_total / reader.bytes_read)


def match(config, engine, index, q):
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
    tests = [f'`{k}` = :{k}' for k in index.schema.key_columns[:len(q) - 1]]

    # append the matching query
    tests.append(f'`{distinct_column}` LIKE :{distinct_column}')

    # build the SQL statement
    sql = (
        f'SELECT `{distinct_column}` FROM `{index.table}` '
        f'USE INDEX (`schema_idx`) '
    )

    # add match conditionals
    if len(tests) > 0:
        sql += f'WHERE {" AND ".join(tests)} '

    # create the match pattern
    pattern = '%' if q[-1] in ['_', '*'] else re.sub(r'_|%|$', lambda m: f'%{m.group(0)}', q[-1])
    prev_key = None

    # fetch all the results
    with engine.connect() as conn:
        params = dict(zip(index.schema.key_columns, q[:-1]))
        params.update({distinct_column: pattern})
        cursor = conn.execution_options(stream_results=True).execute(text(sql), params)

        # yield all the results until no more matches
        for r in cursor:
            if r[0] != prev_key:
                yield r[0]

            # don't return this key again
            prev_key = r[0]


def _run_query(config, engine, index, q, restricted):
    """
    Construct a SQL query to fetch S3 objects and byte offsets. Run it and
    return a RecordReader to the results.
    """
    record_filter = None

    # validate the index
    if not index.built:
        raise ValueError(f'Index "{index.name}" is not built')

    # build the query
    sql = (
        f'SELECT `__Keys`.`key`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{index.table}` '
        f'INNER JOIN `__Keys` '
        f'ON `__Keys`.`id` = `{index.table}`.`key` '
        f'WHERE {index.schema.sql_filters} '
        f'GROUP BY `key` '
        f'ORDER BY `key` ASC'
    )

    # query parameter list
    query_params = q
    escaped_column_names = [col.replace("|", "_") for col in index.schema.schema_columns]
    # if the schema has a locus, parse the query parameter
    if index.schema.has_locus:
        if index.schema.locus_is_template:
            chromosome, start, stop = index.schema.locus_class(q[-1]).region()
        else:
            chromosome, start, stop = parse_region_string(q[-1], config)

        # positions are stepped, and need to be between stepped ranges
        step_start = (start // Locus.LOCUS_STEP) * Locus.LOCUS_STEP
        step_stop = (stop // Locus.LOCUS_STEP) * Locus.LOCUS_STEP

        # replace the last query parameter with the locus
        query_params = dict(zip(escaped_column_names, q[:-1]))
        query_params.update({"chromosome": chromosome, "start_pos": step_start, "end_pos": step_stop})

        # match templated locus or overlapping loci
        def overlaps(row):
            if index.schema.locus_is_template:
                return row[index.schema.locus_columns[0]] == q[-1]

            return index.schema.locus_of_row(row).overlaps(chromosome, start, stop)

        # filter records read by locus
        record_filter = overlaps

    with engine.connect() as conn:
        if isinstance(query_params, list):
            query_params = dict(zip(escaped_column_names, query_params))
        cursor = conn.execute(text(sql), query_params)
        rows = cursor.fetchall()

        # create a RecordSource for each entry in the database
        sources = [RecordSource(*row) for row in rows]

        # create the reader
        return RecordReader(
            config,
            sources,
            index,
            record_filter=record_filter,
            restricted=restricted,
        )
