import botocore.exceptions
import json
import logging

import lib.locus
import lib.metadata
import lib.s3

from lib.profile import profile


def fetch(engine, metadata, bucket, table, q):
    """
    Use the table schema to determine the type of query to execute.
    """
    schema = metadata.get(table.name)
    if schema is None:
        yield None

    # check if the schema is a valid locus
    locus_class, cols = lib.locus.parse_columns(schema)

    if locus_class:
        yield from _by_locus(engine, bucket, table, q, cols)
    else:
        yield from _by_value(engine, bucket, table, q, schema)


def _by_locus(engine, bucket, table, q, cols):
    """
    Query the database for all records that a region overlaps.
    """
    chromosome, start, stop = lib.locus.parse(q, allow_ens_lookup=True)

    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{table.name}` '
        f'WHERE `chromosome` = %s AND `position` BETWEEN %s AND (%s - 1) '
        f'GROUP BY `path` '
    )

    # fetch all the results
    cursor, query_ms = profile(engine.execute, sql, chromosome, start, stop)
    logging.info('Query %s (%s) took %d ms', table.name, q, query_ms)

    # read all the objects in s3
    for r in _read_records(bucket, cursor):
        if r[cols[0]] != chromosome or r[cols[1]] >= stop or (cols[2] and r[cols[2]] < start):
            continue

        yield r


def _by_value(engine, bucket, table, q, schema):
    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{table.name}` '
        f'WHERE `value` = %s '
        f'GROUP BY `path` '
    )

    # fetch all the results
    cursor, query_ms = profile(engine.execute, sql, q)
    logging.info('Query %s (%s) took %d ms', table.name, q, query_ms)

    # read all the objects from s3
    yield from _read_records(bucket, cursor)


def _read_records(bucket, cursor):
    """
    Read the records from all the S3 objects in the cursor.
    """
    for path, start_offset, end_offset in cursor:
        length = end_offset - start_offset

        try:
            content = lib.s3.read_object(bucket, path, offset=start_offset, length=length)

            for line in content.iter_lines():
                yield json.loads(line)

        except botocore.exceptions.ClientError:
            logging.error('Failed to read table %s; some records missing', path)
