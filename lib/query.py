import botocore.exceptions
import json
import logging

import lib.locus
import lib.metadata
import lib.s3
import lib.schema

from lib.profile import profile


def fetch(engine, bucket, table_name, schema, q):
    """
    Use the table schema to determine the type of query to execute.
    """
    if isinstance(schema, lib.schema.LocusSchema):
        yield from _by_locus(engine, bucket, table_name, schema, q)
    else:
        yield from _by_value(engine, bucket, table_name, q)


def _by_locus(engine, bucket, table_name, schema, q):
    """
    Query the database for all records that a region overlaps.
    """
    chromosome, start, stop = lib.locus.parse(q, allow_ens_lookup=True)

    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{table_name}` '
        f'WHERE `chromosome` = %s AND `position` BETWEEN %s AND (%s - 1) '
        f'GROUP BY `path` '
        f'ORDER BY `path` ASC '
    )

    # fetch all the results
    cursor, query_ms = profile(engine.execute, sql, chromosome, start, stop)
    logging.info('Query %s (%s) took %d ms', table_name, q, query_ms)

    # read all the objects in s3, return those that overlap
    for r in _read_records(bucket, cursor):
        if schema.locus_of_row(r).overlaps(chromosome, start, stop):
            yield r


def _by_value(engine, bucket, table_name, q):
    sql = (
        f'SELECT `path`, MIN(`start_offset`), MAX(`end_offset`) '
        f'FROM `{table_name}` '
        f'WHERE `value` = %s '
        f'GROUP BY `path` '
        f'ORDER BY `path` ASC '
    )

    # fetch all the results
    cursor, query_ms = profile(engine.execute, sql, q)
    logging.info('Query %s (%s) took %d ms', table_name, q, query_ms)

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
