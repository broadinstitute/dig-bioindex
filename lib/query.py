import botocore.exceptions
import json
import logging

import lib.locus
import lib.s3
import lib.schema

from lib.profile import profile


def fetch(engine, bucket, table_name, schema, q, limit=None):
    """
    Use the table schema to determine the type of query to execute.
    """
    if isinstance(schema, lib.schema.LocusSchema):
        yield from _by_locus(engine, bucket, table_name, schema, q, limit)
    else:
        yield from _by_value(engine, bucket, table_name, q, limit)


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


def _by_locus(engine, bucket, table_name, schema, q, limit):
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

    def overlaps(row):
        return schema.locus_of_row(row).overlaps(chromosome, start, stop)

    # only keep records that overlap the queried region
    overlapping = filter(overlaps, _read_records(bucket, cursor))

    # arbitrarily limit the number of results
    if not limit:
        yield from overlapping
    else:
        for _, r in zip(range(limit), overlapping):
            yield r


def _by_value(engine, bucket, table_name, q, limit):
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
    if not limit:
        yield from _read_records(bucket, cursor)
    else:
        for _, r in zip(range(limit), _read_records(bucket, cursor)):
            yield r


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
