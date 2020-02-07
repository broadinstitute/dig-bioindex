import botocore.exceptions
import concurrent.futures
import io
import json
import logging

import lib.locus
import lib.s3

from lib.profile import profile


def by_locus(engine, bucket, table, locus):
    """
    Query the database for all records that a region overlaps.
    """
    chromosome, start, stop = lib.locus.parse(locus, allow_ens_lookup=True)

    q = (
        f'SELECT path, MIN(offset), MAX(offset) + length AS END_OFFSET '
        f'FROM {table.name} '
        f'WHERE chromosome = %s AND position BETWEEN %s AND %s '
        f'GROUP BY path '
    )

    # fetch all the results
    cursor, query_ms = profile(engine.execute, q, chromosome, start, stop)
    logging.info('Query %s (%s) took %d ms', table.name, locus, query_ms)

    # create a thread pool to load records in parallel
    # ex = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    # read all the objects in s3 asynchronously
    for record in cursor:
        path, offset, end_offset = record

        try:
            content = lib.s3.read_object(bucket, path, offset=offset, length=end_offset-offset)

            for line in content.iter_lines():
                print(line)
                yield json.loads(line)

        except botocore.exceptions.ClientError:
            logging.error('Failed to read table %s; some records missing', table.path)
