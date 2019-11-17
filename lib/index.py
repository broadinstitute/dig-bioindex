import logging

from .locus import *
from .record import *
from .s3 import *
from .table import *


def index(redis_client, key, dialect, locus, bucket, s3_objs, header=None, update=False, new=False):
    """
    Index table records in s3 to redis.
    """
    locus_cols = parse_locus_columns(locus)
    locus_class = SNPLocus if locus_cols[2] is None else RegionLocus

    # tally record count
    n = 0

    # list all the input tables
    for obj in s3_objs:
        path, tag = obj['Key'], obj['ETag']

        # lookup the existing table and id (if already indexed)
        table_id, existing_table = redis_client.get_table_from_path(path)

        # stream the file from s3
        line_stream = LineStream(s3_uri(bucket, path))

        # if the dialect isn't json, it's csv, read the header
        if dialect != 'json':
            if header is None:
                header = next(csv.reader(line_stream, dialect))
            elif header != '-':
                header = header.split(',')

        # create a new table object from the parameters
        table = Table(path=path, tag=tag, key=key, locus=locus, dialect=dialect, fieldnames=header)

        # if nothing has changed, don't re-index
        if table == existing_table:
            continue

        # register a new table if not already indexed
        if table_id is None:
            table_id = redis_client.register_table(table)

        # skip already existing tables or die
        if existing_table:
            if update:
                logging.info('Deleting %s...', path)

                # delete records, but not the table entry; re-use it
                redis_client.delete_records(table_id)
            elif not new:
                raise AssertionError(f'Table {path} already indexed; --new/update not provided')
            else:
                continue

        # Show that the table is now being indexed
        logging.info('Indexing %s...', path)

        # create the record reader
        reader = table.reader(line_stream)
        records = {}

        # accumulate the records
        for row in reader:
            try:
                locus_obj = locus_class(*(row.get(col) for col in locus_cols if col))
                records[locus_obj] = Record(table_id, line_stream.offset, line_stream.length)

                # tally record
                n += 1
            except (KeyError, ValueError) as e:
                logging.warning('Record error (line %d): %s; skipping...', line_stream.n, e)

        # add them all in a single batch
        redis_client.insert_records(key, records)

    return n
