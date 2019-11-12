import logging
import smart_open

from .locus import *
from .s3 import *
from .schema import *


def index(redis_client, key, locus, bucket, prefix, only=None, exclude=None):
    """
    Index table records in s3 to redis.
    """
    locus_cols = parse_locus_columns(locus)
    locus_class = SNPLocus if locus_cols[2] is None else RegionLocus

    # tally record count
    n = 0

    # list all the input tables
    for obj in s3_list_objects(bucket, prefix, only=only, exclude=exclude):
        logging.info('Indexing %s...', obj)

        # register the table in the db
        table_id = redis_client.register_table(bucket, obj, locus)

        # open the input table and read each record
        fp = smart_open.open(s3_uri(bucket, obj))
        offset = 0
        records = {}

        # accumulate the records
        for line in fp:
            row = json.loads(line)
            length = len(line)

            # extract the locus from the row
            try:
                locus_obj = locus_class(*(row.get(col) for col in locus_cols if col))
                records[locus_obj] = (table_id, offset, length)

                # tally record
                n += 1
            except ValueError:
                pass

            # increase offset to next record
            offset += length

        # add them all in a single batch
        redis_client.insert_records(key, records)

    return n
