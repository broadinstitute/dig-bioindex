import concurrent.futures
import json
import msgpack

from .s3 import *


def query(redis_client, key, chromosome, start, stop):
    """
    Query redis db for all objects that a region overlaps.
    """
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=20)

    # results are locus -> record pairs
    table_ids, records = redis_client.query_records(key, chromosome, start, stop)
    tables = redis_client.get_tables(table_ids)

    # download the record from the table
    def load_record(r):
        table_id, offset, length = r

        s3_bucket = tables[table_id][b'bucket'].decode('utf-8')
        s3_key = tables[table_id][b'key'].decode('utf-8')

        return s3_read_object(s3_bucket, s3_key, offset=offset, length=length)

    # fetch the records from s3 in parallel
    for record in ex.map(load_record, records):
        yield json.load(record)
