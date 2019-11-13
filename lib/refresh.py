import logging

from .s3 import *


def check_tables(redis_client, bucket, delete=False):
    """
    Looks up all the tables in redis and validates their integrity. Optionally
    deletes any tables that no longer exist.
    """
    for table_id in redis_client.scan_tables():
        table = redis_client.get_table(table_id)

        # if the table object exists in s3, continue to the next one
        if table.exists(bucket):
            logging.info('%s... OK', table.path)
            continue

        # just warn unless the delete flag is present
        if not delete:
            logging.warning('%s... MISSING; use --delete to remove', table.path)
            continue

        # indicate a delete it going to happen
        logging.info('%s... MISSING; deleting orphaned records...', table.path)

        # remove all records for this table and the table itself
        redis_client.delete_table(table_id)


def refresh_tables(redis_client, bucket, prefix):
    """
    Check tables with a given prefix. If they no longer exist, delete them,
    and then index new tables. Similar to running check and then index --new,
    but only checks a subset of tables and assumes the same key space for
    new tables as the existing ones.
    """
    for table_id in s3_list_objects(bucket, prefix=prefix):
        table = redis_client.get_table(table_id)
