import logging


def check_tables(redis_client, delete=False):
    """
    Looks up all the tables in redis and validates their integrity. Optionally
    deletes any tables that no longer exist.
    """
    for table_id in redis_client.scan_tables():
        table = redis_client.get_table(table_id)
        table_uri = '%s/%s' % (table.bucket, table.path)

        # if the table object exists in s3, continue to the next one
        if table.exists():
            logging.info('%s... OK', table_uri)
            continue

        # just warn unless the delete flag is present
        if not delete:
            logging.warning('%s... MISSING; use --delete to remove', table_uri)
            continue

        # indicate a delete it going to happen
        logging.info('%s... MISSING; deleting orphaned records...', table_uri)

        # remove all records for this table and the table itself
        redis_client.delete_table(table_id)
