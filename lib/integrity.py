import logging


def check_tables(redis_client, delete=False):
    """
    Looks up all the tables in redis and validates their integrity. Optionally
    deletes any tables that no longer exist.
    """
    for table_id in redis_client.scan_tables():
        table = redis_client.get_table(table_id)
        table_uri = '%s/%s...' % (table.bucket, table.path)

        logging.info('Checking %s...', table_uri)

        # if the table object exists in s3, continue to the next one
        if table.exists():
            logging.info('Table %s... OK', table_uri)
            continue

        # just warn unless the delete flag is present
        if not delete:
            logging.warning('Table %s... ERROR; use --delete to remove', table_uri)
            continue

        # TODO: delete
