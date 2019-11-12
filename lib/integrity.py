import logging


def check_tables(redis_client, delete=False):
    """
    Returns a map of table_id -> {} of tables that cannot be found in s3.
    """
    for table_id in redis_client.scan_tables():
        table = redis_client.get_table(table_id)

        # if the table object exists in s3, continue to the next one
        if table.exists():
            continue

        if delete:
            pass
        else:
            logging.warning('Table %s/%s does not exist and should be deleted')
