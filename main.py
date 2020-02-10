import click
import dotenv
import logging
import os

import lib.index
import lib.metadata
import lib.mixins
import lib.query
import lib.schema
import lib.secrets
import lib.s3


@click.group()
def cli():
    pass


@click.command(name='index')
@click.argument('table')
@click.argument('bucket_prefix')
@click.argument('schema')
def cli_index(table, bucket_prefix, schema):
    cls = getattr(lib.schema, table)
    if not cls:
        logging.error('%s is not a valid index table!', table)
        return

    # get the bucket name and prefix
    bucket, prefix = lib.s3.split_bucket(bucket_prefix)
    if not bucket:
        logging.error('Invalid S3 location to index: %s', bucket_prefix)
        return

    # connect to the database and search s3 for all objects in the desired path
    engine = lib.secrets.connect_to_mysql(os.getenv('RDS_INSTANCE'))
    s3_objects = lib.s3.list_objects(bucket, prefix, exclude='_SUCCESS')

    # perform the index
    lib.index.build(engine, cls.__table__, schema, bucket, s3_objects)
    logging.info('Successfully built %s index.', table)


@click.command(name='query')
@click.argument('table')
@click.argument('bucket')
@click.argument('q')
def cli_query(table, bucket, q):
    cls = getattr(lib.schema, table)
    if not cls:
        logging.error('%s is not a valid index table!', table)
        return

    # connect to the database and read metadata
    engine = lib.secrets.connect_to_mysql(os.getenv('RDS_INSTANCE'))
    metadata = lib.metadata.load_metadata(engine)

    # lookup the table class from the schema
    for obj in lib.query.fetch(engine, metadata, bucket, cls.__table__, q):
        print(obj)


# initialize the cli
cli.add_command(cli_index)
cli.add_command(cli_query)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-5s - %(message)s')

    # disable info logging for 3rd party modules
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)

    # load dot files
    dotenv.load_dotenv()

    # verify environment
    assert os.getenv('RDS_INSTANCE'), 'RDS_INSTANCE not set in environment or .env'

    # connect to the MySQL database
    # engine = lib.secrets.connect_to_mysql(os.getenv('RDS_INSTANCE'))

    # run command
    cli()
