import click
import dotenv
import logging

import lib.config
import lib.index
import lib.metadata
import lib.query
import lib.secrets
import lib.s3


@click.group()
def cli():
    pass


@click.command(name='index')
@click.argument('index')
@click.confirmation_option(prompt='This will rebuild the index; continue? [y/N] ')
def cli_index(index):
    config = lib.config.Config()
    table = config.table(index)

    if not table:
        raise KeyError(f'Unknown index: {index}')

    # connect to mysql and get an s3 object listing
    engine = lib.secrets.connect_to_mysql(config.rds_instance)
    s3_objects = lib.s3.list_objects(config.s3_bucket, table.prefix, exclude='_SUCCESS')

    # build the index
    lib.index.build(engine, index, table.schema, config.s3_bucket, s3_objects)
    logging.info('Successfully built index.')


@click.command(name='query')
@click.argument('index')
@click.argument('q')
def cli_query(index, q):
    config = lib.config.Config()
    table = config.table(index)

    if not table:
        raise KeyError(f'Unknown index: {index}')

    # connect to mysql
    engine = lib.secrets.connect_to_mysql(config.rds_instance)

    # lookup the table class from the schema
    for obj in lib.query.fetch(engine, config.s3_bucket, index, table.schema, q):
        print(obj)


@click.command(name='keys')
@click.argument('index')
def cli_query(index):
    config = lib.config.Config()
    table = config.table(index)

    if not table:
        raise KeyError(f'Unknown index: {index}')

    # connect to mysql
    engine = lib.secrets.connect_to_mysql(config.rds_instance)

    # lookup the table class from the schema
    try:
        for obj in lib.query.keys(engine, index, table.schema):
            print(obj)
    except AssertionError:
        logging.error('Index %s is not indexed by value!', index)


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

    # run command
    cli()
