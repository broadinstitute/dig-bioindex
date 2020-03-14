import click
import colorama
import dotenv
import logging

import lib.config
import lib.index
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
    engine = lib.secrets.connect_to_mysql(config.rds_instance)

    # which tables will be indexed? allow "all" with "*"
    idx = list(config.tables.keys()) if index == '*' else index.split(',')

    for i in idx:
        table = config.table(i)

        if not table:
            raise KeyError(f'Unknown index: {i}')

        # connect to mysql and get an s3 object listing
        s3_objects = lib.s3.list_objects(config.s3_bucket, table.s3_prefix, exclude='_SUCCESS')

        # build the index
        lib.index.build(engine, i, table.schema, config.s3_bucket, s3_objects)
        logging.info('Successfully built index.')

    # finished building all indexes
    logging.info('Done')


@click.command(name='query')
@click.argument('index')
@click.argument('q', nargs=-1)
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


@click.command(name='all')
@click.argument('index')
@click.option('--limit', type=int)
def cli_all(index, limit):
    config = lib.config.Config()
    table = config.table(index)
    records = lib.query.fetch_all(config.s3_bucket, table.s3_prefix)

    # prevent an insane number of results
    if limit:
        records = map(lambda r: r[1], zip(range(limit), records))

    # lookup the table class from the schema
    for obj in records:
        print(obj)


@click.command(name='count')
@click.argument('index')
@click.argument('q', nargs=-1)
def cli_count(index, q):
    config = lib.config.Config()
    table = config.table(index)

    if not table:
        raise KeyError(f'Unknown index: {index}')

    # connect to mysql
    engine = lib.secrets.connect_to_mysql(config.rds_instance)

    # lookup the table class from the schema
    count = lib.query.count(engine, config.s3_bucket, index, table.schema, q)
    print(count)


@click.command(name='keys')
@click.argument('index')
@click.argument('q', nargs=-1)
def cli_keys(index, q):
    config = lib.config.Config()
    table = config.table(index)

    if not table:
        raise KeyError(f'Unknown index: {index}')

    # connect to mysql
    engine = lib.secrets.connect_to_mysql(config.rds_instance)

    # lookup the table class from the schema
    try:
        for obj in lib.query.keys(engine, index, table.schema, q):
            print(obj)
    except AssertionError:
        logging.error('Index %s is not indexed by value!', index)


# initialize the cli
cli.add_command(cli_index)
cli.add_command(cli_query)
cli.add_command(cli_all)
cli.add_command(cli_count)
cli.add_command(cli_keys)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-5s - %(message)s')

    # disable info logging for 3rd party modules
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)

    # load dot files
    dotenv.load_dotenv()

    # initialize ansi terminal
    colorama.init()

    try:
        cli()
    finally:
        colorama.deinit()
