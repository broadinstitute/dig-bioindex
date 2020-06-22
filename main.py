import click
import colorama
import dotenv
import json
import logging

import lib.config
import lib.create
import lib.index
import lib.query
import lib.s3
import lib.schema
import lib.secrets


@click.group()
def cli():
    pass


@click.command(name='create')
@click.argument('index')
@click.argument('s3_prefix')
@click.argument('schema')
@click.confirmation_option(prompt='This will create a new index; continue?')
def cli_create(index, s3_prefix, schema):
    config = lib.config.Config()
    engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)

    # parse the schema to ensure validity; create the index
    try:
        lib.create.create_index(engine, index, s3_prefix, lib.schema.Schema(schema))

        # successfully completed
        logging.info('Done; build with `index %s`', index)
    except AssertionError as e:
        logging.error('Failed to create index %s: %s', index, e)


@click.command(name='list')
def cli_list():
    config = lib.config.Config()
    engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)
    indexes = lib.create.list_indexes(engine, False)

    for index in indexes:
        mark = "\u2713" if index.built else "\u2717"
        print(f'{mark} {index.name}')


@click.command(name='index')
@click.argument('index')
@click.confirmation_option(prompt='This will rebuild the index; continue? ')
def cli_index(index):
    config = lib.config.Config()
    engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)

    # which tables will be indexed? allow "all" with "*"
    indexes = [i.name for i in lib.create.list_indexes(engine)] if index == '*' else index.split(',')

    for i in indexes:
        idx = lib.create.lookup_index(engine, i)

        if not idx:
            raise KeyError(f'Unknown index: {i}')

        try:
            # get an s3 object listing
            s3_objects = lib.s3.list_objects(config.s3_bucket, idx.s3_prefix, exclude='_SUCCESS')

            # build the index
            lib.index.build(engine, idx, config.s3_bucket, s3_objects)
            logging.info('Successfully built index.')
        except AssertionError as e:
            logging.error('Failed to build index %s: %s', i, e)

    # finished building all indexes
    logging.info('Done; query with `query <index>`')


@click.command(name='query')
@click.argument('index')
@click.argument('q', nargs=-1)
def cli_query(index, q):
    config = lib.config.Config()

    # connect to mysql and fetch the results
    engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)
    idx = lib.create.lookup_index(engine, index)

    # query the index
    reader = lib.query.fetch(engine, config.s3_bucket, idx, q)

    # dump all the records
    for record in reader.records:
        print(json.dumps(record))


@click.command(name='all')
@click.argument('index')
def cli_all(index):
    config = lib.config.Config()

    # connect to mysql and lookup the index
    engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)
    idx = lib.create.lookup_index(engine, index)

    # read all records
    reader = lib.query.fetch_all(config.s3_bucket, idx.s3_prefix)

    # lookup the table class from the schema
    for obj in reader.records:
        print(obj)


@click.command(name='count')
@click.argument('index')
@click.argument('q', nargs=-1)
def cli_count(index, q):
    config = lib.config.Config()

    # connect to mysql and fetch the results
    engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)
    idx = lib.create.lookup_index(engine, index)

    # query the index
    count = lib.query.count(engine, config.s3_bucket, idx, q)
    print(count)


@click.command(name='match')
@click.argument('index')
@click.argument('q', nargs=-1)
def cli_match(index, q):
    config = lib.config.Config()

    # connect to mysql and fetch the results
    engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)
    idx = lib.create.lookup_index(engine, index)

    # lookup the table class from the schema
    try:
        for obj in lib.query.match(engine, idx, q):
            print(obj)
    except AssertionError:
        logging.error('Index %s is not indexed by value!', index)


# initialize the cli
cli.add_command(cli_create)
cli.add_command(cli_list)
cli.add_command(cli_index)
cli.add_command(cli_query)
cli.add_command(cli_all)
cli.add_command(cli_count)
cli.add_command(cli_match)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-5s - %(message)s')

    # disable info logging for 3rd party modules
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)

    # load dot files
    dotenv.load_dotenv()
    dotenv.load_dotenv('.bioindex')

    # initialize ansi terminal
    colorama.init()

    try:
        cli()
    finally:
        colorama.deinit()
