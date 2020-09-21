import click
import dotenv
import logging
import orjson
import rich.console
import rich.logging
import rich.table
import sys
import uvicorn

from .lib import aws
from .lib import config
from .lib import index
from .lib import query
from .lib import s3
from .lib import schema
from .lib import tables

# create the global console
console = rich.console.Console()


@click.group()
def cli():
    pass


@click.command(name='serve')
@click.option('--port', '-p', type=int, default=5000)
@click.option('--env-file', '-e', type=str, default='.env')
def cli_serve(port, env_file):
    uvicorn.run(
        'bioindex.server:app',
        host='0.0.0.0',
        port=port,
        env_file=env_file,
        log_level='info',
    )


@click.command(name='create')
@click.argument('index_name')
@click.argument('s3_prefix')
@click.argument('index_schema')
@click.confirmation_option(prompt='This will create/update an index; continue?')
@click.pass_obj
def cli_create(cfg, index_name, s3_prefix, index_schema):
    engine = aws.connect_to_rds(cfg.rds_instance, schema=cfg.bio_schema)

    # parse the schema to ensure validity; create the index
    try:
        tables.create_index(engine, index_name, s3_prefix, schema.Schema(index_schema))

        # successfully completed
        logging.info('Done; build with `index %s`', index_name)
    except AssertionError as e:
        logging.error('Failed to create index %s: %s', index_name, e)


@click.command(name='list')
@click.pass_obj
def cli_list(cfg):
    engine = aws.connect_to_rds(cfg.rds_instance, schema=cfg.bio_schema)
    indexes = tables.list_indexes(engine, False)

    table = rich.table.Table(title='Indexes')
    table.add_column('Last Built')
    table.add_column('Index')
    table.add_column('S3 Prefix')
    table.add_column('Schema')

    for index in sorted(indexes, key=lambda i: i.name):
        built = f'[green]{index.built}[/]' if index.built else '[red]Not built[/]'
        table.add_row(built, index.name, index.s3_prefix, str(index.schema))

    console.print(table)


@click.command(name='index')
@click.argument('index_name')
@click.option('--cont', '-c', is_flag=True)
@click.option('--rebuild', '-r', is_flag=True)
@click.option('--use-lambda', '-l', is_flag=True)
@click.option('--workers', '-w', type=int, default=3)
@click.confirmation_option(prompt='This will build the index; continue? ')
@click.pass_obj
def cli_index(cfg, index_name, use_lambda, cont, rebuild, workers):
    engine = aws.connect_to_rds(cfg.rds_instance, schema=cfg.bio_schema)

    # handle mutually exclusive options
    if rebuild and cont:
        logging.error('Cannot supply both --rebuild and --cont')
        return

    # if --use-lambda specified, then ensure that config options are set
    if use_lambda:
        assert cfg.lambda_function, 'BIOINDEX_LAMBDA_FUNCTION not set; cannot use --use-lambda'

    # discover which indexes will be indexes
    indexes = tables.list_indexes(engine, filter_built=False)
    index_names = index_name.split(',')

    # which tables will be indexed? allow all with "*"
    if '*' not in index_names:
        indexes = [i for i in indexes if i.name in index_names]

    for idx in indexes:
        try:
            logging.info(f'{"Rebuilding" if rebuild else "Updating"} index {idx.name}')

            # get an s3 object listing
            s3_objects = s3.list_objects(cfg.s3_bucket, idx.s3_prefix, exclude='_SUCCESS')

            # build the index
            index.build(
                engine,
                cfg,
                idx,
                s3_objects,
                use_lambda=use_lambda,
                rebuild=rebuild,
                cont=cont,
                workers=workers,
                console=console,
            )
        except AssertionError as e:
            logging.error(f'Failed to build index %s: %s', idx.name, e)

    # finished building all indexes
    logging.info('Done')


@click.command(name='query')
@click.argument('index_name')
@click.argument('q')
@click.pass_obj
def cli_query(cfg, index_name, q):
    engine = aws.connect_to_rds(cfg.rds_instance, schema=cfg.bio_schema)
    idx = tables.lookup_index(engine, index_name)

    # query the index
    reader = query.fetch(engine, cfg.s3_bucket, idx, q.split(','))

    # dump all the records
    for record in reader.records:
        console.print(orjson.dumps(record).decode('utf-8'))


@click.command(name='all')
@click.argument('index_name')
@click.pass_obj
def cli_all(cfg, index_name):
    engine = aws.connect_to_rds(cfg.rds_instance, schema=cfg.bio_schema)
    idx = tables.lookup_index(engine, index_name)

    # read all records
    reader = query.fetch_all(cfg.s3_bucket, idx.s3_prefix)

    # lookup the table class from the schema
    for record in reader.records:
        console.print(orjson.dumps(record).decode('utf-8'))


@click.command(name='count')
@click.argument('index_name')
@click.argument('q', nargs=-1)
@click.pass_obj
def cli_count(cfg, index_name, q):
    engine = aws.connect_to_rds(cfg.rds_instance, schema=cfg.bio_schema)
    idx = tables.lookup_index(engine, index_name)

    # query the index
    count = query.count(engine, cfg.s3_bucket, idx, q)
    console.print(count)


@click.command(name='match')
@click.argument('index_name')
@click.argument('q', nargs=-1)
@click.pass_obj
def cli_match(cfg, index_name, q):
    engine = aws.connect_to_rds(cfg.rds_instance, schema=cfg.bio_schema)
    idx = tables.lookup_index(engine, index_name)

    # lookup the table class from the schema
    try:
        for obj in query.match(engine, idx, q):
            console.print(obj)
    except AssertionError:
        console.log(f'Index {index_name} is not indexed by value!')


# initialize the cli
cli.add_command(cli_serve)
cli.add_command(cli_create)
cli.add_command(cli_list)
cli.add_command(cli_index)
cli.add_command(cli_query)
cli.add_command(cli_all)
cli.add_command(cli_count)
cli.add_command(cli_match)


def main():
    """
    CLI entry point.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[rich.logging.RichHandler(console=console)],
    )

    # disable info logging for 3rd party modules
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)

    # load dot files
    dotenv.load_dotenv('.env')
    dotenv.load_dotenv('.bioindex')

    # run
    cli(obj=config.Config())


if __name__ == '__main__':
    main()
