import concurrent
import time
from enum import Enum

import click
import dotenv
import graphql.utilities
import logging
import orjson
import pymysql
import rich.console
import rich.logging
import rich.table
import uvicorn

from .lib import config, aws
from .lib import index
from .lib import migrate
from .lib import ql
from .lib import query

# create the global console
console = rich.console.Console(markup=True, emoji=False)


@click.group()
@click.option('--env-file', '-e', type=str, default='.bioindex')
@click.pass_context
def cli(ctx, env_file):
    if env_file:
        logging.info('Loading %s environment variables...', env_file)
        dotenv.load_dotenv(env_file)

    # load the configuration into the click object
    ctx.obj = config.Config()


SERVER_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "access.log",
            "maxBytes": 1024 * 1024 * 100,  # 100 MB
            "backupCount": 3,  # keep 3 backup files
        }
    },
    "root": {"level": "INFO", "handlers": ["file"]},
    "loggers": {
        "uvicorn.access": {
            "level": "INFO",
            "handlers": ["file"],
            "propagate": True,
            "formatter": "apache",
        },
    },
    "formatters": {
        "apache": {
            "format": '%(message)s "%(status)d" %(bytes)d',
        },
    },
}


@click.command(name='serve')
@click.option('--port', '-p', type=int, default=5000)
def cli_serve(port):
    uvicorn.run(
        'bioindex.server:app',
        host='0.0.0.0',
        port=port,
        log_level='info',
        log_config=SERVER_LOGGING_CONFIG
    )


@click.command(name='create')
@click.argument('index_name')
@click.argument('rds_table_name')
@click.argument('s3_prefix')
@click.argument('index_schema')
@click.confirmation_option(prompt='This will create/update an index; continue?')
@click.pass_obj
def cli_create(cfg, index_name, rds_table_name, s3_prefix, index_schema):
    engine = migrate.migrate(cfg)

    # parse the schema to ensure validity; create the index
    try:
        index.Index.create(engine, index_name, rds_table_name, s3_prefix, index_schema)

        # successfully completed
        logging.info('Done; build with `index %s`', index_name)
    except AssertionError as e:
        logging.error('Failed to create index %s: %s', index_name, e)


@click.command(name='list')
@click.pass_obj
def cli_list(cfg):
    engine = migrate.migrate(cfg)
    indexes = index.Index.list_indexes(engine, False)

    table = rich.table.Table(title='Indexes')
    table.add_column('Last Built')
    table.add_column('Index')
    table.add_column('S3 Prefix')
    table.add_column('Schema')

    for i in sorted(indexes, key=lambda i: i.name):
        built = f'[green]{i.built}[/]' if i.built else '[red]Not built[/]'
        table.add_row(built, i.name, i.s3_prefix, str(i.schema))

    console.print(table)


@click.command(name='index')
@click.argument('index_name')
@click.option('--rebuild', '-r', is_flag=True)
@click.option('--use-lambda', '-l', is_flag=True)
@click.option('--workers', '-w', type=int, default=None)
@click.confirmation_option(prompt='This will build the index; continue? ')
@click.pass_obj
def cli_index(cfg, index_name, use_lambda, rebuild, workers):
    engine = migrate.migrate(cfg)

    # if --use-lambda specified, then ensure that config options are set
    if use_lambda:
        assert cfg.lambda_function, 'BIOINDEX_LAMBDA_FUNCTION not set; cannot use --use-lambda'

    # discover which indexes will be indexes
    idxs = index.Index.lookup_all(engine, index_name)

    # optional build arguments for build/rebuild function
    build_kwargs = {
        'console': console,
        'use_lambda': use_lambda,
        'workers': workers or (20 if use_lambda else 1),
    }

    # build each index specified
    try:
        for i in idxs:
            logging.info(f'Preparing index {i.name} with arity {i.schema.arity}')
            i.prepare(engine, rebuild=rebuild)
        for i in idxs:
            logging.info(f'{"Rebuilding" if rebuild else "Updating"} index {i.name} with arity {i.schema.arity}')

            # build the index
            i.build(cfg, engine, **build_kwargs)

            # finished building all indexes
            logging.info('Done')
    except AssertionError as e:
        logging.error(f'Failed to build index %s: %s', i.name, e)


class BgzipJobType(str, Enum):
    COMPRESS = 'bgzip-job'
    DECOMPRESS = 'unbgzip-job'
    DELETE_JSON = 'json-delete-job'


@click.command(name='compress')
@click.argument('index_name')
@click.argument('prefix')
@click.pass_obj
def cli_compress(cfg, index_name, prefix):
    check_index_and_launch_job(cfg, index_name, prefix, BgzipJobType.COMPRESS)


def validate_job_type(ctx, param, value):
    job_types = [job.name for job in BgzipJobType]
    if value not in job_types:
        raise click.BadParameter(f"Job type must be one of {', '.join(job_types)}")
    return getattr(BgzipJobType, value)


def convert_cli_arg_to_list(value):
    if value:
        return [item.strip() for item in value]
    return None


@click.command(name='bulk-compression-management')
@click.option('--include', multiple=True, default=None,
              help="List of index names to include for processing. E.g. --include=name1,name2")
@click.option('--exclude', multiple=True, default=None,
              help="List of index names to exclude from processing. E.g. --exclude=name1,name2")
@click.option('--job-type', type=str, required=True, callback=validate_job_type,
              help="Type of job to perform. Must be one of COMPRESS, DECOMPRESS, DELETE_JSON")
@click.pass_obj
def cli_bulk_compression_management(cfg, include, exclude, job_type):
    engine = migrate.migrate(cfg)
    indexes = index.Index.list_indexes(engine, False)

    inclusion_list = convert_cli_arg_to_list(include)
    exclusion_list = convert_cli_arg_to_list(exclude)

    with (concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor):
        futures = []
        for i in indexes:
            if (inclusion_list is None or i.name in inclusion_list) and (
                exclusion_list is None or i.name not in exclusion_list):
                futures.append(
                    executor.submit(check_index_and_launch_job, cfg, i.name, i.s3_prefix, job_type))

        # wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()


@click.command(name='decompress')
@click.argument('index_name')
@click.argument('prefix')
@click.pass_obj
def cli_decompress(cfg, index_name, prefix):
    check_index_and_launch_job(cfg, index_name, prefix, BgzipJobType.DECOMPRESS)


@click.command(name='remove-uncompressed-files')
@click.argument('index_name')
@click.argument('prefix')
@click.pass_obj
def cli_remove_uncompressed_files(cfg, index_name, prefix):
    check_index_and_launch_job(cfg, index_name, prefix, BgzipJobType.DELETE_JSON)


def is_index_prefix_valid(cfg, idx: str, prefix: str):
    engine = migrate.migrate(cfg)
    selected_index = [i for i in index.Index.lookup_all(engine, idx) if i.s3_prefix == prefix]
    return len(selected_index) == 1


def check_index_and_launch_job(cfg, index_name, prefix, job_type):
    if is_index_prefix_valid(cfg, index_name, prefix):
        start_and_monitor_aws_batch_job(job_type, index_name, prefix)
    else:
        console.print(f'Could not find unique index with name {index_name} and prefix {prefix}, quitting')


def start_and_monitor_aws_batch_job(job_type: BgzipJobType, index_name: str, s3_path: str,
                                    initial_wait: int = 90, check_interval: int = 30):
    job_id = aws.start_batch_job(index_name, s3_path, job_type.value)
    console.print(f'{job_type} started with id {job_id}')
    time.sleep(initial_wait)
    while True:
        status = aws.get_bgzip_job_status(job_id)
        console.print(f'{job_type} job, id: {job_id} status: {status}')
        if status == 'SUCCEEDED':
            console.print(f'{job_type} job {job_id} succeeded')
            break
        elif status == 'FAILED':
            console.print(f'{job_type} job {job_id} failed')
            break
        time.sleep(check_interval)


@click.command(name='update-compressed-status')
@click.argument('index_name')
@click.argument('prefix')
@click.option('--compressed', '-c', is_flag=True)
@click.pass_obj
def update_compressed_status(cfg, index_name, prefix, compressed):
    if not is_index_prefix_valid(cfg, index_name, prefix):
        console.print(f'Could not find unique index with name {index_name} and prefix {prefix}, quitting')
        return
    console.print(f'Updating compressed status of index {index_name} with prefix {prefix} to {compressed}')
    index.Index.set_compressed(migrate.migrate(cfg), index_name, prefix, compressed)


@click.command(name='query')
@click.argument('index_name')
@click.argument('q', nargs=-1)
@click.pass_obj
def cli_query(cfg, index_name, q):
    engine = migrate.migrate(cfg)
    i = index.Index.lookup(engine, index_name, len(q))

    # query the index
    reader = query.fetch(cfg, engine, i, q)

    # dump all the records
    for record in reader.records:
        console.print(orjson.dumps(record).decode('utf-8'))


@click.command(name='all')
@click.argument('index_name')
@click.pass_obj
def cli_all(cfg, index_name):
    engine = migrate.migrate(cfg)
    idxs = index.Index.lookup_all(engine, index_name)

    # read all records
    for idx in idxs:
        reader = query.fetch_all(cfg, idx)

        # lookup the table class from the schema
        for record in reader.records:
            console.print(orjson.dumps(record).decode('utf-8'))


@click.command(name='count')
@click.argument('index_name')
@click.argument('q', nargs=-1)
@click.pass_obj
def cli_count(cfg, index_name, q):
    engine = migrate.migrate(cfg)
    i = index.Index.lookup(engine, index_name, len(q))

    # query the index
    count = query.count(cfg, engine, i, q)
    console.print(count)


@click.command(name='match')
@click.argument('index_name')
@click.argument('q', nargs=-1)
@click.pass_obj
def cli_match(cfg, index_name, q):
    engine = migrate.migrate(cfg)
    i = index.Index.lookup(engine, index_name, len(q))

    # lookup the table class from the schema
    try:
        for obj in query.match(cfg, engine, i, q):
            console.print(obj)
    except AssertionError:
        console.log(f'Index {index_name} is not indexed by value!')


@click.command(name='build-schema')
@click.option('--save', '-s', is_flag=True)
@click.option('--out', '-o', type=str, default=None)
@click.argument('indexes', nargs=-1)
@click.pass_obj
def cli_build_schema(cfg, save, out, indexes):
    engine = migrate.migrate(cfg)

    # attempt to build the graphql object class for this index
    schema = ql.build_schema(cfg, engine, subset=indexes)

    # file the output the schema to
    out_file = out or cfg.graphql_schema

    # special case: allow asserting output to stdout (via --out "-")
    if out_file == '-':
        out_file = None

    # output the schema to a string
    schema_str = graphql.utilities.print_schema(schema)

    # output the schema to a file
    if save and out_file:
        logging.info('Writing schema to %s...', out_file)

        # write the schema file
        with open(out_file, mode='w') as fp:
            print(schema_str, file=fp)
    else:
        print(schema_str)


# initialize the cli
cli.add_command(cli_serve)
cli.add_command(cli_create)
cli.add_command(cli_list)
cli.add_command(cli_index)
cli.add_command(cli_query)
cli.add_command(cli_all)
cli.add_command(cli_count)
cli.add_command(cli_match)
cli.add_command(cli_build_schema)
cli.add_command(cli_compress)
cli.add_command(update_compressed_status)
cli.add_command(cli_decompress)
cli.add_command(cli_remove_uncompressed_files)
cli.add_command(cli_bulk_compression_management)


def main():
    """
    CLI entry point.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[rich.logging.RichHandler(console=console)],
    )

    # load the default .env file
    dotenv.load_dotenv()

    # disable info logging for 3rd party modules
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)

    # install mysql drivers
    pymysql.install_as_MySQLdb()

    # run
    try:
        cli()
    except ValueError as ex:
        logging.error(ex)
    except RuntimeError as ex:
        logging.error(ex)
    except AssertionError as ex:
        logging.error(ex)


if __name__ == '__main__':
    main()
