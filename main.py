import click
import datetime
import dotenv
import time

from lib.client import *
from lib.index import *
from lib.query import *
from lib.refresh import *
from lib.s3 import *


@click.group()
def cli():
    pass


@click.command(name='index')
@click.option('--only', help='only process s3 keys matching the pattern')
@click.option('--exclude', help='exclude s3 keys matching the pattern')
@click.option('--new', is_flag=True, help='skip tables already indexed')
@click.option('--update', is_flag=True, help='update tables already indexed')
@click.argument('key')
@click.argument('locus')
@click.argument('prefix')
def cli_index(only, exclude, new, update, key, locus, prefix):
    """
    Index s3 table records in to a redis key.
    """
    bucket = os.getenv('S3_BUCKET')
    t0 = time.time()

    # fix the prefix to be something valid for s3
    prefix = prefix.strip('/') + '/'

    if new and update:
        raise AssertionError('Cannot provide both --new and --update')

    # connect to redis
    with Client() as client:
        n = index(client, key, locus, bucket, prefix, only=only, exclude=exclude, new=new, update=update)
        dt = datetime.timedelta(seconds=time.time() - t0)

        # done output report
        logging.info('%d records indexed in %s', n, str(dt))


@click.command(name='count')
@click.argument('key')
@click.argument('locus')
def cli_count(key, locus):
    """
    Count and output the number of key records that overlap a locus.
    """
    chromosome, start, stop = parse_locus(locus)

    # open the db in read-only mode
    with Client(readonly=True) as client:
        print(client.count_records(key, chromosome, start, stop))


@click.command(name='query')
@click.argument('key')
@click.argument('locus')
def cli_query(key, locus):
    """
    Query redis db and print key records that overlaps a locus.
    """
    bucket = os.getenv('S3_BUCKET')
    chromosome, start, stop = parse_locus(locus)

    # open the db in read-only mode
    with Client(readonly=True) as client:
        for record in query(client, key, chromosome, start, stop, bucket):
            print(record)


@click.command(name='check')
@click.option('--delete', is_flag=True, help='delete bad keys')
def cli_check(delete):
    """
    Check integrity, ensuring s3 tables exist and delete orphaned records.
    """
    bucket = os.getenv('S3_BUCKET')

    if delete:
        logging.warning('This will delete orphaned records; are you sure? [y/N] ')
        if input().lower() != 'y':
            return

    logging.info('Running table check...')

    with Client(readonly=not delete) as client:
        check_tables(client, bucket, delete=delete)

    logging.info('Check complete')


# initialize the cli
cli.add_command(cli_index)
cli.add_command(cli_count)
cli.add_command(cli_query)
cli.add_command(cli_check)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-5s - %(message)s')

    # disable info logging for 3rd party modules
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)

    # load dot files
    dotenv.load_dotenv()

    # verify environment
    assert os.getenv('S3_BUCKET'), 'S3_BUCKET not set in environment or .env'

    # run command
    cli()
