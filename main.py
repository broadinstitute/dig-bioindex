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


@click.command(name='test')
def cli_test():
    """
    Test connections to redis and the aws s3 bucket.
    """
    logging.info('Testing redis connection...')
    with Client(readonly=True) as client:
        pass

    logging.info('Testing aws s3 credentials and bucket...')
    bucket = os.getenv('S3_BUCKET')

    # only get the first object in the bucket and ensure access
    first = next(s3_list_objects(bucket, '/'))
    assert s3_test_object(bucket, first), f'Failed to access s3 bucket {bucket}'


@click.command(name='index')
@click.option('--only', help='only process s3 keys matching the pattern')
@click.option('--exclude', help='exclude s3 keys matching the pattern')
@click.option('--new', is_flag=True, help='skip tables already indexed')
@click.option('--update', is_flag=True, help='update tables already indexed')
@click.option('--dialect', default='json', help='record dialect to use (default=json)')
@click.argument('key')
@click.argument('prefix')
@click.argument('locus')
def cli_index(only, exclude, new, update, dialect, key, prefix, locus):
    """
    Index s3 table records in to a redis key.
    """
    bucket = os.getenv('S3_BUCKET')
    t0 = time.time()

    # fetch the list of all paths to index
    paths = s3_list_objects(bucket, prefix, only=only, exclude=exclude)

    # connect to redis
    with Client() as client:
        n = index(client, key, dialect, locus, bucket, paths, update=update, new=new)
        dt = datetime.timedelta(seconds=time.time() - t0)

        # done output report
        logging.info('%d records indexed in %s', n, str(dt))


@click.command(name='keys')
def cli_keys():
    """
    Output the indexed key spaces.
    """
    with Client(readonly=True) as client:
        for key in client.get_table_keys():
            print(key)


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
            print(json.dumps(record))


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
cli.add_command(cli_test)
cli.add_command(cli_index)
cli.add_command(cli_keys)
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
