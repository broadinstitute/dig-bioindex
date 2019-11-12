import click
import datetime
import time

from lib.client import *
from lib.index import *
from lib.integrity import *
from lib.locus import *
from lib.query import *
from lib.s3 import *


@click.group()
def cli():
    pass


@click.command(name='index')
@click.option('--host', default='localhost', help='redis host')
@click.option('--port', default=6379, type=int, help='redis port')
@click.option('--only', help='only process s3 keys matching the pattern')
@click.option('--exclude', help='exclude s3 keys matching the pattern')
@click.option('--new', is_flag=True, help='index new tables not already indexed')
@click.argument('key')
@click.argument('locus')
@click.argument('source')
def cli_index(host, port, only, exclude, new, key, locus, source):
    """
    Index table records in s3 to redis.
    """
    bucket, prefix = s3_parse_url(source)
    t0 = time.time()

    with Client(host=host, port=port) as client:
        n = index(client, key, locus, bucket, prefix, only=only, exclude=exclude, new=new)
        dt = datetime.timedelta(seconds=time.time() - t0)

        # done output report
        logging.info('%d records indexed in %s', n, str(dt))


@click.command(name='query')
@click.option('--host', default='localhost', help='redis host')
@click.option('--port', default=6379, type=int, help='redis port')
@click.option('--count', is_flag=True, help='count overlapping records')
@click.argument('key')
@click.argument('locus')
def cli_query(host, port, count, key, locus):
    """
    Query redis db for all objects that a region overlaps.
    """
    chromosome, start, stop = parse_locus(locus)

    # open the db in read-only mode
    with Client(host=host, port=port) as client:
        records = query(client, key, chromosome, start, stop)

        if count:
            print(len(records))
        else:
            for record in records:
                print(record)


@click.command(name='check')
@click.option('--host', default='localhost', help='redis host')
@click.option('--port', default=6379, type=int, help='redis port')
@click.option('--delete', is_flag=True, help='delete bad keys')
def cli_check(host, port, delete):
    logging.info('Checking tables...')

    with Client(host=host, port=port) as client:
        check_tables(client, delete=delete)


# initialize the cli
cli.add_command(cli_index)
cli.add_command(cli_query)
cli.add_command(cli_check)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)-5s - %(message)s')

    # disable info logging for 3rd party modules
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)

    # run command
    cli()
