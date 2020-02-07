import click
import dotenv
import logging
import os

import lib.index
import lib.query
import lib.schema
import lib.secrets
import lib.s3


@click.group()
def cli():
    pass


@click.command(name='index')
@click.argument('bucket')
@click.argument('module')
def cli_index(bucket, module):
    engine = lib.secrets.connect_to_mysql(os.getenv('RDS_INSTANCE'))
    s3_objects = lib.s3.list_objects(bucket, module, exclude='_SUCCESS')

    if module == 'genes':
        lib.index.by_locus(engine, lib.schema.Genes.__table__, 'chromosome:start-end', bucket, s3_objects)
    else:
        pass  # TODO: error

    logging.info('Successfully built %s index.', module)


@click.command(name='query')
@click.argument('bucket')
@click.argument('module')
@click.argument('locus')
def cli_query(bucket, module, locus):
    engine = lib.secrets.connect_to_mysql(os.getenv('RDS_INSTANCE'))

    if module == 'genes':
        for obj in lib.query.by_locus(engine, bucket, lib.schema.Genes.__table__, locus):
            print(obj)
    else:
        pass  # TODO: error

    logging.info('Successfully built %s index.', module)


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
