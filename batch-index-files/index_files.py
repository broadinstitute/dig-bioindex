import os

import boto3
import click

from bioindex.lib.config import Config
from bioindex.lib.index import Index
from bioindex.lib.migrate import migrate


@click.command()
@click.option('--file', '-f', type=str)
@click.option('--index', '-i', type=str)
@click.option('--arity', '-a', type=str)
@click.option('--bucket', '-b', type=str)
@click.option('--rds-secret', '-r', type=str)
@click.option('--rds-schema', '-s', type=str)
@click.option('--size', '-z', type=str)
def main(file, index, arity, bucket, rds_secret, rds_schema, size):
    os.environ['BIOINDEX_S3_BUCKET'] = bucket
    os.environ['BIOINDEX_RDS_SECRET'] = rds_secret
    os.environ['BIOINDEX_BIO_SCHEMA'] = rds_schema

    config = Config()

    # connect to the BioIndex MySQL database
    print(f'Connecting to {rds_secret}/{rds_schema}...')
    engine = migrate(config)
    assert engine, 'Failed to connect to RDS instance'

    # find the index by name
    print(f'Looking up index {index}')
    index = Index.lookup(engine, index, arity)
    assert index, 'Failed to find index'

    print(f'Indexing s3://{bucket}/{file}')
    s3_client = boto3.client('s3')
    s3_resp = s3_client.head_object(Bucket=bucket, Key=f"{file}")
    s3_obj = {'Key': file, 'Size': s3_resp['ContentLength'], 'ETag': s3_resp['ETag']}

    s3_key, records = index.index_object(engine, bucket, s3_obj)
    records = list(records)

    # bulk insert them into the table
    print(f'Inserting {len(records):,} records')
    index.insert_records_batched(engine, records)



if __name__ == "__main__":
    main()
