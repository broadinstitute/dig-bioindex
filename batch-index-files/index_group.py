import os

import boto3
import click

from bioindex.lib.config import Config
from bioindex.lib.index import Index
from bioindex.lib.migrate import migrate


def read_manifest(keys_uri):
    """Read a newline-delimited s3:// manifest into a list of object keys."""
    assert keys_uri.startswith('s3://'), keys_uri
    bucket, key = keys_uri[len('s3://'):].split('/', 1)
    body = boto3.client('s3').get_object(Bucket=bucket, Key=key)['Body'].read()
    return [line for line in body.decode('utf-8').splitlines() if line.strip()]


def index_keys(index, engine, bucket, keys):
    """Index every key in the manifest sequentially, streaming each file's records."""
    s3_client = boto3.client('s3')
    for file in keys:
        head = s3_client.head_object(Bucket=bucket, Key=file)
        obj = {'Key': file, 'Size': head['ContentLength'], 'ETag': head['ETag']}
        print(f'Indexing s3://{bucket}/{file}')
        s3_key, records = index.index_object(engine, bucket, obj)
        index.insert_records_iter(engine, records)


@click.command()
@click.option('--keys-uri', '-k', type=str, required=True)
@click.option('--index', '-i', 'index_name', type=str, required=True)
@click.option('--arity', '-a', type=str, required=True)
@click.option('--bucket', '-b', type=str, required=True)
@click.option('--rds-secret', '-r', type=str, required=True)
@click.option('--rds-schema', '-s', type=str, required=True)
def main(keys_uri, index_name, arity, bucket, rds_secret, rds_schema):
    os.environ['BIOINDEX_S3_BUCKET'] = bucket
    os.environ['BIOINDEX_RDS_SECRET'] = rds_secret
    os.environ['BIOINDEX_BIO_SCHEMA'] = rds_schema

    config = Config()
    print(f'Connecting to {rds_secret}/{rds_schema}...')
    engine = migrate(config)
    assert engine, 'Failed to connect to RDS instance'

    index = Index.lookup(engine, index_name, arity)
    assert index, 'Failed to find index'

    keys = read_manifest(keys_uri)
    print(f'Grouped index {index_name}: {len(keys)} files')
    index_keys(index, engine, bucket, keys)


if __name__ == '__main__':
    main()
