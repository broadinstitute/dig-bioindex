import os

import click

from bioindex.lib.config import Config
from bioindex.lib.index import Index, _chunk_objects, _key_is_current, list_index_objects
from bioindex.lib.migrate import migrate


def select_chunk(objects, chunk_index, chunk_count, group_size, group_max_bytes, expected_total):
    """Re-derive this worker's chunk from the full listing; fail loud on listing drift."""
    assert len(objects) == expected_total, \
        f'listing drift: {len(objects)} objects != expected {expected_total}'
    chunks = _chunk_objects(objects, group_size, group_max_bytes)
    assert chunk_count == len(chunks), \
        f'chunk-count drift: parent {chunk_count} != worker {len(chunks)}'
    assert 0 <= chunk_index < len(chunks), \
        f'chunk_index {chunk_index} out of range [0, {len(chunks)})'
    return chunks[chunk_index]


def index_chunk(index, engine, bucket, db_keys, chunk):
    """Index every key in the chunk that is not already indexed at its current version."""
    for obj in chunk:
        if _key_is_current(db_keys, obj):
            print(f'Skipping already-current s3://{bucket}/{obj["Key"]}')
            continue
        print(f'Indexing s3://{bucket}/{obj["Key"]}')
        _, records = index.index_object(engine, bucket, obj)
        index.insert_records_iter(engine, records)


@click.command()
@click.option('--index', '-i', 'index_name', type=str, required=True)
@click.option('--arity', '-a', type=str, required=True)
@click.option('--bucket', '-b', type=str, required=True)
@click.option('--rds-secret', '-r', type=str, required=True)
@click.option('--rds-schema', '-s', type=str, required=True)
@click.option('--s3-subdir', type=str, default='')
@click.option('--prefix', '-p', type=str, required=True)
@click.option('--prefer-compressed', type=int, required=True)
@click.option('--chunk-index', type=int, required=True)
@click.option('--chunk-count', type=int, required=True)
@click.option('--group-size', type=int, required=True)
@click.option('--group-max-bytes', type=int, required=True)
@click.option('--expected-total', type=int, required=True)
def main(index_name, arity, bucket, rds_secret, rds_schema, s3_subdir, prefix, prefer_compressed,
         chunk_index, chunk_count, group_size, group_max_bytes, expected_total):
    os.environ['BIOINDEX_S3_BUCKET'] = bucket
    os.environ['BIOINDEX_RDS_SECRET'] = rds_secret
    os.environ['BIOINDEX_BIO_SCHEMA'] = rds_schema
    if s3_subdir:
        os.environ['BIOINDEX_S3_SUBDIR'] = s3_subdir

    config = Config()
    print(f'Connecting to {rds_secret}/{rds_schema}...')
    engine = migrate(config)
    assert engine, 'Failed to connect to RDS instance'

    index = Index.lookup(engine, index_name, arity)
    assert index, 'Failed to find index'

    objects = list_index_objects(bucket, config.s3_path(prefix), bool(prefer_compressed))
    chunk = select_chunk(objects, chunk_index, chunk_count, group_size, group_max_bytes, expected_total)
    print(f'Grouped index {index_name}: chunk {chunk_index}/{chunk_count}, {len(chunk)} files')

    db_keys = index.lookup_keys(config, engine)
    index_chunk(index, engine, bucket, db_keys, chunk)


if __name__ == '__main__':
    main()
