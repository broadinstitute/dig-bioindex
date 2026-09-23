"""One child of the `bgzip-array-job` AWS Batch array job.

Each child of the array derives its share of an index's .json files from its array index
and the array size (Batch does not tell children the array size, so the submitter passes
it as a parameter), then compresses each file with multi-threaded bgzip and validates the
archive. There is deliberately no per-file timeout: the job definition's attempt timeout
and Batch's retry strategy are the guard rails, and a re-run skips finished files.

This script exists alongside compress_json_files.py, which handles whole prefixes in one
task and remains the default path; this one is for indexes whose individual files are far
too large for that (10+ GB each).
"""
import os
import subprocess
import sys

import boto3
import click

import bioindex.lib.s3 as s3
import utils

SKIPPED = 'skipped'
COMPRESSED = 'compressed'
FAILED = 'failed'


def log(message):
    """print() with an explicit flush.

    Under the awslogs driver, stdout is a pipe, so Python fully buffers it; if Batch's
    attempt timeout SIGTERMs the interpreter mid-file, an unflushed buffer means the
    operator sees an empty CloudWatch stream instead of which key was in progress.
    """
    print(message, flush=True)


def array_index_from_env(environ=os.environ):
    """Batch sets AWS_BATCH_JOB_ARRAY_INDEX on array children only; a plain job is child 0."""
    return int(environ.get('AWS_BATCH_JOB_ARRAY_INDEX', '0'))


def select_files(keys, array_index, array_size):
    """This child's share of the keys: sorted positions array_index, array_index+size, ..."""
    assert array_size >= 1, f'array size must be >= 1, got {array_size}'
    assert 0 <= array_index < array_size, \
        f'array index {array_index} out of range [0, {array_size})'
    return sorted(keys)[array_index::array_size]


def compressed_pair_exists(boto_s3, bucket, key):
    """True only when both bgzip outputs (<key>.gz and <key>.gz.gzi) already exist."""
    resp = boto_s3.list_objects_v2(Bucket=bucket, Prefix=key + '.gz')
    found = {obj['Key'] for obj in resp.get('Contents', [])}
    return {key + '.gz', key + '.gz.gzi'} <= found


def _run(command):
    """Run one bgzip command with no timeout; return (returncode, stderr)."""
    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    return result.returncode, result.stderr


def _delete_partial_outputs(boto_s3, bucket, key):
    """Remove <key>.gz and <key>.gz.gzi after any failure.

    A failed compress step may have written nothing, and a failed validate step leaves a
    corrupt-but-complete pair; either way, deleting both (S3 delete of a missing key
    succeeds) guarantees a retry recompresses instead of compressed_pair_exists() skipping
    a bad or partial archive.
    """
    for suffix in ('.gz', '.gz.gzi'):
        boto_s3.delete_object(Bucket=bucket, Key=key + suffix)
    log(f'Deleted partial outputs for {key} after failure: {key}.gz, {key}.gz.gzi')


def compress_one(boto_s3, bucket, key, threads):
    """Compress one .json into <key>.gz + <key>.gz.gzi and validate it. Never raises."""
    if compressed_pair_exists(boto_s3, bucket, key):
        log(f'Compressed index file already exists: {key}')
        return SKIPPED

    url = f's3://{bucket}/{key}'
    log(f'Compressing {url} with {threads} threads')
    for command in (['bgzip', '-@', str(threads), '-i', url], ['bgzip', '-t', f'{url}.gz']):
        code, stderr = _run(command)
        if code != 0:
            log(f'Error: {" ".join(command)} exited {code} for {key}')
            if stderr and stderr.strip():
                log(f'bgzip stderr: {stderr.strip()}')
            _delete_partial_outputs(boto_s3, bucket, key)
            return FAILED

    log(f'Finished compressing and validating {key}')
    return COMPRESSED


@click.command()
@click.option('--index', '-i', type=str, required=True)
@click.option('--bucket', '-b', type=str, required=True)
@click.option('--path', '-p', type=str, required=True)
@click.option('--array-size', '-n', type=int, required=True,
              help='Size of the Batch array this child belongs to (the stride over the file list)')
@click.option('--threads', '-t', type=int, default=4, show_default=True,
              help='bgzip compression threads')
def main(index, bucket, path, array_size, threads):
    array_index = array_index_from_env()

    # Resolve the task role's credentials BEFORE set_bgzip_creds() exports the bgzip
    # user's keys into the environment: that user cannot delete objects, and a failed
    # file's partial .gz/.gzi must be removed so a retry recompresses instead of
    # skipping it. botocore caches the credentials on the session, so clients made
    # from it keep the task role regardless of the env vars.
    session = boto3.Session()
    session.get_credentials()
    utils.set_bgzip_creds()

    keys = [obj['Key'] for obj in s3.list_objects(bucket, path, only='*.json')]
    files = select_files(keys, array_index, array_size)
    log(f'{index}: child {array_index}/{array_size} owns {len(files)} of {len(keys)} json files')

    boto_s3 = session.client('s3')
    counts = {SKIPPED: 0, COMPRESSED: 0, FAILED: 0}
    for key in files:
        counts[compress_one(boto_s3, bucket, key, threads)] += 1

    log(f'compressed {counts[COMPRESSED]}, skipped {counts[SKIPPED]}, failed {counts[FAILED]}')
    if counts[FAILED]:
        sys.exit(1)


if __name__ == '__main__':
    main()
