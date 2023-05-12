import concurrent.futures
import json
import subprocess

import boto3
import click
import os
from boto3 import Session

import bioindex.lib.s3 as s3
import requests
import threading


@click.command()
@click.option('--index', '-i', type=str)
@click.option('--bucket', '-b', type=str)
@click.option('--path', '-p', type=str)
@click.option('--delete', '-d', type=bool, default=False)
def main(index, bucket, path, delete):
    print("starting")
    keys = get_access_keys()
    # transfer the secrets to environment variables (keep sensitive info out of source control)
    os.environ['AWS_ACCESS_KEY_ID'] = keys['access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = keys['secret_access_key']
    s3_objects = list(s3.list_objects(bucket, path, only='*.json'))
    boto_s3 = boto3.client('s3')
    print(f"will compress {len(s3_objects)} files")
    files_to_retry = process_files_concurrently(boto_s3, bucket, delete, [file['Key'] for file in s3_objects])
    if len(files_to_retry) > 0:
        print(f"retrying {len(files_to_retry)} files")
        files_to_retry = process_files_concurrently(boto_s3, bucket, delete, files_to_retry)
        print(f"remaining files after second pass {len(files_to_retry)}")
    if len(files_to_retry) == 0:
        bio_idx_host = 'http://ec2-18-215-38-136.compute-1.amazonaws.com:5001'
        requests.post(f"{bio_idx_host}/bgcompress/mark-completed/{index}")


def process_files_concurrently(boto_s3, bucket, delete, files):
    files_to_retry = []
    print_lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=60) as executor:
        futures = []
        for file in files:
            futures.append(executor.submit(bg_compress_and_index_file, bucket, file, boto_s3, delete,
                                           files_to_retry, print_lock))
        # wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()
    return files_to_retry


def get_access_keys():
    client = Session().client('secretsmanager')
    return json.loads(client.get_secret_value(SecretId='bgzip-credentials')['SecretString'])


def bg_compress_and_index_file(bucket_name, file, boto_s3, delete_json_file, files_to_retry, print_lock):
    error_message = None
    results = boto_s3.list_objects(Bucket=bucket_name, Prefix=file + ".gz")
    if results.get('Contents', None) and len(results.get('Contents')) == 2:
        with print_lock:
            print(f"Compressed index file already exists: {file}")
        if delete_json_file:
            boto_s3.delete_object(Bucket=bucket_name, Key=file)
        return
    with print_lock:
        print(f"starting {file}")
    command = ['bgzip', '-i', f"s3://{bucket_name}/{file}"]
    try:
        subprocess.run(command, check=True, timeout=120, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if delete_json_file:
            boto_s3.delete_object(Bucket=bucket_name, Key=file)
    except subprocess.CalledProcessError as e:
        error_message = f"Error: Command exited with non-zero status: {e.returncode}, {file}"
    except subprocess.TimeoutExpired as e:
        error_message = f"Error: Command timed out after {e.timeout} seconds"

    if error_message:
        with print_lock:
            print(error_message)
            files_to_retry.append(file)
    else:
        with print_lock:
            print(f"finished {file}")


if __name__ == "__main__":
    main()
