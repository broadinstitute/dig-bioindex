import json
import os
import threading
import concurrent.futures

from boto3 import Session


def process_files_concurrently(boto_s3, bucket, files, file_function):
    files_to_retry = []
    print_lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=60) as executor:
        futures = []
        for file in files:
            futures.append(executor.submit(file_function, bucket, file, boto_s3,
                                           files_to_retry, print_lock))
        # wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()
    return files_to_retry


def get_access_keys():
    client = Session().client('secretsmanager')
    return json.loads(client.get_secret_value(SecretId='bgzip-credentials')['SecretString'])


def set_bgzip_creds():
    keys = get_access_keys()
    # transfer the secrets to environment variables (keep sensitive info out of source control)
    os.environ['AWS_ACCESS_KEY_ID'] = keys['access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = keys['secret_access_key']
