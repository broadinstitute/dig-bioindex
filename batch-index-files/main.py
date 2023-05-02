import concurrent.futures
import subprocess

import boto3
import click

import bioindex.lib.s3 as s3


@click.command()
@click.option('--bucket', '-b', type=str)
@click.option('--path', '-p', type=str)
@click.option('--delete', '-d', type=bool, default=False)
def main(bucket, path, delete):
    s3_objects = list(s3.list_objects(bucket, path, only='*.json'))
    boto_s3 = boto3.client('s3')
    print(f"will compress {len(s3_objects)} files")
    with concurrent.futures.ThreadPoolExecutor(max_workers=60) as executor:
        futures = []
        for file in s3_objects:
            futures.append(executor.submit(bg_compress_and_index_file, bucket, file['Key'], boto_s3, delete))
        # wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()


def bg_compress_and_index_file(bucket_name, file, boto_s3, delete_json_file):
    error_messaage = None
    results = boto_s3.list_objects(Bucket=bucket_name, Prefix=file + ".gz")
    if results.get('Contents', None) and len(results.get('Contents')) == 2:
        print(f"Compressed index file already exists: {file}")
        return
    print(f"starting {file}")
    command = ['bgzip', '-i', f"s3://{bucket_name}/{file}"]
    try:
        subprocess.run(command, check=True, timeout=120)
        if delete_json_file:
            boto_s3.delete_object(Bucket=bucket_name, Key=file)
    except subprocess.CalledProcessError as e:
        error_messaage = f"Error: Command exited with non-zero status: {e.returncode}"
    except subprocess.TimeoutExpired as e:
        error_messaage = f"Error: Command timed out after {e.timeout} seconds"
    except FileNotFoundError as e:
        error_messaage = "Error: Command not found"

    if error_messaage:
        print(error_messaage)
    else:
        print(f"finished {file}")


if __name__ == "__main__":
    main()
