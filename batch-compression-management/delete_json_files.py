import boto3
import click

import bioindex.lib.s3 as s3
import utils


@click.command()
@click.option('--index', '-i', type=str)
@click.option('--bucket', '-b', type=str)
@click.option('--path', '-p', type=str)
def main(index, bucket, path):
    s3_objects = list(s3.list_objects(bucket, path, only='*.json'))
    boto_s3 = boto3.client('s3')
    print(f"will delete {len(s3_objects)} files for index {index}")
    files_to_retry = utils.process_files_concurrently(boto_s3, bucket, [file['Key'] for file in s3_objects],
                                                      delete_json_file)
    return len(files_to_retry)


def delete_json_file(bucket_name, file, boto_s3, files_to_retry, print_lock):
    results = boto_s3.list_objects(Bucket=bucket_name, Prefix=file + ".gz")
    if results.get('Contents', None) and len(results.get('Contents')) == 2:
        with print_lock:
            print(f"Deleting file: {file}")
        try:
            boto_s3.delete_object(Bucket=bucket_name, Key=file)
        except Exception as e:
            print(e)
            files_to_retry.append(file)


if __name__ == "__main__":
    main()
