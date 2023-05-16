import gzip
import os

import boto3
import click

import bioindex.lib.s3 as s3
import utils


@click.command()
@click.option('--index', '-i', type=str)
@click.option('--bucket', '-b', type=str)
@click.option('--path', '-p', type=str)
def main(index, bucket, path):
    utils.set_bgzip_creds()
    s3_objects = list(s3.list_objects(bucket, path, only='*.gz'))
    boto_s3 = boto3.client('s3')
    print(f"will uncompress {len(s3_objects)} files for index {index}")
    files_to_retry = utils.process_files_concurrently(boto_s3, bucket, [file['Key'] for file in s3_objects],
                                                      decompress_file)
    return len(files_to_retry)


def decompress_file(bucket_name, file, boto_s3, files_to_retry, print_lock):
    error_message = None
    with print_lock:
        print(f"Decompressing file: {file}")

    try:
        os.makedirs(os.path.dirname(f'/tmp/{file}'), exist_ok=True)
        with open(f'/tmp/{file}', 'wb') as f:
            boto_s3.download_fileobj(bucket_name, file, f)
        decompressed_name = file.replace('.gz', '')
        with gzip.open(f'/tmp/{file}', 'rb') as f_in:
            with open(f"/tmp/{decompressed_name}", 'wb') as f_out:
                f_out.write(f_in.read())
        with open(f"/tmp/{decompressed_name}", 'rb') as data:
            boto_s3.upload_fileobj(data, bucket_name, decompressed_name)
            boto_s3.delete_object(Bucket=bucket_name, Key=f"{file}.gzi")
            boto_s3.delete_object(Bucket=bucket_name, Key=f"{file}")
    except Exception as e:
        error_message = f"Error: Failed to decompress: {e}, {file}"

    if error_message:
        with print_lock:
            print(error_message)
            files_to_retry.append(file)
    else:
        with print_lock:
            print(f"Finished compressing {file}")


if __name__ == "__main__":
    main()
