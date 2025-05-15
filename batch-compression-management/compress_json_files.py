import subprocess

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
    s3_objects = list(s3.list_objects(bucket, path, only='*.json'))
    boto_s3 = boto3.client('s3')
    print(f"will compress {len(s3_objects)} files for index {index}")
    files_to_retry = utils.process_files_concurrently(boto_s3, bucket, [file['Key'] for file in s3_objects],
                                                      bg_compress_and_index_file)
    if len(files_to_retry) > 0:
        print(f"retrying {len(files_to_retry)} files")
        files_to_retry = utils.process_files_concurrently(boto_s3, bucket, files_to_retry, bg_compress_and_index_file)
        print(f"remaining files after second pass {len(files_to_retry)}")
    return len(files_to_retry)


def bg_compress_and_index_file(bucket_name, file, boto_s3, files_to_retry, print_lock):
    error_message = None

    results = boto_s3.list_objects(Bucket=bucket_name, Prefix=file + ".gz")
    if results.get('Contents', None) and len(results.get('Contents')) == 2:
        with print_lock:
            print(f"Compressed index file already exists: {file}")
        return

    with print_lock:
        print(f"Compressing {file}")

    # Run bgzip compression and capture stderr
    command = ['bgzip', '-i', f"s3://{bucket_name}/{file}"]
    try:
        result = subprocess.run(command, check=True, timeout=1200,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                               universal_newlines=True)
        stderr_output = result.stderr
    except subprocess.CalledProcessError as e:
        error_message = f"Error: Command exited with non-zero status: {e.returncode}, {file}"
        stderr_output = e.stderr
    except subprocess.TimeoutExpired as e:
        error_message = f"Error: Command timed out after {e.timeout} seconds"
        stderr_output = e.stderr if hasattr(e, 'stderr') else None

    if not error_message:
        validation_command = ['bgzip', '-t', f"s3://{bucket_name}/{file}.gz"]
        try:
            subprocess.run(validation_command, check=True, timeout=300,
                                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                              universal_newlines=True)
        except subprocess.CalledProcessError as e:
            error_message = f"Error: Validation failed for compressed file {file}.gz: {e.returncode}"
            if e.stderr:
                error_message += f"\nValidation stderr: {e.stderr.strip()}"
        except subprocess.TimeoutExpired as e:
            error_message = f"Error: Validation timed out after {e.timeout} seconds for {file}.gz"

    if error_message:
        with print_lock:
            print(error_message)
            if stderr_output and stderr_output.strip():
                print(f"Original compression stderr: {stderr_output.strip()}")
            files_to_retry.append(file)
    else:
        with print_lock:
            print(f"Finished compressing and validating {file}")


if __name__ == "__main__":
    main()
