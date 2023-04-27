import sys
import boto3
import subprocess
import concurrent.futures
import click
import sqlalchemy


@click.command()
@click.option('--bucket', '-b', type=str)
@click.option('--path', '-p', type=str)
def main(bucket, path):
    print(f'bucket = {bucket}')
    print(path)
    s3 = boto3.client('s3')
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute("select distinct k.key from Associations a join __Keys k on k.id = a.key")

        with concurrent.futures.ThreadPoolExecutor(max_workers=60) as executor:
            futures = []
            for row in rows:
                futures.append(executor.submit(bg_index_file, bucket, row[0], s3))
        # get the results of the tasks as they complete
            for future in concurrent.futures.as_completed(futures):
                future.result()


def get_engine():
    uri = 'mysql+pymysql://digduguser:kpnteam@dig-bio-index.cxrzznxifeib.us-east-1.rds.amazonaws.com/bio'

    # create the connection pool
    engine = sqlalchemy.create_engine(uri, pool_recycle=3600)

    # test the engine by making a single connection
    with engine.connect():
        return engine


def bg_index_file(bucket_name, file, s3):
    error_messaage = None
    results = s3.list_objects(Bucket=bucket_name, Prefix=file + ".gz")
    if results.get('Contents', None) and len(results.get('Contents')) == 2:
        print(f"Compressed index file already exists: {file}")
        return
    print(f"starting {file}")
    command = ['bgzip', '-i', f"s3://{bucket_name}/{file}"]
    try:
        subprocess.run(command, check=True, timeout=118)
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
