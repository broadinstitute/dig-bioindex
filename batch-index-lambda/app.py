import subprocess
import boto3


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    s3_bucket = event.get('s3_bucket')
    s3_obj = event.get('s3_obj')
    print(f'Indexing s3://{s3_bucket}/{s3_obj}')
    s3.download_file(s3_bucket, s3_obj, '/tmp/to_compress.json')
    command = ['./bgzip', '-i', '/tmp/to_compress.json']
    error_messaage = None
    try:
        subprocess.run(command, check=True, timeout=118)
    except subprocess.CalledProcessError as e:
        error_messaage = f"Error: Command exited with non-zero status: {e.returncode}"
    except subprocess.TimeoutExpired as e:
        error_messaage = f"Error: Command timed out after {e.timeout} seconds"
    except FileNotFoundError as e:
        error_messaage = "Error: Command not found"

    if error_messaage:
        return {
            "statusCode": 500,
            "body": error_messaage
        }
    else:
        s3.upload_file('/tmp/to_compress.json.gz', s3_bucket, s3_obj + '.gz')
        s3.upload_file('/tmp/to_compress.json.gz.gzi', s3_bucket, s3_obj + '.gz.gzi')
        return {
            "statusCode": 200,
            "body": f'Indexed s3://{s3_bucket}/{s3_obj}'
        }
