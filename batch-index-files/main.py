import sys
import boto3
import subprocess
import concurrent.futures


def main(argv):
    s3 = boto3.client('s3')

    bucket_name = 'dig-bio-index'
    response = s3.list_objects(Bucket=bucket_name, Prefix=argv[0])

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for file in response['Contents']:
            if not str(file['Key']).endswith(".bgz"):
                continue
            futures.append(executor.submit(bg_index_file, bucket_name, file))

        # get the results of the tasks as they complete
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            print(result)


def bg_index_file(bucket_name, file):
    command = ['bgzip', '-r', f"s3://{bucket_name}/{file['Key']}"]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    print(f"{result.returncode} {result.stderr}")
    print(file['Key'])


if __name__ == "__main__":
    main(sys.argv[1:])
