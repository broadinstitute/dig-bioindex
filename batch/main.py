import sys
import boto3
from Bio.bgzf import BgzfReader

def main(argv):
    s3 = boto3.client('s3')

    # Download block gzip files from S3 bucket
    s3.download_file('dig-bio-index', argv[0], 'file.bgz')
    for line in BgzfReader('file.bgz', 'r'):
        print(line.rstrip())


if __name__ == "__main__":
    main(sys.argv[1:])
