import boto3
import botocore.config
import botocore.errorfactory
import fnmatch
import os
import os.path
import re
import urllib.parse

# create an s3 session from ~/.aws credentials
s3_config = botocore.config.Config(max_pool_connections=200)
s3_session = boto3.session.Session()
s3_client = s3_session.client('s3', config=s3_config)


def split_bucket(s3_key):
    """
    Returns the bucket name and the key from an s3 location string.
    """
    match = re.match(r'(?:s3://)?([^/]+)/(.*)', s3_key, re.IGNORECASE)

    if not match:
        return None, s3_key

    return match.group(1), match.group(2)


def uri(bucket, path):
    """
    Returns an s3 URI for a given path in a bucket.
    """
    return f's3://{bucket}/{path}'


def parse_url(url):
    """
    Extract the bucket and prefix from the URL
    """
    url = urllib.parse.urlparse(url)

    if url.scheme != 's3':
        raise ValueError(f'Invalid S3 URI: {url}')

    # separate the bucket name from the path
    return url.netloc, url.path


def list_objects(bucket, prefix, only=None, exclude=None):
    """
    Generator function that returns all the objects in S3 with a given prefix.
    If the prefix is an absolute path (beginning with "s3://" then the bucket
    of the URI is used instead.
    """
    kwargs = {
        'Bucket': bucket,
        'Delimiter': '/',
        'Prefix': prefix.strip('/') + '/',
    }

    # initial call
    resp = s3_client.list_objects_v2(**kwargs)

    while True:
        if resp.get('KeyCount', 0) == 0:
            break

        # yield all paths that matches only and not exclude
        for obj in resp.get('Contents', []):
            path = obj['Key']
            file = os.path.basename(path)

            # ignore any files beginning with _
            if file[0] == '_':
                continue

            # filter by --only and --exclude
            if only and not fnmatch.fnmatch(file, only):
                continue
            if exclude and fnmatch.fnmatch(file, exclude):
                continue

            yield obj

        # recursively search the common prefixes for folder prefixes
        if prefix[-1] == '/':
            for common_prefix in resp.get('CommonPrefixes', []):
                yield from list_objects(bucket, common_prefix['Prefix'], only=only, exclude=exclude)

        # no more paths?
        if not resp['IsTruncated']:
            break

        # next call, use the continuation token
        resp = s3_client.list_objects_v2(ContinuationToken=resp['NextContinuationToken'], **kwargs)


def read_object(bucket, path, offset=None, length=None):
    """
    Open an s3 object and return a streaming portion of it. If the path is
    an "absolute" path (begins with "s3://") then the bucket name is overridden
    and the bucket from the path is used.
    """
    kwargs = {
        'Bucket': str(bucket),
        'Key': str(path),
    }

    # specify the range parameter
    if offset is not None and length is not None:
        kwargs['Range'] = f'bytes={offset}-{offset + length - 1}'
    elif offset is not None:
        kwargs['Range'] = f'bytes={offset}-'
    elif length is not None:
        kwargs['Range'] = f'bytes=-{length}'

    # download the object
    return s3_client.get_object(**kwargs).get('Body')


def test_object(bucket, s3_obj):
    """
    Checks to see if the path exists in the bucket.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_obj['Key'])
        return True
    except botocore.errorfactory.ClientError:
        return False
