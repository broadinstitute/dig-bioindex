import gzip
from io import BytesIO

import botocore.errorfactory
import fnmatch
import os
import os.path
import re
import urllib.parse

from .aws import s3_client


def is_absolute(s3_key):
    """
    True if the s3 key points to an absolute bucket location.
    """
    return s3_key.startsWith('s3://')


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


def relative_key(key, common_prefix, strip_uuid=True):
    """
    Given an S3 key like:

      foo/bar/baz/part-00015-59b75a7e-56ef-4183-bf26-48f67c6f33c7-c000.json

    And a common prefix for the key like:

      foo/bar/

    This should simplify and return: baz/part-00015.json
    """
    simple_key = key

    if simple_key.startswith(common_prefix):
        simple_key = simple_key[len(common_prefix):]

    if strip_uuid:
        simple_key = re.sub(r'(?:-[0-9a-f]+){6}(?=\.)', '', simple_key, re.IGNORECASE)

    return simple_key


def list_objects(bucket, prefix, only=None, exclude=None, max_keys=None):
    """
    Generator function that returns all the objects in S3 with a given prefix.
    If the prefix is an absolute path (beginning with "s3://" then the bucket
    of the URI is used instead.
    """
    kwargs = {
        'Bucket': bucket,
        'Prefix': prefix.strip('/') + '/',
    }

    # allow for a limit to be placed on the number of objects returned
    if max_keys:
        kwargs['MaxKeys'] = max_keys

    # initial call
    resp = s3_client.list_objects_v2(**kwargs)

    while True:
        if resp.get('KeyCount', 0) == 0:
            break

        # yield all paths that matches only and not exclude
        for obj in resp.get('Contents', []):
            path = obj['Key']
            file = os.path.basename(path)

            # ignore empty files
            if obj['Size'] == 0:
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

    raw = s3_client.get_object(**kwargs).get('Body')

    if path.endswith('.gz'):
        bytestream = BytesIO(raw.read())
        return gzip.open(bytestream, 'rt')
    else:
        return raw.iter_lines()


def test_object(bucket, s3_obj):
    """
    Checks to see if the path exists in the bucket.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_obj['Key'])
        return True
    except botocore.errorfactory.ClientError:
        return False
