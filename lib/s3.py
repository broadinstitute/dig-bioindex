import boto3
import botocore.config
import botocore.errorfactory
import fnmatch
import os
import os.path
import urllib.parse

# create an s3 session from ~/.aws credentials
s3_config = botocore.config.Config(max_pool_connections=200)
s3_session = boto3.session.Session()
s3_client = s3_session.client('s3', config=s3_config)


def s3_uri(bucket, path):
    """
    Returns an s3 URI for a given path in a bucket.
    """
    return 's3://%s/%s' % (bucket, path)


def s3_parse_url(uri):
    """
    Extract the bucket and prefix from the URL
    """
    url = urllib.parse.urlparse(uri)

    if url.scheme != 's3':
        raise ValueError('Invalid S3 URI: %s' % uri)

    # separate the bucket name from the path
    return url.netloc, url.path


def s3_list_objects(bucket, prefix, only=None, exclude=None):
    """
    Generator function that lists all the objects in S3 with a given prefix.
    """
    kwargs = {
        'Bucket': bucket,
        'Delimiter': '/',
        'Prefix': prefix.lstrip('/'),
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

            yield path

        # recursively search the common prefixes
        for common_prefix in resp.get('CommonPrefixes', []):
            yield from s3_list_objects(bucket, common_prefix['Prefix'], only=only, exclude=exclude)

        # no more paths?
        if not resp['IsTruncated']:
            break

        # next call, use the continuation token
        resp = s3_client.list_objects_v2(ContinuationToken=resp['NextContinuationToken'], **kwargs)


def s3_read_object(bucket, path, offset=None, length=None):
    """
    Open an s3 object and return a streaming portion of it.
    """
    kwargs = {
        'Bucket': str(bucket),
        'Key': str(path),
    }

    # specify the range parameter
    if offset is not None and length is not None:
        kwargs['Range'] = 'bytes=%d-%d' % (offset, offset + length - 1)
    elif offset is not None:
        kwargs['Range'] = 'bytes=%d-' % offset
    elif length is not None:
        kwargs['Range'] = 'bytes=-%d' % length

    # download the object
    return s3_client.get_object(**kwargs).get('Body')


def s3_test_object(bucket, path):
    """
    Checks to see if the path exists in the bucket.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=path)
        return True
    except botocore.errorfactory.ClientError:
        return False
