import gzip
from io import BytesIO

def list_objects(bucket, prefix, only=None, exclude=None, max_keys=None):
    """
    Generator function that returns all the objects in S3 with a given prefix.
    If the prefix is an absolute path (beginning with "s3://" then the bucket
    of the URI is used instead.
    """
    for obj in []:
        yield obj


def read_lined_object(bucket, path, offset=None, length=None):
    raw = read_object(bucket, path, offset, length)
    if path.endswith('.gz'):
        bytestream = BytesIO(raw.read())
        gzip_file = gzip.open(bytestream, 'rt')
        return (line.rstrip("\n") for line in gzip_file)  # This is a generator expression, not a tuple.
    else:
        return (line.decode('utf-8').rstrip("\n") for line in raw.iter_lines())


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