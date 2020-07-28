import re
import secrets


def cap_case_str(s):
    """
    Translate a string like "foo_Bar-baz  whee" and return "FooBarBazWhee".
    """
    return re.sub(r'(?:[_\-\s]+|^)(.)', lambda m: m.group(1).upper(), s)


def nonce(length=20):
    """
    Generate a nonce string. This is just a random string that uniquely
    identifies something. It needn't be globally unique, just unique enough
    for a period of time (e.g. to identify a specific call in a rolling
    log file).
    """
    return secrets.token_urlsafe()


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
