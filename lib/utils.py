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
