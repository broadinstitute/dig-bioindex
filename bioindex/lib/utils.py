import re
import secrets
import time


def profile(f, *args, **kwargs):
    """
    Execute f and return the result along with the time in seconds.
    """
    now = time.perf_counter()

    # execute and determine how long it took
    return f(*args, **kwargs), time.perf_counter() - now


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
