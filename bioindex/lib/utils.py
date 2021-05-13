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


async def profile_async(awaitable):
    """
    Execute f and return the result along with the time in seconds.
    """
    now = time.perf_counter()

    # execute and determine how long it took
    return await awaitable, time.perf_counter() - now


def cap_case_str(s):
    """
    Translate a string like "foo_Bar-baz  whee" and return "FooBarBazWhee".
    """
    return re.sub(r'(?:[^a-z0-9]+|^)(.)', lambda m: m.group(1).upper(), s, flags=re.IGNORECASE)


def camel_case_str(s):
    """
    Like cap_case_str, but the first character is lower-cased unless it is
    part of an acronym.
    """
    s=re.sub(r'(?:[^a-z0-9]+)(.)', lambda m: m.group(1).upper(), s, flags=re.IGNORECASE)
    s=re.sub(r'^[A-Z][a-z]+', lambda m: m.group(0).lower(), s)

    return s


def snake_case_str(s):
    """
    Translate a string like "foo_Bar-baz  whee" and return "foo_bar_baz_whee".
    """
    return re.sub(r'([^a-z0-9]+|^)(.)', lambda m: f'{"_" if m.group(1) else ""}{m.group(2).lower()}', s, flags=re.IGNORECASE)


def nonce(length=20):
    """
    Generate a nonce string. This is just a random string that uniquely
    identifies something. It needn't be globally unique, just unique enough
    for a period of time (e.g. to identify a specific call in a rolling
    log file).
    """
    return secrets.token_urlsafe()
