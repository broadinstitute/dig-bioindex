import csv
import http.cookies
import re
import secrets
import smart_open
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
    s = re.sub(r'(?:[^a-z0-9]+)(.)', lambda m: m.group(1).upper(), s, flags=re.IGNORECASE)
    s = re.sub(r'^[A-Z][a-z]+', lambda m: m.group(0).lower(), s)

    return s


def snake_case_str(s):
    """
    Translate a string like "foo_Bar-baz  whee" and return "foo_bar_baz_whee".
    """
    return re.sub(r'([^a-z0-9]+|^)(.)', lambda m: f'{"_" if m.group(1) else ""}{m.group(2).lower()}', s,
                  flags=re.IGNORECASE)


def nonce(length=20):
    """
    Generate a nonce string. This is just a random string that uniquely
    identifies something. It needn't be globally unique, just unique enough
    for a period of time (e.g. to identify a specific call in a rolling
    log file).
    """
    return secrets.token_urlsafe()


def read_gff(uri):
    """
    Open a GFF3 file (possibly remote) and read it line-by-line,
    yielding those records as a list of:

     * Chromosome
     * Source
     * Type
     * Start
     * End
     * Attribute dictionary

    Score, strand, and frame are ignored as they are not needed.
    """
    with smart_open.smart_open(uri, mode='rb', encoding='utf-8') as fp:
        r = csv.reader(fp, dialect='excel', delimiter='\t')

        # read each record, split it into columns
        for chromosome, source, typ, start, end, score, strand, frame, attr in r:
            yield (
                chromosome.upper(),
                source if source and source != '.' else None,
                typ if typ and typ != '.' else None,
                int(start),
                int(end),
                {k: m.value for k, m in http.cookies.SimpleCookie(attr).items()},
            )
