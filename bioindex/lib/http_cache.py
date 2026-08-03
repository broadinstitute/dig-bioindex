"""
Conditional-request helpers.

The point of an ETag here is not to save bytes - these responses are small -
but to let a shared cache in front of us answer at all. Nothing the service
returns carries a validator today, so every request reaches a worker and runs
its query, however many times the same answer has already been given.
"""
import hashlib

import orjson

# Fields the metadata envelope regenerates on every response: a random nonce
# and the wall-clock time the query took. They say nothing about the data, and
# hashing them would give a fresh tag every time - which is exactly the trap,
# because the result still looks like a working ETag.
VOLATILE_FIELDS = frozenset({'nonce', 'profile'})


def etag_for(content):
    """
    A tag for an already-encoded response payload, over the parts that
    describe the data and nothing else.
    """
    stable = {k: v for k, v in content.items() if k not in VOLATILE_FIELDS}
    digest = hashlib.sha256(orjson.dumps(stable, option=orjson.OPT_SORT_KEYS))

    return f'"{digest.hexdigest()[:32]}"'


def if_none_match(header, etag):
    """
    True if the request already holds this version. The header is a list,
    may be the wildcard, and may mark its validators weak - which is fine
    here, since our tag is opaque either way.
    """
    if not header:
        return False

    for candidate in (c.strip() for c in header.split(',')):
        if candidate == '*':
            return True
        if candidate.startswith('W/'):
            candidate = candidate[2:]
        if candidate == etag:
            return True

    return False
