import fastapi
import itertools

import lib.config
import lib.continuation
import lib.query
import lib.reader
import lib.secrets
import lib.tables

from lib.auth import restricted_keywords
from lib.profile import profile
from lib.utils import nonce


# load dot files and configuration
config = lib.config.Config()

# create flask app; this will load .env
router = fastapi.APIRouter()

# connect to database
engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.bio_schema)
portal = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.portal_schema)

# max number of bytes to read from s3 per request
RESPONSE_LIMIT = config.response_limit
MATCH_LIMIT = config.match_limit


def _load_indexes():
    """
    Create a cache of the indexes in the database.
    """
    return dict((i.name, i) for i in lib.tables.list_indexes(engine, filter_built=False))


# initialize with all the indexes, get them all, whether built or not
INDEXES = _load_indexes()


@router.get('/indexes', response_class=fastapi.responses.ORJSONResponse)
async def api_list_indexes():
    """
    Return all queryable indexes. This also refreshes the internal
    cache of the table so the server doesn't need to be bounced when
    the table is updated (very rare!).
    """
    global INDEXES

    # update the global index cache
    INDEXES = _load_indexes()
    data = []

    # add each index to the response data
    for index in sorted(INDEXES.values(), key=lambda i: i.name):
        data.append({
            'index': index.name,
            'built': index.built,
            'schema': str(index.schema),
            'query': {
                'keys': index.schema.key_columns,
                'locus': index.schema.has_locus,
            },
        })

    return {
        'count': len(data),
        'data': data,
        'nonce': nonce(),
    }


@router.get('/match/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_match(index: str, req: fastapi.Request, q: str, limit: int = None):
    """
    Return all the unique keys for a value-indexed table.
    """
    try:
        idx = INDEXES[index]
        qs = _parse_query(q)

        # execute the query
        keys, query_s = profile(lib.query.match, engine, idx, qs)

        # allow an upper limit on the total number of keys returned
        if limit is not None:
            keys = itertools.islice(keys, limit)

        # read the matched keys
        return _match_keys(keys, index, qs, limit, query_s=query_s)
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/count/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_count_index(index: str, req: fastapi.Request, q: str=None):
    """
    Query the database and estimate how many records will be returned.
    """
    try:
        idx = INDEXES[index]
        qs = _parse_query(q)

        # lookup the schema for this index and perform the query
        count, query_s = profile(lib.query.count, engine, config.s3_bucket, idx, qs)

        return {
            'profile': {
                'query': query_s,
            },
            'index': index,
            'q': qs,
            'count': count,
            'nonce': nonce(),
        }
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/all/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_all(index: str, req: fastapi.Request, fmt: str='row'):
    """
    Query the database and return ALL records for a given index.
    """
    try:
        idx = INDEXES[index]

        # discover what the user doesn't have access to see
        restricted, auth_s = profile(restricted_keywords, portal, req)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(lib.query.fetch_all, config.s3_bucket, idx.s3_prefix, restricted=restricted)

        # fetch records from the reader
        return _fetch_records(reader, index, None, fmt, query_s=auth_s+query_s)
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/all/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_test_all(index: str, req: fastapi.Request):
    """
    Query the database fetch ALL records for a given index. Don't read
    the records from S3, but instead set the Content-Length to the total
    number of bytes what would be read.
    """
    try:
        idx = INDEXES[index]

        # lookup the schema for this index and perform the query
        reader, query_s = profile(lib.query.fetch_all, config.s3_bucket, idx.s3_prefix)

        # return the total number of bytes that need to be read
        return fastapi.Response(headers={'Content-Length': str(reader.bytes_total)})
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/query/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_query_index(index: str, q: str, req: fastapi.Request, fmt='row', limit: int=None):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    try:
        idx = INDEXES[index]
        qs = _parse_query(q, required=True)

        # discover what the user doesn't have access to see
        restricted, auth_s = profile(restricted_keywords, portal, req)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            lib.query.fetch,
            engine,
            config.s3_bucket,
            idx,
            qs,
            restricted=restricted,
        )

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            reader.set_limit(limit)

        # the results of the query
        return _fetch_records(reader, index, qs, fmt, query_s=auth_s+query_s)
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/query/{index}')
async def api_test_index(index: str, q: str, req: fastapi.Request):
    """
    Query the database for records matching the query parameter. Don't
    read the records from S3, but instead set the Content-Length to the
    total number of bytes what would be read.
    """
    try:
        idx = INDEXES[index]
        qs = _parse_query(q, required=True)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(lib.query.fetch, engine, config.s3_bucket, idx, qs)

        return fastapi.Response(headers={'Content-Length': str(reader.bytes_total)})
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/cont', response_class=fastapi.responses.ORJSONResponse)
async def api_cont(token: str):
    """
    Lookup a continuation token and get the next set of records.
    """
    try:
        cont = lib.continuation.lookup_continuation(token)

        # the token is no longer valid
        lib.continuation.remove_continuation(token)

        # execute the continuation callback
        return cont.callback(cont)

    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail='Invalid, expired, or missing continuation token')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


def _parse_query(q, required=False):
    """
    Get the `q` query parameter and split it by comma into query parameters
    for a schema query.
    """
    if required and q is None:
        raise ValueError('Missing query parameter')

    # if no query parameter is provided, assume empty string
    return q.split(',') if q else []


def _match_keys(keys, index, qs, limit, page=1, query_s=None):
    """
    Collects up to MATCH_LIMIT keys from a database cursor and then
    return a JSON response object with them.
    """
    fetched, fetch_s = profile(list, itertools.islice(keys, MATCH_LIMIT))

    # create a continuation if there is more data
    token = None if len(fetched) < MATCH_LIMIT else lib.continuation.make_continuation(
        callback=lambda cont: _match_keys(keys, index, limit, qs, page=page+1),
    )

    return {
        'profile': {
            'fetch': fetch_s,
            'query': query_s,
        },
        'index': index,
        'qs': qs,
        'limit': limit,
        'page': page,
        'count': len(fetched),
        'data': list(fetched),
        'continuation': token,
        'nonce': nonce(),
    }


def _fetch_records(reader, index, qs, fmt, page=1, query_s=None):
    """
    Reads up to RESPONSE_LIMIT bytes from a RecordReader, format them,
    and then return a JSON response object with the records.
    """
    bytes_limit = reader.bytes_read + RESPONSE_LIMIT
    restricted_count = reader.restricted_count

    # similar to itertools.takewhile, but keeps the final record
    def take():
        for r in reader.records:
            yield r

            # stop if the byte limit was reached
            if reader.bytes_read > bytes_limit:
                break

    # validate query parameters
    if fmt not in ['r', 'row', 'c', 'col', 'column']:
        raise ValueError('Invalid output format')

    # profile how long it takes to fetch the records from s3
    fetched_records, fetch_s = profile(list, take())
    count = len(fetched_records)

    # transform a list of dictionaries into a dictionary of lists
    if fmt[0] == 'c':
        fetched_records = {k: [r.get(k) for r in fetched_records] for k in fetched_records[0].keys()}

    # create a continuation if there is more data
    token = None if reader.at_end else lib.continuation.make_continuation(
        callback=lambda cont: _fetch_records(reader, index, qs, fmt, page=page+1),
    )

    # build JSON response
    return {
        'profile': {
            'fetch': fetch_s,
            'query': query_s,
        },
        'index': index,
        'q': qs,
        'count': count,
        'restricted': reader.restricted_count - restricted_count,
        'progress': {
            'bytes_read': reader.bytes_read,
            'bytes_total': reader.bytes_total,
        },
        'page': page,
        'limit': reader.limit,
        'data': fetched_records,
        'continuation': token,
        'nonce': nonce(),
    }
