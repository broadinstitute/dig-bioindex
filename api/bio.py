import dotenv
import fastapi
import os

import lib.config
import lib.continuation
import lib.query
import lib.secrets

from lib.profile import profile


# load dot files and configuration
dotenv.load_dotenv()
config = lib.config.Config()

# create flask app; this will load .env
router = fastapi.APIRouter()

# connect to database
engine = lib.secrets.connect_to_mysql(config.rds_instance, schema='bio')

# max number of bytes to return per request
RESPONSE_LIMIT = int(os.getenv('BIOINDEX_RESPONSE_LIMIT', 1 * 1024 * 1024))


@router.get('/api/indexes')
async def api_indexes():
    """
    Return all queryable indexes.
    """
    indexes = []

    for k in config.indexes.keys():
        index = config.index(k)

        indexes.append({
            'index': k,
            'schema': str(index.schema),
            'query': {
                'keys': index.schema.key_columns,
                'locus': index.schema.has_locus,
            },
        })

    return {
        'count': len(indexes),
        'data': indexes,
    }


@router.get('/api/keys/{index}')
async def api_keys(index: str, q: str = None):
    """
    Return all the unique keys for a value-indexed table.
    """
    try:
        idx = config.index(index)

        # get the partial query parameters to apply
        qs = parse_query(q)

        # execute the query
        keys, query_s = profile(lib.query.keys, engine, idx, qs)
        fetched = list(keys)

        return {
            'profile': {
                'query': query_s,
            },
            'index': index,
            'count': len(fetched),
            'data': list(fetched),
        }
    except AssertionError:
        raise fastapi.HTTPException(status_code=400, detail=f'Index {index} is not indexed by value')
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/api/count/{index}')
async def api_count(index: str, q: str):
    """
    Query the database and estimate how many records will be returned.
    """
    try:
        qs = parse_query(q)

        # lookup the schema for this index and perform the query
        idx = config.index(index)
        count, query_s = profile(lib.query.count, engine, config.s3_bucket, idx, qs)

        return {
            'profile': {
                'query': query_s,
            },
            'index': index,
            'q': qs,
            'count': count,
        }
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/api/all/{index}')
async def api_all(index: str, fmt: str = 'row', limit: int = None):
    """
    Query the database and return ALL records for a given index.
    """
    try:
        idx = config.index(index)

        # validate query parameters
        if fmt.lower() not in ['r', 'row', 'c', 'col', 'column']:
            raise ValueError('Invalid output format')

        # lookup the schema for this index and perform the query
        reader, query_s = profile(lib.query.fetch_all, config.s3_bucket, idx.s3_prefix)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            reader.set_limit(limit)

        # fetch the records from S3
        fetched_records, fetch_s, count = fetch_records(reader, fmt)
        needs_cont = not reader.at_end

        # make a continuation token if there are more records left to read
        cont_token = None if not needs_cont else lib.continuation.make_continuation(
            reader=reader,
            idx=index,
            fmt=fmt,
        )

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': index,
            'count': count,
            'progress': {
                'bytes_read': reader.bytes_read,
                'bytes_total': reader.bytes_total,
            },
            'page': 1,
            'limit': reader.limit,
            'data': fetched_records,
            'continuation': cont_token,
        }
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/api/query/{index}')
async def api_test(index: str, q: str):
    """
    Query the database for records matching the query parameter. Don't
    read the records from S3, but instead set the Content-Length to the
    total number of bytes what would be read.
    """
    try:
        qs = parse_query(q, required=True)

        # lookup the schema for this index and perform the query
        idx = config.index(index)
        reader, query_s = profile(lib.query.fetch, engine, config.s3_bucket, idx, qs)

        return fastapi.Response(headers={'Content-Length': reader.bytes_total})
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/api/query/{index}')
async def api_query(index: str, q: str, fmt='row', limit: int = None):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    try:
        qs = parse_query(q, required=True)

        # validate query parameters
        if fmt.lower() not in ['r', 'row', 'c', 'col', 'column']:
            raise ValueError('Invalid output format')

        # lookup the schema for this index and perform the query
        idx = config.index(index)
        reader, query_s = profile(lib.query.fetch, engine, config.s3_bucket, idx, qs)

        # HEAD requests just return the number of bytes that would be read
        # if flask.request.method == 'HEAD':
        #     return flask.Response(headers={'Content-Length': reader.bytes_total})

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            reader.set_limit(limit)

        # fetch the records from s3
        fetched_records, fetch_s, count = fetch_records(reader, fmt)
        needs_cont = not reader.at_end

        # make a continuation token if there are more records to-be read
        cont_token = None if not needs_cont else lib.continuation.make_continuation(
            reader=reader,
            idx=index,
            q=qs,
            fmt=fmt,
        )

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': index,
            'q': qs,
            'count': count,
            'progress': {
                'bytes_read': reader.bytes_read,
                'bytes_total': reader.bytes_total,
            },
            'page': 1,
            'limit': reader.limit,
            'data': fetched_records,
            'continuation': cont_token,
        }
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/api/cont')
async def api_cont_test(token: str):
    """
    Lookup a continuation token and determine how many bytes are
    left to be read.
    """
    try:
        cont = lib.continuation.lookup_continuation(token)
        bytes_left = cont.reader.bytes_total - cont.reader.bytes_read

        return fastapi.Response(headers={'Content-Length':bytes_left })
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail='Invalid, expired, or missing continuation token')


@router.get('/api/cont')
async def api_cont(token: str):
    """
    Lookup a continuation token and get the next set of records.
    """
    try:
        cont = lib.continuation.lookup_continuation(token)

        # HEAD requests just return the number of bytes left to-be read
        # if flask.request.method == 'HEAD':
        #     return flask.Response(headers={'Content-Length': cont.reader.bytes_total - cont.reader.bytes_read})

        # fetch more records from S3
        fetched_records, fetch_s, count = fetch_records(cont.reader, cont.fmt)
        needs_cont = not cont.reader.at_end

        # remove the continuation
        lib.continuation.remove_continuation(token)
        token = None

        # create another continuation
        if needs_cont:
            token = lib.continuation.next_continuation(cont)

        return {
            'profile': {
                'fetch': fetch_s,
            },
            'index': cont.idx,
            'q': cont.q,
            'count': count,
            'progress': {
                'bytes_read': cont.reader.bytes_read,
                'bytes_total': cont.reader.bytes_total,
            },
            'page': cont.page + 1,
            'limit': cont.reader.limit,
            'data': fetched_records,
            'continuation': token,
        }
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail='Invalid, expired, or missing continuation token')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


def parse_query(q, required=False):
    """
    Get the `q` query parameter and split it by comma into query parameters
    for a schema query.
    """
    if required and q is None:
        raise ValueError('Missing query parameter')

    # if no query parameter is provided, assume empty string
    return q.split(',') if q else []


def fetch_records(reader, fmt):
    """
    Reads up to RESPONSE_LIMIT bytes from a RecordReader and format
    them before returning.

    Returns the records, how long it took (in seconds), along with the
    count of how many were read.

    NOTE: Use the count returned and NOT len(records), since the
    length of column-major format will the number of columns and not
    the number of records!
    """
    bytes_limit = reader.bytes_read + RESPONSE_LIMIT

    # similar to itertools.takewhile, but keeps the final record
    def take():
        for r in reader.records:
            yield r

            # stop if the byte limit was reached
            if reader.bytes_read > bytes_limit:
                break

    # profile how long it takes to fetch the records from s3
    fetched_records, fetch_s = profile(list, take())
    count = len(fetched_records)

    # transform a list of dictionaries into a dictionary of lists
    if fmt.lower() in ['c', 'col', 'column']:
        fetched_records = {k: [r.get(k) for r in fetched_records] for k in fetched_records[0].keys()}

    # return the fetched records
    return fetched_records, fetch_s, count
