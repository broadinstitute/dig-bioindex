import asyncio
import concurrent.futures
from bioindex.lib.region_set import Region, RegionSet, Variant
import fastapi
import graphql
import itertools
import re

from pydantic import BaseModel, Json
from pydantic.validators import str_validator
from typing import List, Optional

from .utils import *

from ..lib import config
from ..lib import continuation
from ..lib import index
from ..lib import ql
from ..lib import query
from ..lib.auth import restricted_keywords
from ..lib.utils import nonce, profile, profile_async
from ..lib.region_set import RegionSet, Variant

# load dot files and configuration
CONFIG = config.Config()

# create flask app; this will load .env
router = fastapi.APIRouter()

# connect to database
engine = connect_to_bio(CONFIG)
portal = connect_to_portal(CONFIG)

# max number of bytes to read from s3 per request
RESPONSE_LIMIT = CONFIG.response_limit
RESPONSE_LIMIT_MAX = CONFIG.response_limit_max
MATCH_LIMIT = CONFIG.match_limit

# multi-query executor
executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

# by default, there is no graphql schema
gql_schema = None

# if the graphql schema file exists, load it
if CONFIG.graphql_schema:
    gql_schema = ql.load_schema(CONFIG, engine, CONFIG.graphql_schema)


class Query(BaseModel):
    q: List[str]
    fmt: Optional[str] = 'row'
    limit: Optional[int] = None

__VARIANT_DELIM = ':'

def __variant_from_str(s): 
  if s is None or s.strip() == '':
    raise ValueError("Can't parse variant string '%s'" % s)

  parts = s.split(__VARIANT_DELIM)

  try:
    return Variant(parts[0], int(parts[1]))
  except:
    raise ValueError("Can't parse variant string '%s'" % s)

class RegionMembershipQuery(BaseModel):
  limit: int
  variants: List[str]

def _load_indexes():
    """
    Create a cache of the indexes in the database.
    """
    indexes = index.Index.list_indexes(engine, filter_built=False)
    return dict((i.name, i) for i in indexes)


# initialize with all the indexes, get them all, whether built or not
INDEXES = _load_indexes()

REGION_SET = RegionSet(CONFIG.regions_dir)

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
    for i in sorted(INDEXES.values(), key=lambda i: i.name):
        data.append({
            'index': i.name,
            'built': i.built,
            'schema': str(i.schema),
            'query': {
                'keys': i.schema.key_columns,
                'locus': i.schema.has_locus,
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
        i = INDEXES[index]
        qs = _parse_query(q)

        # execute the query
        keys, query_s = profile(query.match, CONFIG, engine, i, qs)

        # allow an upper limit on the total number of keys returned
        if limit is not None:
            keys = itertools.islice(keys, limit)

        # read the matched keys
        return _match_keys(keys, index, qs, limit, query_s=query_s)
    except KeyError:
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/count/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_count_index(index: str, req: fastapi.Request, q: str=None):
    """
    Query the database and estimate how many records will be returned.
    """
    try:
        i = INDEXES[index]
        qs = _parse_query(q)

        # lookup the schema for this index and perform the query
        count, query_s = profile(query.count, CONFIG, engine, i, qs)

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
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/all/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_all(index: str, req: fastapi.Request, fmt: str='row'):
    """
    Query the database and return ALL records for a given index. If the
    total number of bytes read exceeds a pre-configured server limit, then
    a 413 response will be returned.
    """
    try:
        i = INDEXES[index]

        # discover what the user doesn't have access to see
        restricted, auth_s = profile(restricted_keywords, portal, req) if portal else (None, 0)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch_all,
            CONFIG,
            i.s3_prefix,
            restricted=restricted,
        )

        # will this request exceed the limit?
        if reader.bytes_total > RESPONSE_LIMIT_MAX:
            raise fastapi.HTTPException(status_code=413)

        # fetch records from the reader
        return _fetch_records(reader, index, None, fmt, query_s=auth_s + query_s)
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
        i = INDEXES[index]

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch_all,
            CONFIG,
            i.s3_prefix,
        )

        # return the total number of bytes that need to be read
        return fastapi.Response(headers={'Content-Length': str(reader.bytes_total)})
    except KeyError:
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/query/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_query_index(index: str, q: str, req: fastapi.Request, fmt='row', limit: int=None):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    try:
        i = INDEXES[index]
        qs = _parse_query(q, required=True)

        # discover what the user doesn't have access to see
        restricted, auth_s = profile(restricted_keywords, portal, req) if portal else (None, 0)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch,
            CONFIG,
            engine,
            i,
            qs,
            restricted=restricted,
        )

        # with no limit, will this request exceed the limit?
        if not limit and reader.bytes_total > RESPONSE_LIMIT_MAX:
            raise fastapi.HTTPException(status_code=413)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            reader.set_limit(limit)

        # the results of the query
        return _fetch_records(reader, index, qs, fmt, query_s=auth_s + query_s)
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/schema', response_class=fastapi.responses.PlainTextResponse)
async def api_schema(req: fastapi.Request):
    """
    Returns the GraphQL schema definition (SDL).
    """
    if gql_schema is None:
        raise fastapi.HTTPException(status_code=503, detail='GraphQL Schema not built')

    return graphql.utilities.print_schema(gql_schema)


@router.post('/query', response_class=fastapi.responses.ORJSONResponse)
async def api_query_gql(req: fastapi.Request):
    """
    Treat the body of the POST as a GraphQL query to be resolved.
    """
    #restricted, auth_s = profile(restricted_keywords, portal, req)
    body = await req.body()

    # ensure the graphql schema is loaded
    if gql_schema is None:
        raise fastapi.HTTPException(status_code=503, detail='GraphQL Schema not built')

    try:
        query = body.decode(encoding='utf-8')

        # execute the query asynchronously using the schema
        co = asyncio.wait_for(
            graphql.graphql(gql_schema, query),
            timeout=CONFIG.script_timeout,
        )

        # wait for it to complete
        result, query_s = await profile_async(co)

        if result.errors:
            raise fastapi.HTTPException(
                status_code=400,
                detail=[str(e) for e in result.errors],
            )

        # send the response
        return {
            'profile': {
                'query': query_s,
            },
            'q': body,
            'count': {k: len(v) for k, v in result.data.items()},
            'data': result.data,
            'nonce': nonce(),
        }
    except asyncio.TimeoutError:
        raise fastapi.HTTPException(status_code=408, detail=f'Query execution timed out after {CONFIG.script_timeout} seconds')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/query/{index}')
async def api_test_index(index: str, q: str, req: fastapi.Request):
    """
    Query the database for records matching the query parameter. Don't
    read the records from S3, but instead set the Content-Length to the
    total number of bytes what would be read. If the total number of
    bytes read exceeds a pre-configured server limit, then a 413
    response will be returned.
    """
    try:
        i = INDEXES[index]
        qs = _parse_query(q, required=True)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(query.fetch, engine, CONFIG.s3_bucket, i, qs)

        return fastapi.Response(
            headers={'Content-Length': str(reader.bytes_total)})
    except KeyError:
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/cont', response_class=fastapi.responses.ORJSONResponse)
async def api_cont(token: str):
    """
    Lookup a continuation token and get the next set of records.
    """
    try:
        cont = continuation.lookup_continuation(token)

        # the token is no longer valid
        continuation.remove_continuation(token)

        # execute the continuation callback
        return cont.callback(cont)

    except KeyError:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Invalid, expired, or missing continuation token')
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
    token = None if len(fetched) < MATCH_LIMIT else continuation.make_continuation(
        callback=lambda cont: _match_keys(keys, index, limit, qs, page=page + 1),
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

    # did the reader exceed the configured, maximum number of bytes to read?
    if reader.bytes_read > RESPONSE_LIMIT_MAX:
        raise fastapi.HTTPException(status_code=413)

    # transform a list of dictionaries into a dictionary of lists
    if fmt[0] == 'c':
        fetched_records = {
            k: [r.get(k) for r in fetched_records]
            for k in fetched_records[0].keys()
        }

    # create a continuation if there is more data
    token = None if reader.at_end else continuation.make_continuation(
        callback=lambda cont: _fetch_records(reader, index, qs, fmt, page=page + 1),
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

@router.post('/regions-variants', response_class=fastapi.responses.ORJSONResponse)
async def api_query_region_membership(query: RegionMembershipQuery):
  def fetch_data(page=0):
    try:
      parsed_variants = [__variant_from_str(v) for v in query.variants]

      result = REGION_SET.regions_containing(parsed_variants)

      num_to_drop = page * query.limit

      after_dropped = itertools.islice(result, num_to_drop, None)

      to_return = list(itertools.islice(after_dropped, query.limit))

      no_more_pages = len(to_return) == 0 or len(to_return) <= query.limit

      # create a continuation if there is more data
      token = None if no_more_pages else continuation.make_continuation(
          callback=lambda cont: fetch_data(page=page + 1),
      )

      return {
        'page': page,
        'continuation': token,
        'nonce': nonce(),
        'data': to_return
      }
    except asyncio.TimeoutError:
        raise fastapi.HTTPException(status_code=408, detail=f'Query execution timed out after {CONFIG.script_timeout} seconds')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))

  return fetch_data()

