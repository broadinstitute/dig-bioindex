import asyncio
import concurrent.futures
import re
from enum import Enum
from typing import List, Optional

import fastapi
import graphql
from pydantic import BaseModel

from .utils import *
from ..lib import config
from ..lib import continuation
from ..lib import index
from ..lib import ql
from ..lib import query
from ..lib import signed_tokens
from ..lib.auth import restricted_keywords
from ..lib.generation import index_generation
from ..lib.utils import nonce, profile, profile_async

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


def _load_indexes():
    """
    Create a cache of the indexes in the database.
    """
    indexes = index.Index.list_indexes(engine, filter_built=False)
    return dict(((i.name, int(i.schema.arity)), i) for i in indexes)


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
    for i in sorted(INDEXES.values(), key=lambda i: i.name):
        data.append({
            'index': i.name,
            'built': i.built,
            'schema': str(i.schema),
            'compressed': i.compressed,
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
        qs = _parse_query(q)
        i = INDEXES[(index, len(qs))]

        # budget is enforced inside _match_keys so it also applies on /cont
        return _match_keys(i, qs, limit)
    except KeyError:
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/count/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_count_index(index: str, req: fastapi.Request, q: str = None):
    """
    Query the database and estimate how many records will be returned.
    """
    try:
        qs = _parse_query(q)
        i = INDEXES[(index, len(qs))]

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


@router.get('/keys/{index}/{arity}', response_class=fastapi.responses.ORJSONResponse)
async def api_keys_index(index: str, arity: int, req: fastapi.Request, columns: str = None):
    """
    Query the database and return all non-locus keys.
    """
    try:
        if columns is not None:
            columns = columns.split(',')
        i = INDEXES[(index, arity)]

        keys, query_s = profile(query.fetch_keys, engine, i, columns)

        return {
            'profile': {
                'query': query_s,
            },
            'index': index,
            'keys': keys,
            'nonce': nonce(),
        }
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/varIdLookup/{rsid}', response_class=fastapi.responses.ORJSONResponse)
async def api_lookup_variant_for_rs_id(rsid: str):
    """
    Lookup the variant ID for a given rsID.
    """
    dynamodb_table = CONFIG.variant_dynamodb_table
    data, fetch_s = profile(aws.look_up_var_id, rsid, dynamodb_table)
    return {
        'profile': {
          'dynamo_fetch': fetch_s
        },
        'index': dynamodb_table,
        'q': rsid,
        'data': data
    }


@router.get('/query/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_query_index(index: str, q: str, req: fastapi.Request, fmt='row', limit: int = None):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    global INDEXES

    try:
        qs = _parse_query(q, required=True)
        # in the event we've added a new index
        if (index, len(qs)) not in INDEXES:
            INDEXES = _load_indexes()
        i = INDEXES[(index, len(qs))]

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
    # restricted, auth_s = profile(restricted_keywords, portal, req)
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
        raise fastapi.HTTPException(status_code=408,
                                    detail=f'Query execution timed out after {CONFIG.script_timeout} seconds')
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
        qs = _parse_query(q, required=True)
        i = INDEXES[(index, len(qs))]

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
async def api_cont(token: str, req: fastapi.Request):
    """
    Decode a signed continuation token and resume the paginated query.
    """
    global INDEXES

    # Verify HMAC signature — constant-time, before any parsing
    try:
        state = signed_tokens.decode(token, signed_tokens.signing_key())
    except signed_tokens.TokenError as e:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'Invalid continuation token: {e}',
        )

    # Look up the index (refresh once on miss in case a new index was added)
    i = INDEXES.get((state.index_name, state.index_arity))
    if i is None:
        INDEXES = _load_indexes()
        i = INDEXES.get((state.index_name, state.index_arity))
        if i is None:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Index '{state.index_name}' no longer present",
            )

    # Generation check — reject if the index was rebuilt since the token was issued
    current_gen = index_generation(engine, state.index_name)
    if state.generation != current_gen:
        raise fastapi.HTTPException(
            status_code=409,
            detail="continuation is stale (index was rebuilt); re-run the query",
        )

    # Re-derive restricted set from current requester (not from token)
    restricted, _ = profile(restricted_keywords, portal, req) if portal else (None, 0)

    try:
        if state.type == 'fetch':
            reader, query_s = profile(
                query.fetch,
                CONFIG,
                engine,
                i,
                state.qs,
                restricted=restricted,
                start_source_index=state.source_index,
                start_byte_offset=state.byte_offset,
            )
            if state.limit is not None:
                reader.set_limit(state.limit)
            return _fetch_records(reader, state.index_name, state.qs, state.fmt,
                                  page=state.page, query_s=query_s)

        elif state.type == 'match':
            return _match_keys(i, state.qs, state.limit,
                               after=state.last_key, page=state.page)

        else:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f'Unknown continuation type: {state.type}',
            )

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


def _match_keys(i, qs, limit, after=None, page=1):
    """
    Fetch one page of distinct match keys for index object `i` via keyset
    pagination and build the JSON response. `limit` caps TOTAL keys across
    continuation pages (not per page); `after` resumes past the previous page's
    last key.
    """
    # per-page cap, further bounded by the remaining budget
    page_size = MATCH_LIMIT if limit is None else min(MATCH_LIMIT, limit)
    fetched, query_s = profile(query.match, CONFIG, engine, i, qs, after, page_size)

    # remaining budget after this page (None == unlimited)
    remaining = None if limit is None else limit - len(fetched)

    # mint a continuation only if this page filled (more data likely) AND the
    # budget is not exhausted; carry the decremented budget into the token.
    token = None
    if len(fetched) >= page_size and (remaining is None or remaining > 0):
        state = continuation.ContState(
            type='match',
            index_name=i.name,
            index_arity=len(qs),
            qs=qs,
            fmt=None,
            limit=remaining,
            last_key=fetched[-1] if fetched else None,
            page=page + 1,
            generation=index_generation(engine, i.name),
        )
        try:
            token = signed_tokens.encode(state, signed_tokens.signing_key())
        except signed_tokens.TokenError as e:
            raise fastapi.HTTPException(status_code=413, detail=str(e))

    return {
        'profile': {
            'fetch': query_s,
            'query': query_s,
        },
        'index': i.name,
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

    # create a signed continuation token if there is more data
    token = None
    if not reader.at_end:
        state = continuation.ContState(
            type='fetch',
            index_name=index,
            # Always carry the reader's index schema arity. This is correct for
            # /query (where len(qs) == schema arity) and for /all on every page
            # including resume (where qs is [] so len(qs)==0 would be wrong).
            index_arity=int(reader.index.schema.arity),
            qs=qs or [],
            fmt=fmt,
            page=page + 1,
            source_index=reader._source_index,
            byte_offset=reader._source_byte_offset,
            # Carry the REMAINING budget so each page decrements it. Carrying
            # the full reader.limit would let /cont re-issue up to N records per
            # page, returning far more than N total across pages.
            limit=(reader.limit - count) if reader.limit is not None else None,
            generation=index_generation(engine, index),
        )
        try:
            token = signed_tokens.encode(state, signed_tokens.signing_key())
        except signed_tokens.TokenError as e:
            raise fastapi.HTTPException(status_code=413, detail=str(e))

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
