import asyncio
import concurrent.futures
from typing import List, Optional

import fastapi
import graphql
from pydantic import BaseModel

from ..lib import aws
from ..lib import continuation
from ..lib import index
from ..lib import query
from ..lib import signed_tokens
from ..lib.auth import restricted_keywords
from ..lib.generation import index_generation
from ..lib.utils import nonce, profile, profile_async
from ..middleware.portal import get_portal_ctx

# create flask app; this will load .env
router = fastapi.APIRouter()

# multi-query executor
executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)


class Query(BaseModel):
    q: List[str]
    fmt: Optional[str] = 'row'
    limit: Optional[int] = None


def _load_indexes(ctx):
    """
    Create a cache of the indexes in the database.
    """
    indexes = index.Index.list_indexes(ctx.engine, filter_built=False)
    return dict(((i.name, int(i.schema.arity)), i) for i in indexes)


@router.get('/indexes', response_class=fastapi.responses.ORJSONResponse)
async def api_list_indexes(req: fastapi.Request):
    """
    Return all queryable indexes. This also refreshes the internal
    cache of the table so the server doesn't need to be bounced when
    the table is updated (very rare!).
    """
    ctx = get_portal_ctx(req)

    # update the portal's index cache
    ctx.indexes = _load_indexes(ctx)
    data = []

    # add each index to the response data
    for i in sorted(ctx.indexes.values(), key=lambda i: i.name):
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
    ctx = get_portal_ctx(req)

    try:
        qs = _parse_query(q)
        i = ctx.indexes[(index, len(qs))]

        # budget is enforced inside _match_keys so it also applies on /cont
        return _match_keys(ctx, i, qs, limit)
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
    ctx = get_portal_ctx(req)

    try:
        qs = _parse_query(q)
        i = ctx.indexes[(index, len(qs))]

        # lookup the schema for this index and perform the query
        count, query_s = profile(query.count, ctx.config, ctx.engine, i, qs)

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
    ctx = get_portal_ctx(req)

    try:
        if columns is not None:
            columns = columns.split(',')
        i = ctx.indexes[(index, arity)]

        keys, query_s = profile(query.fetch_keys, ctx.engine, i, columns)

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
async def api_lookup_variant_for_rs_id(rsid: str, req: fastapi.Request):
    """
    Lookup the variant ID for a given rsID.
    """
    ctx = get_portal_ctx(req)

    dynamodb_table = ctx.config.variant_dynamodb_table
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
    ctx = get_portal_ctx(req)

    try:
        qs = _parse_query(q, required=True)
        # in the event we've added a new index
        if (index, len(qs)) not in ctx.indexes:
            ctx.indexes = _load_indexes(ctx)
        i = ctx.indexes[(index, len(qs))]

        # discover what the user doesn't have access to see
        restricted, auth_s = profile(restricted_keywords, ctx.portal, req) if ctx.portal else (None, 0)
        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch,
            ctx.config,
            ctx.engine,
            i,
            qs,
            restricted=restricted,
        )

        # with no limit, will this request exceed the limit?
        if not limit and reader.bytes_total > ctx.config.response_limit_max:
            raise fastapi.HTTPException(status_code=413)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            reader.set_limit(limit)

        # the results of the query
        return _fetch_records(ctx, reader, index, qs, fmt, query_s=auth_s + query_s)
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/schema', response_class=fastapi.responses.PlainTextResponse)
async def api_schema(req: fastapi.Request):
    """
    Returns the GraphQL schema definition (SDL).
    """
    ctx = get_portal_ctx(req)

    if ctx.gql_schema is None:
        raise fastapi.HTTPException(status_code=503, detail='GraphQL Schema not built')

    return graphql.utilities.print_schema(ctx.gql_schema)


@router.post('/query', response_class=fastapi.responses.ORJSONResponse)
async def api_query_gql(req: fastapi.Request):
    """
    Treat the body of the POST as a GraphQL query to be resolved.
    """
    ctx = get_portal_ctx(req)
    body = await req.body()

    # ensure the graphql schema is loaded
    if ctx.gql_schema is None:
        raise fastapi.HTTPException(status_code=503, detail='GraphQL Schema not built')

    try:
        gql_query = body.decode(encoding='utf-8')

        # execute the query asynchronously using the schema
        co = asyncio.wait_for(
            graphql.graphql(ctx.gql_schema, gql_query),
            timeout=ctx.config.script_timeout,
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
                                    detail=f'Query execution timed out after {ctx.config.script_timeout} seconds')
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
    ctx = get_portal_ctx(req)

    try:
        qs = _parse_query(q, required=True)
        i = ctx.indexes[(index, len(qs))]

        # lookup the schema for this index and perform the query
        reader, query_s = profile(query.fetch, ctx.engine, ctx.config.s3_bucket, i, qs)

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
    ctx = get_portal_ctx(req)

    # Verify HMAC signature — constant-time, before any parsing
    try:
        state = signed_tokens.decode(token, signed_tokens.signing_key())
    except signed_tokens.TokenError as e:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'Invalid continuation token: {e}',
        )

    # A token is bound to the portal that issued it; don't resume it elsewhere
    if state.portal_name != ctx.name:
        raise fastapi.HTTPException(
            status_code=400,
            detail='Continuation token is for a different portal',
        )

    # Look up the index (refresh once on miss in case a new index was added)
    i = ctx.indexes.get((state.index_name, state.index_arity))
    if i is None:
        ctx.indexes = _load_indexes(ctx)
        i = ctx.indexes.get((state.index_name, state.index_arity))
        if i is None:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Index '{state.index_name}' no longer present",
            )

    # Generation check — reject if the index was rebuilt since the token was issued
    if state.generation != index_generation(ctx.engine, state.index_name):
        raise fastapi.HTTPException(
            status_code=409,
            detail="continuation is stale (index was rebuilt); re-run the query",
        )

    # Re-derive restricted set from current requester (not from token)
    restricted, _ = profile(restricted_keywords, ctx.portal, req) if ctx.portal else (None, 0)

    try:
        if state.type == 'fetch':
            reader, query_s = profile(
                query.fetch,
                ctx.config,
                ctx.engine,
                i,
                state.qs,
                restricted=restricted,
                start_source_index=state.source_index,
                start_byte_offset=state.byte_offset,
            )
            if state.limit is not None:
                reader.set_limit(state.limit)
            return _fetch_records(ctx, reader, state.index_name, state.qs, state.fmt,
                                  page=state.page, query_s=query_s)

        elif state.type == 'match':
            return _match_keys(ctx, i, state.qs, state.limit,
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


def _match_keys(ctx, i, qs, limit, after=None, page=1):
    """
    Fetch one page of distinct match keys for index object `i` via keyset
    pagination and build the JSON response. `limit` caps TOTAL keys across
    continuation pages (not per page); `after` resumes past the previous page's
    last key.
    """
    # per-page cap, further bounded by the remaining budget
    match_limit = ctx.config.match_limit
    page_size = match_limit if limit is None else min(match_limit, limit)
    fetched, query_s = profile(query.match, ctx.config, ctx.engine, i, qs, after, page_size)

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
            portal_name=ctx.name,
            fmt=None,
            limit=remaining,
            last_key=fetched[-1] if fetched else None,
            page=page + 1,
            generation=index_generation(ctx.engine, i.name),
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


def _fetch_records(ctx, reader, index, qs, fmt, page=1, query_s=None):
    """
    Reads up to the portal's response limit of bytes from a RecordReader,
    format them, and then return a JSON response object with the records.
    """
    bytes_limit = reader.bytes_read + ctx.config.response_limit
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
    if reader.bytes_read > ctx.config.response_limit_max:
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
            portal_name=ctx.name,
            fmt=fmt,
            page=page + 1,
            source_index=reader._source_index,
            byte_offset=reader._source_byte_offset,
            # Carry the REMAINING budget so each page decrements it. Carrying
            # the full reader.limit would let /cont re-issue up to N records per
            # page, returning far more than N total across pages.
            limit=(reader.limit - count) if reader.limit is not None else None,
            generation=index_generation(ctx.engine, index),
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
