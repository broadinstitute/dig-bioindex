import asyncio
import concurrent.futures
import itertools
from typing import List, Optional

import fastapi
import graphql
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel

from .utils import *
from ..lib import continuation
from ..lib import index
from ..lib import query
from ..lib import signed_tokens
from ..lib.auth import restricted_keywords
from ..lib.utils import nonce, profile, profile_async
from ..middleware.portal import get_portal_ctx

router = fastapi.APIRouter()

# multi-query executor (stateless, shared across portals)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)


class Query(BaseModel):
    q: List[str]
    fmt: Optional[str] = 'row'
    limit: Optional[int] = None


def _refresh_indexes(ctx):
    """
    Rebuild the portal's index cache from RDS. Called on cache miss
    (e.g., a new index was created via the CLI since this process started).
    Atomic — replaces the dict reference rather than mutating in place.
    """
    fresh = {(i.name, int(i.schema.arity)): i for i in index.Index.list_indexes(ctx.engine, filter_built=False)}
    ctx.indexes = fresh


@router.get('/indexes', response_class=fastapi.responses.ORJSONResponse)
async def api_list_indexes(req: fastapi.Request):
    """
    Return all queryable indexes. This also refreshes the internal
    cache of the table so the server doesn't need to be bounced when
    the table is updated (very rare!).
    """
    ctx = get_portal_ctx(req)

    # update the portal's index cache
    _refresh_indexes(ctx)
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
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            raise KeyError

        # execute the query
        keys, query_s = profile(query.match, ctx.config, ctx.engine, i, qs)

        # allow an upper limit on the total number of keys returned
        if limit is not None:
            keys = itertools.islice(keys, limit)

        # read the matched keys
        return _match_keys(ctx, keys, index, qs, limit, query_s=query_s)
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
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            raise KeyError

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
        i = ctx.indexes.get((index, arity))
        if i is None:
            raise KeyError

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


@router.get('/all/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_all(index: str, req: fastapi.Request, fmt: str = 'row'):
    """
    Query the database and return ALL records for a given index. If the
    total number of bytes read exceeds a pre-configured server limit, then
    a 413 response will be returned. If multiple indexes share a name
    with different arity it'll throw a 400.
    """
    ctx = get_portal_ctx(req)
    try:
        idxs = [idx for key, idx in ctx.indexes.items() if key[0] == index]

        if len(idxs) == 0:
            raise KeyError
        elif len(idxs) == 1:
            # discover what the user doesn't have access to see
            restricted, auth_s = profile(restricted_keywords, ctx.portal, req) if ctx.portal else (None, 0)

            # lookup the schema for this index and perform the query
            reader, query_s = profile(
                query.fetch_all,
                ctx.config,
                idxs[0],
                restricted=restricted,
            )

            # will this request exceed the limit?
            if reader.bytes_total > ctx.config.response_limit_max:
                raise fastapi.HTTPException(status_code=413)

            # fetch records from the reader
            return _fetch_records(ctx, reader, index, None, fmt, restricted=restricted,
                                  cont_type='all', query_s=auth_s + query_s)
        else:
            raise ValueError(f'Multiple indexes found for {index}, try arity-specific endpoint')
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/all/{index}/{arity}', response_class=fastapi.responses.ORJSONResponse)
async def api_all_arity(index: str, arity: int, req: fastapi.Request):
    """
    Query the database fetch ALL records for a given index and arity. Don't read
    the records from S3, but instead set the Content-Length to the total
    number of bytes what would be read.
    """
    ctx = get_portal_ctx(req)
    try:
        i = ctx.indexes.get((index, arity))
        if i is None:
            raise KeyError

        # discover what the user doesn't have access to see
        restricted, auth_s = profile(restricted_keywords, ctx.portal, req) if ctx.portal else (None, 0)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch_all,
            ctx.config,
            i,
            restricted=restricted,
        )

        # will this request exceed the limit?
        if reader.bytes_total > ctx.config.response_limit_max:
            raise fastapi.HTTPException(status_code=413)

        # fetch records from the reader
        return _fetch_records(ctx, reader, index, None, fmt, restricted=restricted,
                              cont_type='all', query_s=auth_s + query_s)
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/all/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_test_all(index: str, req: fastapi.Request):
    """
    Query the database fetch ALL records for a given index. Don't read
    the records from S3, but instead set the Content-Length to the total
    number of bytes what would be read. If multiple indexes share a name
    with different arity it'll throw a 400.
    """
    ctx = get_portal_ctx(req)
    try:
        idxs = [idx for key, idx in ctx.indexes.items() if key[0] == index]

        if len(idxs) == 0:
            raise KeyError
        elif len(idxs) == 1:
            # lookup the schema for this index and perform the query
            reader, query_s = profile(
                query.fetch_all,
                ctx.config,
                idxs[0],
            )

            # return the total number of bytes that need to be read
            return fastapi.Response(headers={'Content-Length': str(reader.bytes_total)})
        else:
            raise ValueError(f'Multiple indexes found for {index}, try arity-specific endpoint')
    except KeyError:
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/all/{index}/{arity}', response_class=fastapi.responses.ORJSONResponse)
async def api_test_all_arity(index: str, arity: int, req: fastapi.Request):
    """
    Query the database fetch ALL records for a given index and arity. Don't read
    the records from S3, but instead set the Content-Length to the total
    number of bytes what would be read.
    """
    ctx = get_portal_ctx(req)
    try:
        i = ctx.indexes.get((index, arity))
        if i is None:
            raise KeyError

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch_all,
            ctx.config,
            i,
        )

        # return the total number of bytes that need to be read
        return fastapi.Response(headers={'Content-Length': str(reader.bytes_total)})
    except KeyError:
        raise fastapi.HTTPException(
            status_code=400, detail=f'Invalid index: {index}')
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
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            _refresh_indexes(ctx)
            i = ctx.indexes.get((index, len(qs)))
            if i is None:
                raise KeyError

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
        return _fetch_records(ctx, reader, index, qs, fmt, restricted=restricted,
                              cont_type='fetch', query_s=auth_s + query_s)
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
    # restricted, auth_s = profile(restricted_keywords, ctx.portal, req)
    body = await req.body()

    # ensure the graphql schema is loaded
    if ctx.gql_schema is None:
        raise fastapi.HTTPException(status_code=503, detail='GraphQL Schema not built')

    try:
        query = body.decode(encoding='utf-8')

        # execute the query asynchronously using the schema
        co = asyncio.wait_for(
            graphql.graphql(ctx.gql_schema, query),
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
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            raise KeyError

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

    try:
        state = signed_tokens.decode(token, signed_tokens.signing_key())
    except signed_tokens.TokenError as e:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f'Invalid or expired continuation token: {e}',
        )

    # C1: tokens are bound to the portal that issued them. A token minted
    # under portal A must not be replayable against portal B (which would
    # resolve to B's indexes/data with A's resume state).
    if state.portal_name != ctx.name:
        raise fastapi.HTTPException(
            status_code=403,
            detail='Token issued for different portal',
        )

    i = ctx.indexes.get((state.index_name, state.index_arity))
    if i is None:
        _refresh_indexes(ctx)
        i = ctx.indexes.get((state.index_name, state.index_arity))
        if i is None:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Index '{state.index_name}' no longer present",
            )

    # C2: re-derive the restricted set from the *current* requester's
    # identity, not from anything embedded in (or omitted from) the token.
    # This ensures revoked permissions take effect immediately and leaked
    # tokens don't grant the original requester's access to a third party.
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
                                  restricted=restricted, cont_type='fetch',
                                  page=state.page, query_s=query_s)

        elif state.type == 'all':
            reader, query_s = profile(
                query.fetch_all,
                ctx.config,
                i,
                restricted=restricted,
                start_source_index=state.source_index,
                start_byte_offset=state.byte_offset,
            )
            if state.limit is not None:
                reader.set_limit(state.limit)
            return _fetch_records(ctx, reader, state.index_name, state.qs, state.fmt,
                                  restricted=restricted, cont_type='all',
                                  page=state.page, query_s=query_s)

        elif state.type == 'match':
            all_keys = query.match(ctx.config, ctx.engine, i, state.qs)
            # skip past keys already returned on previous pages
            if state.last_key is not None:
                all_keys = itertools.dropwhile(lambda k: k <= state.last_key, all_keys)
            return _match_keys(ctx, all_keys, state.index_name, state.qs, state.limit,
                               page=state.page)

        else:
            raise fastapi.HTTPException(status_code=400, detail=f'Unknown continuation type: {state.type}')

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


def _match_keys(ctx, keys, index, qs, limit, page=1, query_s=None):
    """
    Collects up to match_limit keys from a database cursor and then
    return a JSON response object with them.
    """
    match_limit = ctx.config.match_limit
    fetched, fetch_s = profile(list, itertools.islice(keys, match_limit))

    # create a continuation if there is more data
    token = None
    if len(fetched) >= match_limit:
        state = continuation.ContState(
            type='match',
            index_name=index,
            index_arity=len(qs),
            qs=qs,
            portal_name=ctx.name,
            limit=limit,
            last_key=fetched[-1] if fetched else None,
            page=page + 1,
        )
        try:
            token = signed_tokens.encode(state, signed_tokens.signing_key())
        except signed_tokens.TokenError as e:
            raise fastapi.HTTPException(status_code=413, detail=str(e))

    body = {
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
    headers = {'Cache-Control': 'no-store'} if token is not None else None
    return ORJSONResponse(content=body, headers=headers)


def _fetch_records(ctx, reader, index, qs, fmt, restricted=None, cont_type='fetch', page=1, query_s=None):
    """
    Reads up to response_limit bytes from a RecordReader, format them,
    and then return a JSON response object with the records.
    """
    response_limit = ctx.config.response_limit
    response_limit_max = ctx.config.response_limit_max
    bytes_limit = reader.bytes_read + response_limit
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
    if reader.bytes_read > response_limit_max:
        raise fastapi.HTTPException(status_code=413)

    # transform a list of dictionaries into a dictionary of lists
    if fmt[0] == 'c':
        fetched_records = {
            k: [r.get(k) for r in fetched_records]
            for k in fetched_records[0].keys()
        }

    # create a continuation if there is more data
    token = None
    if not reader.at_end:
        # index_arity must be the SCHEMA arity (used by ctx.indexes lookup),
        # not len(qs). For /all (cont_type='all') qs is None, so deriving from
        # len(qs) would give 0 and fail the lookup on resume. The reader knows
        # its source index, which knows its schema.
        state = continuation.ContState(
            type=cont_type,
            index_name=index,
            index_arity=int(reader.index.schema.arity),
            qs=qs,
            portal_name=ctx.name,
            fmt=fmt,
            limit=reader.limit,
            page=page + 1,
            source_index=reader._source_index,
            byte_offset=reader._source_byte_offset,
        )
        try:
            token = signed_tokens.encode(state, signed_tokens.signing_key())
        except signed_tokens.TokenError as e:
            raise fastapi.HTTPException(status_code=413, detail=str(e))

    # build JSON response
    body = {
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
    headers = {'Cache-Control': 'no-store'} if token is not None else None
    return ORJSONResponse(content=body, headers=headers)
