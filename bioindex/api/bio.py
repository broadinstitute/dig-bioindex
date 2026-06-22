import asyncio
import concurrent.futures
from typing import List, Optional

import fastapi
import graphql
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel

from .utils import *
from ..lib import continuation
from ..lib import index
from ..lib import ql
from ..lib import query
from ..lib import signed_tokens
from ..lib.auth import restricted_keywords
from ..lib.generation import index_generation
from ..lib.utils import nonce, profile, profile_async
from ..middleware.portal import get_portal_ctx

# create router
router = fastapi.APIRouter()

# multi-query executor (stateless, shared across portals)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)


class Query(BaseModel):
    q: List[str]
    fmt: Optional[str] = 'row'
    limit: Optional[int] = None


def _finalize(body: dict) -> ORJSONResponse:
    body = dict(body)
    body["nonce"] = nonce()
    return ORJSONResponse(content=body, headers={"Cache-Control": "no-store"})


def _refresh_indexes(ctx):
    fresh = {(i.name, int(i.schema.arity)): i
             for i in index.Index.list_indexes(ctx.engine, filter_built=False)}
    ctx.indexes = fresh


@router.get('/indexes', response_class=fastapi.responses.ORJSONResponse)
async def api_list_indexes(req: fastapi.Request):
    ctx = get_portal_ctx(req)
    _refresh_indexes(ctx)
    data = []
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
    return _finalize({'count': len(data), 'data': data})


@router.get('/match/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_match(index: str, req: fastapi.Request, q: str, limit: int = None):
    ctx = get_portal_ctx(req)
    try:
        qs = _parse_query(q)
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            raise KeyError
        gen = index_generation(ctx.engine, index)
        return _finalize(_match_keys(ctx, i, qs, limit, generation=gen))
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/count/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_count_index(index: str, req: fastapi.Request, q: str = None):
    ctx = get_portal_ctx(req)
    try:
        qs = _parse_query(q)
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            raise KeyError
        count, query_s = profile(query.count, ctx.config, ctx.engine, i, qs)
        return _finalize({
            'profile': {'query': query_s},
            'index': index,
            'q': qs,
            'count': count,
        })
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/keys/{index}/{arity}', response_class=fastapi.responses.ORJSONResponse)
async def api_keys_index(index: str, arity: int, req: fastapi.Request, columns: str = None):
    ctx = get_portal_ctx(req)
    try:
        if columns is not None:
            columns = columns.split(',')
        i = ctx.indexes.get((index, arity))
        if i is None:
            raise KeyError
        keys, query_s = profile(query.fetch_keys, ctx.engine, i, columns)
        return _finalize({
            'profile': {'query': query_s},
            'index': index,
            'keys': keys,
        })
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/varIdLookup/{rsid}', response_class=fastapi.responses.ORJSONResponse)
async def api_lookup_variant_for_rs_id(rsid: str, req: fastapi.Request):
    ctx = get_portal_ctx(req)
    dynamodb_table = ctx.config.variant_dynamodb_table
    data, fetch_s = profile(aws.look_up_var_id, rsid, dynamodb_table)
    body = {
        'profile': {'dynamo_fetch': fetch_s},
        'index': dynamodb_table,
        'q': rsid,
        'data': data,
    }
    return _finalize(body)


@router.get('/query/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_query_index(index: str, q: str, req: fastapi.Request, fmt='row', limit: int = None):
    ctx = get_portal_ctx(req)
    try:
        qs = _parse_query(q, required=True)
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            _refresh_indexes(ctx)
            i = ctx.indexes.get((index, len(qs)))
            if i is None:
                raise KeyError

        gen = index_generation(ctx.engine, index)
        restricted, auth_s = profile(restricted_keywords, ctx.portal, req) if ctx.portal else (None, 0)
        reader, query_s = profile(
            query.fetch,
            ctx.config,
            ctx.engine,
            i,
            qs,
            restricted=restricted,
        )

        if not limit and reader.bytes_total > ctx.config.response_limit_max:
            raise fastapi.HTTPException(status_code=413)

        if limit is not None:
            reader.set_limit(limit)

        return _finalize(_fetch_records(ctx, reader, index, qs, fmt,
                                        generation=gen, query_s=auth_s + query_s))
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/schema', response_class=fastapi.responses.PlainTextResponse)
async def api_schema(req: fastapi.Request):
    ctx = get_portal_ctx(req)
    if ctx.gql_schema is None:
        raise fastapi.HTTPException(status_code=503, detail='GraphQL Schema not built')
    return graphql.utilities.print_schema(ctx.gql_schema)


@router.post('/query', response_class=fastapi.responses.ORJSONResponse)
async def api_query_gql(req: fastapi.Request):
    ctx = get_portal_ctx(req)
    body = await req.body()

    if ctx.gql_schema is None:
        raise fastapi.HTTPException(status_code=503, detail='GraphQL Schema not built')

    try:
        gql_query = body.decode(encoding='utf-8')
        co = asyncio.wait_for(
            graphql.graphql(ctx.gql_schema, gql_query),
            timeout=ctx.config.script_timeout,
        )
        result, query_s = await profile_async(co)

        if result.errors:
            raise fastapi.HTTPException(
                status_code=400,
                detail=[str(e) for e in result.errors],
            )

        return _finalize({
            'profile': {'query': query_s},
            'q': body,
            'count': {k: len(v) for k, v in result.data.items()},
            'data': result.data,
        })
    except asyncio.TimeoutError:
        raise fastapi.HTTPException(
            status_code=408,
            detail=f'Query execution timed out after {ctx.config.script_timeout} seconds',
        )
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.head('/query/{index}')
async def api_test_index(index: str, q: str, req: fastapi.Request):
    ctx = get_portal_ctx(req)
    try:
        qs = _parse_query(q, required=True)
        i = ctx.indexes.get((index, len(qs)))
        if i is None:
            raise KeyError
        reader, query_s = profile(query.fetch, ctx.engine, ctx.config.s3_bucket, i, qs)
        return fastapi.Response(headers={'Content-Length': str(reader.bytes_total)})
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/cont', response_class=fastapi.responses.ORJSONResponse)
async def api_cont(token: str, req: fastapi.Request):
    ctx = get_portal_ctx(req)

    try:
        state = signed_tokens.decode(token, signed_tokens.signing_key())
    except signed_tokens.TokenError as e:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid continuation token: {e}')

    if state.portal_name and state.portal_name != ctx.name:
        raise fastapi.HTTPException(
            status_code=400,
            detail="continuation token is for a different portal",
        )

    current_gen = index_generation(ctx.engine, state.index_name)
    if state.generation != current_gen:
        raise fastapi.HTTPException(
            status_code=409,
            detail="continuation is stale (index was rebuilt); re-run the query",
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
            return _finalize(_fetch_records(ctx, reader, state.index_name, state.qs, state.fmt,
                                            generation=current_gen, page=state.page, query_s=query_s))

        elif state.type == 'match':
            return _finalize(_match_keys(ctx, i, state.qs, state.limit,
                                         after=state.last_key, generation=current_gen,
                                         page=state.page))

        else:
            raise fastapi.HTTPException(status_code=400, detail=f'Unknown continuation type: {state.type}')

    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


def _parse_query(q, required=False):
    if required and q is None:
        raise ValueError('Missing query parameter')
    return q.split(',') if q else []


def _match_keys(ctx, i, qs, limit, *, after=None, page=1, generation):
    match_limit = ctx.config.match_limit
    page_size = match_limit if limit is None else min(match_limit, limit)
    fetched, query_s = profile(query.match, ctx.config, ctx.engine, i, qs, after, page_size)

    remaining = None if limit is None else limit - len(fetched)

    token = None
    if len(fetched) >= page_size and (remaining is None or remaining > 0):
        state = continuation.ContState(
            type='match',
            index_name=i.name,
            index_arity=len(qs),
            qs=qs,
            portal_name=ctx.name,
            limit=remaining,
            last_key=fetched[-1] if fetched else None,
            page=page + 1,
            generation=generation,
        )
        try:
            token = signed_tokens.encode(state, signed_tokens.signing_key())
        except signed_tokens.TokenError as e:
            raise fastapi.HTTPException(status_code=413, detail=str(e))

    return {
        'profile': {'fetch': query_s, 'query': query_s},
        'index': i.name,
        'qs': qs,
        'limit': limit,
        'page': page,
        'count': len(fetched),
        'data': list(fetched),
        'continuation': token,
    }


def _fetch_records(ctx, reader, index, qs, fmt, *, page=1, generation, query_s=None):
    response_limit = ctx.config.response_limit
    response_limit_max = ctx.config.response_limit_max
    bytes_limit = reader.bytes_read + response_limit
    restricted_count = reader.restricted_count

    def take():
        for r in reader.records:
            yield r
            if reader.bytes_read > bytes_limit:
                break

    if fmt not in ['r', 'row', 'c', 'col', 'column']:
        raise ValueError('Invalid output format')

    fetched_records, fetch_s = profile(list, take())
    count = len(fetched_records)

    if reader.bytes_read > response_limit_max:
        raise fastapi.HTTPException(status_code=413)

    if fmt[0] == 'c':
        fetched_records = {
            k: [r.get(k) for r in fetched_records]
            for k in fetched_records[0].keys()
        }

    token = None
    if not reader.at_end:
        state = continuation.ContState(
            type='fetch',
            index_name=index,
            index_arity=int(reader.index.schema.arity),
            qs=qs or [],
            portal_name=ctx.name,
            fmt=fmt,
            page=page + 1,
            source_index=reader._source_index,
            byte_offset=reader._source_byte_offset,
            limit=(reader.limit - count) if reader.limit is not None else None,
            generation=generation,
        )
        try:
            token = signed_tokens.encode(state, signed_tokens.signing_key())
        except signed_tokens.TokenError as e:
            raise fastapi.HTTPException(status_code=413, detail=str(e))

    return {
        'profile': {'fetch': fetch_s, 'query': query_s},
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
    }
