import asyncio
import concurrent.futures
import itertools
import os
import re
from enum import Enum
from typing import List, Optional

import fastapi
import graphql
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

from .utils import *
from ..lib import aws, config as lib_config, continuation, index, ql, query, s3
from ..lib.auth import restricted_keywords
from ..lib.utils import nonce, profile, profile_async
from ..lib.config import Config
from ..lib.index import Index

router = APIRouter()
load_config = Config()

# MCP pigean tool endpoint using dynamic schema (no Pydantic model)
@router.post('/pigean_query', response_class=ORJSONResponse, tags=["pigean"])
async def pigean_query(
    body: dict,
):
    """
    Query any pigean_* index with the same logic as the REST endpoints. Uses schema from environment/config.
    Expects JSON body with at least: {"index": "pigean_*", "q": <query>, "fmt": <format>, "limit": <int>}
    """
    index_name = body.get("index")
    if not index_name or not index_name.startswith("pigean_"):
        raise fastapi.HTTPException(status_code=400, detail="Index must start with 'pigean_'")
    q = body.get("q")
    fmt = body.get("fmt", "row")
    limit = body.get("limit")

    global INDEXES
    try:
        qs = _parse_query(q, required=False)
        if (index_name, len(qs)) not in INDEXES:
            INDEXES = _load_indexes()
        i = INDEXES[(index_name, len(qs))]
        reader, query_s = profile(
            query.fetch,
            load_config,
            connect_to_bio(load_config),
            i,
            qs,
        )
        if not limit and reader.bytes_total > RESPONSE_LIMIT_MAX:
            raise fastapi.HTTPException(status_code=413)
        if limit is not None:
            reader.set_limit(limit)
        # fetch records
        fetched_records, fetch_s = profile(list, reader.records)
        count = len(fetched_records)
        # build response using the same structure as other endpoints
        return {
            "index": index_name,
            "q": q,
            "count": count,
            "data": fetched_records,
            "page": 1,
            "limit": limit,
            "continuation": None,
            "nonce": nonce(),
        }
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid pigean index: {index_name}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))

# max number of bytes to read from s3 per request
RESPONSE_LIMIT = load_config.response_limit
RESPONSE_LIMIT_MAX = load_config.response_limit_max
MATCH_LIMIT = load_config.match_limit

# multi-query executor
executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

# by default, there is no graphql schema
gql_schema = None

# if the graphql schema file exists, load it
if load_config.graphql_schema:
    gql_schema = ql.load_schema(load_config, connect_to_bio(load_config), load_config.graphql_schema)


class Query(BaseModel):
    q: List[str]
    fmt: Optional[str] = 'row'
    limit: Optional[int] = None


class Index(BaseModel):
    name: str


class QueryResult(BaseModel):
    index: str
    q: str
    data: List[dict]


class CountResult(BaseModel):
    index: str
    q: str
    count: int


class Schema(BaseModel):
    index: str
    schema: dict


class IndexesResponse(BaseModel):
    indexes: List[str]


@router.get('/indexes', response_class=ORJSONResponse)
async def api_list_indexes(config: Config = Depends(load_config)):
    """
    Return all available indexes using the schema defined in BIOINDEX_BIO_SCHEMA.
    """
    schema = os.getenv("BIOINDEX_BIO_SCHEMA", "bio")  # Default to "bio" if not set
    indexes = list(Index.list_indexes(config.engine, filter_built=False))
    # Optionally, filter or format the output as needed
    return {
        "count": len(indexes),
        "data": [i.name for i in indexes],
        "nonce": "some_nonce_value"  # Replace with actual nonce logic
    }


@router.get('/match/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_match(index: str, req: fastapi.Request, q: str, limit: int = None):
    """
    Return all the unique keys for a value-indexed table.
    """
    try:
        qs = _parse_query(q)
        i = INDEXES[(index, len(qs))]

        # execute the query
        keys, query_s = profile(query.match, load_config, connect_to_bio(load_config), i, qs)

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
async def api_count_index(index: str, req: fastapi.Request, q: str = None):
    """
    Query the database and estimate how many records will be returned.
    """
    try:
        qs = _parse_query(q)
        i = INDEXES[(index, len(qs))]

        # lookup the schema for this index and perform the query
        count, query_s = profile(query.count, load_config, connect_to_bio(load_config), i, qs)

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

        keys, query_s = profile(query.fetch_keys, connect_to_bio(load_config), i, columns)

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
    try:
        idxs = [idx for key, idx in INDEXES.items() if key[0] == index]

        if len(idxs) == 0:
            raise KeyError
        elif len(idxs) == 1:
            # discover what the user doesn't have access to see
            restricted, auth_s = profile(restricted_keywords, connect_to_portal(load_config), req) if connect_to_portal(load_config) else (None, 0)

            # lookup the schema for this index and perform the query
            reader, query_s = profile(
                query.fetch_all,
                load_config,
                idxs[0],
                restricted=restricted,
            )

            # will this request exceed the limit?
            if reader.bytes_total > RESPONSE_LIMIT_MAX:
                raise fastapi.HTTPException(status_code=413)

            # fetch records from the reader
            return _fetch_records(reader, index, None, fmt, query_s=auth_s + query_s)
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
    try:
        i = INDEXES[(index, arity)]

        # discover what the user doesn't have access to see
        restricted, auth_s = profile(restricted_keywords, connect_to_portal(load_config), req) if connect_to_portal(load_config) else (None, 0)

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch_all,
            load_config,
            i,
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
    number of bytes what would be read. If multiple indexes share a name
    with different arity it'll throw a 400.
    """
    try:
        idxs = [idx for key, idx in INDEXES.items() if key[0] == index]

        if len(idxs) == 0:
            raise KeyError
        elif len(idxs) == 1:
            # lookup the schema for this index and perform the query
            reader, query_s = profile(
                query.fetch_all,
                load_config,
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
    try:
        i = INDEXES[(index, arity)]

        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch_all,
            load_config,
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
async def api_lookup_variant_for_rs_id(rsid: str):
    """
    Lookup the variant ID for a given rsID.
    """
    dynamodb_table = load_config.variant_dynamodb_table
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
        restricted, auth_s = profile(restricted_keywords, connect_to_portal(load_config), req) if connect_to_portal(load_config) else (None, 0)
        # lookup the schema for this index and perform the query
        reader, query_s = profile(
            query.fetch,
            load_config,
            connect_to_bio(load_config),
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
            timeout=load_config.script_timeout,
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
                                    detail=f'Query execution timed out after {load_config.script_timeout} seconds')
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
        reader, query_s = profile(query.fetch, connect_to_bio(load_config), load_config.s3_bucket, i, qs)

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
