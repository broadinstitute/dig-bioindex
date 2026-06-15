import mimetypes
import os

import fastapi

from .utils import *

from ..lib import s3
from ..lib.auth import verify_permissions
from ..lib.response_cache import ResponseCache
from ..middleware.portal import get_portal_ctx


# create web server
router = fastapi.APIRouter()


def _etag_matches(if_none_match, etag):
    """
    True if the request's If-None-Match header satisfies a 304 for `etag`.
    Handles '*', weak validators (W/"..."), and comma-separated lists.
    """
    if not if_none_match:
        return False
    for candidate in (c.strip() for c in if_none_match.split(',')):
        if candidate == '*':
            return True
        if candidate.startswith('W/'):
            candidate = candidate[2:]
        if candidate == etag:
            return True
    return False


# Per-worker LRU of raw-file bodies, keyed by bucket|path|etag so a changed
# S3 object (new ETag) is a new key — an automatic miss, never stale.
# Default 100 MiB; set BIOINDEX_RAW_CACHE_BYTES=0 to disable the body cache
# (revalidation/304 still work). Mirrors BIOINDEX_RESP_CACHE_BYTES in bio.py.
_RAW_CACHE = ResponseCache(
    max_bytes=int(os.environ.get("BIOINDEX_RAW_CACHE_BYTES", 100 * 1024 * 1024))
)


def _raw_cache_headers(etag):
    return {"ETag": etag, "Cache-Control": "public, no-cache, must-revalidate"}


def _raw_200(file, body, etag, x_cache):
    content_type, encoding = mimetypes.guess_type(file)
    if content_type is None:
        content_type = 'application/octet-stream'
    headers = _raw_cache_headers(etag)
    headers["X-Cache"] = x_cache
    if encoding is not None:
        headers["Content-Encoding"] = encoding
    return fastapi.Response(content=body, media_type=content_type, headers=headers)


def _raw_response(bucket, path, file, if_none_match):
    """
    Serve S3 object `path` with ETag revalidation + LRU body cache.
    Returns a 200 or 304 fastapi.Response; raises HTTPException(404) if absent.
    """
    meta = s3.head_object(bucket, path)
    if meta is None:
        raise fastapi.HTTPException(status_code=404)
    etag = meta["ETag"]

    if _etag_matches(if_none_match, etag):
        return fastapi.Response(status_code=304, headers=_raw_cache_headers(etag))

    cached = _RAW_CACHE.get(f"{bucket}|{path}|{etag}")
    if cached is not None:
        return _raw_200(file, cached, etag, "HIT")

    body, etag2 = s3.read_object_with_etag(bucket, path)
    _RAW_CACHE.set(f"{bucket}|{path}|{etag2}", body, size=len(body))
    return _raw_200(file, body, etag2, "MISS")


@router.get('/plot/dataset/{dataset}/{file:path}')
async def api_raw_plot_dataset(dataset: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for a dataset.
    """
    ctx = get_portal_ctx(req)
    if not verify_permissions(ctx.portal, req, dataset=dataset):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = s3.read_object(ctx.config.s3_bucket, ctx.config.s3_path(f'plot/dataset/{dataset}/{file}'))
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/plot/phenotype/{phenotype}/{file:path}')
async def api_raw_plot_phenotype(phenotype: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    ctx = get_portal_ctx(req)
    if not verify_permissions(ctx.portal, req, phenotype=phenotype):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = s3.read_object(ctx.config.s3_bucket, ctx.config.s3_path(f'plot/phenotype/{phenotype}/{file}'))
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/plot/phenotype/{phenotype}/{ancestry}/{file:path}')
async def api_raw_plot_phenotype_ancestry(phenotype: str, ancestry: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    ctx = get_portal_ctx(req)
    if not verify_permissions(ctx.portal, req, phenotype=phenotype):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = s3.read_object(ctx.config.s3_bucket, ctx.config.s3_path(f'plot/phenotype/{phenotype}/{ancestry}/{file}'))
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/file/{file:path}')
async def api_raw_file(file: str, req: fastapi.Request):
    ctx = get_portal_ctx(req)
    path = ctx.config.s3_path(f'raw/{file}')
    return _raw_response(
        ctx.config.s3_bucket, path, file, req.headers.get('if-none-match')
    )
