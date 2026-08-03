import mimetypes

import fastapi

from ..lib import s3
from ..lib.auth import verify_permissions
from ..middleware.portal import get_portal_ctx


# create web server
router = fastapi.APIRouter()


def _if_none_match(header, etag):
    """
    True if the request already holds this version. The header is a list,
    may be the wildcard, and may mark its validators weak - which is fine
    here, as S3 gives us one opaque tag either way.
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


def _raw_headers(etag):
    """
    Revalidate on every request, but let the answer be a 304. no-cache is
    not "do not store" - it means "ask first", which is what makes the
    ETag worth sending.
    """
    return {'ETag': etag, 'Cache-Control': 'public, no-cache, must-revalidate'}


def _raw_plot_response(bucket, path):
    """
    Serve a plot image, or 404 if it isn't there.

    A referenced plot that was never generated is an ordinary miss, not a
    failure: the database names the file whether or not the job that draws
    it ever ran.
    """
    read = s3.read_object_with_etag(bucket, path)
    if read is None:
        raise fastapi.HTTPException(status_code=404)

    body, _etag = read

    return fastapi.Response(content=body, media_type='image/png')


def _raw_file_response(bucket, path, file, header):
    """
    Serve an S3 object with ETag revalidation, as a 200 or a 304.
    """
    meta = s3.head_object(bucket, path)
    if meta is None:
        raise fastapi.HTTPException(status_code=404)

    if _if_none_match(header, meta['ETag']):
        return fastapi.Response(status_code=304, headers=_raw_headers(meta['ETag']))

    # read the body and answer with the tag it actually came back at, which
    # is not necessarily the one HEAD reported a moment ago - and the object
    # may be gone entirely by now, which is a 404 rather than a failure
    read = s3.read_object_with_etag(bucket, path)
    if read is None:
        raise fastapi.HTTPException(status_code=404)

    body, etag = read

    content_type, encoding = mimetypes.guess_type(file)
    headers = _raw_headers(etag)
    if encoding is not None:
        headers['Content-Encoding'] = encoding

    return fastapi.Response(
        content=body,
        media_type=content_type or 'application/octet-stream',
        headers=headers,
    )


@router.get('/plot/dataset/{dataset}/{file:path}')
async def api_raw_plot_dataset(dataset: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for a dataset.
    """
    ctx = get_portal_ctx(req)
    if not verify_permissions(ctx.portal, req, dataset=dataset):
        raise fastapi.HTTPException(status_code=401)

    return _raw_plot_response(ctx.config.s3_bucket, ctx.config.s3_path(f'plot/dataset/{dataset}/{file}'))


@router.get('/plot/phenotype/{phenotype}/{file:path}')
async def api_raw_plot_phenotype(phenotype: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    ctx = get_portal_ctx(req)
    if not verify_permissions(ctx.portal, req, phenotype=phenotype):
        raise fastapi.HTTPException(status_code=401)

    return _raw_plot_response(ctx.config.s3_bucket, ctx.config.s3_path(f'plot/phenotype/{phenotype}/{file}'))


@router.get('/plot/phenotype/{phenotype}/{ancestry}/{file:path}')
async def api_raw_plot_phenotype_ancestry(phenotype: str, ancestry: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    ctx = get_portal_ctx(req)
    if not verify_permissions(ctx.portal, req, phenotype=phenotype):
        raise fastapi.HTTPException(status_code=401)

    return _raw_plot_response(
        ctx.config.s3_bucket,
        ctx.config.s3_path(f'plot/phenotype/{phenotype}/{ancestry}/{file}'),
    )


@router.get('/file/{file:path}')
async def api_raw_file(file: str, req: fastapi.Request):
    """
    Returns a raw file, revalidated against its S3 ETag.
    """
    ctx = get_portal_ctx(req)

    return _raw_file_response(
        ctx.config.s3_bucket,
        ctx.config.s3_path(f'raw/{file}'),
        file,
        req.headers.get('if-none-match'),
    )
