import mimetypes

import fastapi

from ..lib import s3
from ..lib.auth import verify_permissions
from ..middleware.portal import get_portal_ctx


# create web server
router = fastapi.APIRouter()


@router.get('/plot/dataset/{dataset}/{file:path}')
async def api_raw_plot_dataset(dataset: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for a dataset.
    """
    ctx = get_portal_ctx(req)
    if not verify_permissions(ctx.portal, req, dataset=dataset):
        raise fastapi.HTTPException(status_code=401)

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

    content = s3.read_object(ctx.config.s3_bucket, ctx.config.s3_path(f'plot/phenotype/{phenotype}/{ancestry}/{file}'))
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/file/{file:path}')
async def api_raw_file(file: str, req: fastapi.Request):
    ctx = get_portal_ctx(req)
    content = s3.read_object(ctx.config.s3_bucket, ctx.config.s3_path(f'raw/{file}'))
    if content is None:
        raise fastapi.HTTPException(status_code=404)
    content_type, encoding = mimetypes.guess_type(file)
    if content_type is None:
        content_type = 'application/octet-stream'
    headers = {}
    if encoding is not None:
        headers['Content-Encoding'] = encoding

    return fastapi.Response(content=content.read(), media_type=content_type, headers=headers)
