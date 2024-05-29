import mimetypes

import fastapi

from .utils import *

from ..lib import config
from ..lib import s3
from ..lib.auth import verify_permissions


# load dot files and configuration
CONFIG = config.Config()

# create web server
router = fastapi.APIRouter()

# optionally connect to the portal/metadata schema
portal = connect_to_portal(CONFIG)

# if there is no portal schema defined, then patch the router
if not portal:
    monkey_patch_router(router)


@router.get('/plot/dataset/{dataset}/{file:path}')
async def api_raw_plot_dataset(dataset: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for a dataset.
    """
    if not verify_permissions(portal, req, dataset=dataset):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = s3.read_object(CONFIG.s3_bucket, f'plot/dataset/{dataset}/{file}')
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/plot/phenotype/{phenotype}/{file:path}')
async def api_raw_plot_phenotype(phenotype: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    if not verify_permissions(portal, req, phenotype=phenotype):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = s3.read_object(CONFIG.s3_bucket, f'plot/phenotype/{phenotype}/{file}')
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/plot/phenotype/{phenotype}/{ancestry}/{file:path}')
async def api_raw_plot_phenotype_ancestry(phenotype: str, ancestry: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    if not verify_permissions(portal, req, phenotype=phenotype):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = s3.read_object(CONFIG.s3_bucket, f'plot/phenotype/{phenotype}/{ancestry}/{file}')
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/file/{file:path}')
async def api_raw_file(file: str, req: fastapi.Request):
    content = s3.read_object(CONFIG.s3_bucket, f'raw/{file}')
    if content is None:
        raise fastapi.HTTPException(status_code=404)
    content_type, encoding = mimetypes.guess_type(file)
    if content_type is None:
        content_type = 'application/octet-stream'
    headers = {}
    if encoding is not None:
        headers['Content-Encoding'] = encoding

    return fastapi.Response(content=content.read(), media_type=content_type, headers=headers)
