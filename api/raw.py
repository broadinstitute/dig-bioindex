import fastapi

import lib.config
import lib.s3

from lib.auth import verify_permissions


# load dot files and configuration
config = lib.config.Config()

# create web server
router = fastapi.APIRouter()

# connect to database
engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.portal_schema)


@router.get('/plot/dataset/{dataset}/{file:path}')
async def api_raw_plot_dataset(dataset: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for a dataset.
    """
    if not verify_permissions(engine, req, dataset=dataset):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = lib.s3.read_object(config.s3_bucket, f'plot/dataset/{dataset}/{file}')
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')


@router.get('/plot/phenotype/{phenotype}/{file:path}')
async def api_raw_plot_phenotype(phenotype: str, file: str, req: fastapi.Request):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    if not verify_permissions(engine, req, phenotype=phenotype):
        raise fastapi.HTTPException(status_code=401)

    # load the object from s3
    content = lib.s3.read_object(config.s3_bucket, f'plot/phenotype/{phenotype}/{file}')
    if content is None:
        raise fastapi.HTTPException(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')
