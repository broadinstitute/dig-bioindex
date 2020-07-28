import fastapi

import lib.config
import lib.s3


# load dot files and configuration
config = lib.config.Config()

# create web server
router = fastapi.APIRouter()


@router.get('/plot/{image_path:path}')
async def api_raw_plot(image_path: str):
    """
    Returns a raw, image plot for the bottom-line analysis of a phenotype.
    """
    content = lib.s3.read_object(config.s3_bucket, f'plot/{image_path}')

    if content is None:
        return fastapi.Response(status_code=404)

    return fastapi.Response(content=content.read(), media_type='image/png')
