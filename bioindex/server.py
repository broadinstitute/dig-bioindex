import contextlib
import logging
import os

import fastapi
import pymysql

from .api import bio
from .api import portal
from .api import raw
from .lib.portal_loader import build_portal_contexts
from .lib.portal_registry import get_registry, init_registry
from .middleware.portal import PortalResolveMiddleware

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

pymysql.install_as_MySQLdb()


def _init_registry_from_env():
    """
    Load every portal defined for BIOINDEX_ENV out of BIOINDEX_CONFIG_DIR.
    """
    config_dir = os.environ.get('BIOINDEX_CONFIG_DIR', '/etc/bioindex')
    env = os.environ.get('BIOINDEX_ENV')

    if not env:
        raise RuntimeError('BIOINDEX_ENV is not set; it names the file at '
                           '<BIOINDEX_CONFIG_DIR>/envs/<env>.yaml (e.g. qa, prod)')
    if not os.path.isdir(config_dir):
        raise RuntimeError(f"BIOINDEX_CONFIG_DIR '{config_dir}' is not a directory; it should "
                           f'contain portals/*.yaml and envs/{env}.yaml')

    contexts = build_portal_contexts(config_dir, env)
    if not contexts:
        raise RuntimeError(f"No portals loaded from '{config_dir}' for env '{env}'; expected at "
                           f"least one portals/*.yaml with an 'envs.{env}' block")

    init_registry(contexts)
    logging.info('Loaded %d portal(s): %s', len(contexts), ', '.join(c.name for c in contexts))


@contextlib.asynccontextmanager
async def lifespan(app):
    """
    Build the portal registry before serving, and drop the connection
    pools on the way out.
    """
    _init_registry_from_env()
    yield

    for ctx in get_registry():
        ctx.engine.dispose()
        if ctx.portal:
            ctx.portal.dispose()


# create web server
app = fastapi.FastAPI(title='BioIndex', redoc_url=None, lifespan=lifespan)

# the leading path segment selects the portal for every api route
app.add_middleware(PortalResolveMiddleware, reserved_prefixes=('static', 'docs', 'openapi.json'))

# all the various routers for each api
app.include_router(bio.router, prefix='/api/bio', tags=['bio'])
app.include_router(portal.router, prefix='/api/portal', tags=['portal'])
app.include_router(raw.router, prefix='/api/raw', tags=['raw'])

# enable cross-origin resource sharing
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
# serve static content
app.mount('/static', StaticFiles(directory="web/static"), name="static")


@app.get('/')
def index():
    """
    SPA demonstration page.
    """
    return FileResponse('web/index.html')
