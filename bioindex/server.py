import contextlib
import logging
import os

import anyio.to_thread
import fastapi
import pymysql

from .api import bio
from .api import health
from .api import portal
from .api import raw
from .lib import config
from .lib.portal_loader import build_portal_context, build_portal_contexts
from .lib.portal_registry import get_registry, init_registry
from .middleware.portal import PortalResolveMiddleware

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

pymysql.install_as_MySQLdb()


def _init_registry_from_env():
    """
    With BIOINDEX_CONFIG_DIR set, serve every portal defined for BIOINDEX_ENV
    out of that directory. Without it, serve a single portal configured from
    the environment - the local `--env-file` workflow - named by
    BIOINDEX_PORTAL_NAME.
    """
    config_dir = os.environ.get('BIOINDEX_CONFIG_DIR')

    if not config_dir:
        name = os.environ.get('BIOINDEX_PORTAL_NAME', 'local')
        contexts = [build_portal_context(config.Config(), name)]
    else:
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


def _configure_thread_pool():
    """
    Size the pool that runs the sync handlers. Every S3 read and database
    round-trip occupies one of its threads for the whole call, so the ceiling
    is really how many slow requests a worker can have in flight. anyio
    defaults to 40; BIOINDEX_THREAD_POOL moves it per environment.
    """
    threads = int(os.environ.get('BIOINDEX_THREAD_POOL', '40'))
    anyio.to_thread.current_default_thread_limiter().total_tokens = threads
    logging.info('Thread pool sized to %d', threads)


@contextlib.asynccontextmanager
async def lifespan(app):
    """
    Build the portal registry before serving, and drop the connection
    pools on the way out.
    """
    _init_registry_from_env()
    _configure_thread_pool()
    yield

    for ctx in get_registry():
        ctx.engine.dispose()
        if ctx.portal:
            ctx.portal.dispose()


# create web server
app = fastapi.FastAPI(title='BioIndex', redoc_url=None, lifespan=lifespan)

# paths served by the process itself rather than by one of its portals
RESERVED = ('health', 'ready', 'static', 'docs', 'openapi.json')

# the leading path segment selects the portal for every api route
app.add_middleware(PortalResolveMiddleware, reserved_prefixes=RESERVED)

# all the various routers for each api
app.include_router(bio.router, prefix='/api/bio', tags=['bio'])
app.include_router(portal.router, prefix='/api/portal', tags=['portal'])
app.include_router(raw.router, prefix='/api/raw', tags=['raw'])

# load balancer probes; not portal-scoped
app.include_router(health.router, tags=['health'])

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
