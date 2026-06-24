import contextlib
import os
import sys

import fastapi
import pymysql

from .api import bio
from .api import portal
from .api import raw
from .lib.portal_loader import build_portal_contexts
from .lib.portal_registry import init_registry
from .middleware.portal import PortalResolveMiddleware

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


pymysql.install_as_MySQLdb()


def _init_registry_from_env():
    config_dir = os.environ.get("BIOINDEX_CONFIG_DIR", "/etc/bioindex")
    env = os.environ.get("BIOINDEX_ENV")
    if not env:
        raise RuntimeError(
            "BIOINDEX_ENV is not set. Set it to the environment name (e.g. qa, prod, dev) "
            "matching a file at <BIOINDEX_CONFIG_DIR>/envs/<env>.yaml."
        )
    if not os.path.isdir(config_dir):
        raise RuntimeError(
            f"BIOINDEX_CONFIG_DIR '{config_dir}' is not a directory. Point it at a config dir "
            f"containing portals/*.yaml and envs/{env}.yaml (default /etc/bioindex)."
        )
    contexts = build_portal_contexts(config_dir, env=env)
    if not contexts:
        raise RuntimeError(
            f"No portals loaded from '{config_dir}' for env '{env}'. Expected at least one "
            f"portals/*.yaml with an 'envs.{env}' block."
        )
    init_registry(contexts)
    print(f"BioIndex: loaded {len(contexts)} portal(s): "
          f"{', '.join(c.name for c in contexts)}", file=sys.stderr, flush=True)


@contextlib.asynccontextmanager
async def lifespan(app):
    try:
        _init_registry_from_env()
    except Exception as e:
        print(f"BioIndex startup failed: {e}", file=sys.stderr, flush=True)
        raise
    yield
    from .lib.portal_registry import get_registry
    try:
        registry = get_registry()
    except RuntimeError:
        return
    for name in registry.names():
        ctx = registry.get(name)
        if ctx is None:
            continue
        try:
            ctx.engine.dispose()
            if ctx.portal:
                ctx.portal.dispose()
        except Exception:
            pass


app = fastapi.FastAPI(title='BioIndex', redoc_url=None, lifespan=lifespan)

app.add_middleware(PortalResolveMiddleware, reserved_prefixes=("static", "docs", "openapi.json"))

app.include_router(bio.router,    prefix='/api/bio',    tags=['bio'])
app.include_router(portal.router, prefix='/api/portal', tags=['portal'])
app.include_router(raw.router,    prefix='/api/raw',    tags=['raw'])

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.mount('/static', StaticFiles(directory="web/static"), name="static")


@app.get('/')
def index():
    return FileResponse('web/index.html')
