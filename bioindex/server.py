import contextlib
import os

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
        raise RuntimeError("BIOINDEX_ENV must be set (qa, prod, etc.)")
    contexts = build_portal_contexts(config_dir, env=env)
    init_registry(contexts)


@contextlib.asynccontextmanager
async def lifespan(app):
    _init_registry_from_env()
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
