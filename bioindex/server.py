import logging
import os

import fastapi
import pymysql

from .api import bio
from .api import portal
from .api import raw
from .api import health
from .lib.portal_loader import build_portal_contexts
from .lib.portal_registry import init_registry
from .middleware.portal import PortalResolveMiddleware

from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


pymysql.install_as_MySQLdb()


def _init_registry_from_env():
    config_dir = os.environ.get("BIOINDEX_CONFIG_DIR", "/etc/bioindex")
    env = os.environ.get("BIOINDEX_ENV")
    if not env:
        raise RuntimeError("BIOINDEX_ENV must be set (qa, prod, etc.)")
    logging.info("Loading portal configs from %s for env=%s", config_dir, env)
    contexts = build_portal_contexts(config_dir, env=env)
    init_registry(contexts)
    logging.info("Registered %d portals: %s",
                 len(contexts), [c.name for c in contexts])


# Build registry at import time so multi-worker uvicorn forks have it ready.
_init_registry_from_env()

app = fastapi.FastAPI(title='BioIndex', redoc_url=None)

# Reserved (non-portal) path prefixes. More specific entries first.
RESERVED = ("health", "ready", "static", "_admin", "docs", "openapi.json")
app.add_middleware(PortalResolveMiddleware, reserved_prefixes=RESERVED)

# Portal-scoped routers (paths inside these were originally /api/...)
app.include_router(bio.router,    prefix='/api/bio',    tags=['bio'])
app.include_router(portal.router, prefix='/api/portal', tags=['portal'])
app.include_router(raw.router,    prefix='/api/raw',    tags=['raw'])

# Reserved routers at root (bypass portal middleware)
app.include_router(health.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.mount('/static', StaticFiles(directory="web/static"), name="static")
