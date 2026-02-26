import dotenv
import os
import pathlib

env_loc = os.path.join(pathlib.Path(__file__).parent.parent, '.bioindex')
dotenv.load_dotenv(env_loc)

import fastapi
import pymysql

from .api import bio
from .api import portal
from .api import raw

from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

pymysql.install_as_MySQLdb()
# create web server
app = fastapi.FastAPI(title='BioIndex', redoc_url=None)

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

# enable compression of large response bodies
app.add_middleware(GZipMiddleware)

# serve static content
app.mount('/static', StaticFiles(directory="web/static"), name="static")


@app.get('/')
def index():
    """
    SPA demonstration page.
    """
    return FileResponse('web/index.html')
