import fastapi
import pymysql

from .api import bio, enrichr
from .api import portal
from .api import raw

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

pymysql.install_as_MySQLdb()
# create web server
app = fastapi.FastAPI(title='BioIndex', redoc_url=None)

# all the various routers for each api
app.include_router(bio.router, prefix='/api/bio', tags=['bio'])
app.include_router(portal.router, prefix='/api/portal', tags=['portal'])
app.include_router(raw.router, prefix='/api/raw', tags=['raw'])

app.include_router(enrichr.router, prefix='/api/enrichr', tags=['enrichr'])

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
