import fastapi

import api.bio
import api.portal
import api.raw

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# create web server
app = fastapi.FastAPI(title='BioIndex', redoc_url=None)

# all the various routers for each api
app.include_router(api.bio.router, prefix='/api/bio', tags=['bio'])
app.include_router(api.portal.router, prefix='/api/portal', tags=['portal'])
app.include_router(api.raw.router, prefix='/api/raw', tags=['raw'])

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
