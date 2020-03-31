import dotenv
import fastapi

import api.bio
import api.portal

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# load dot files and configuration
dotenv.load_dotenv()

# create flask app; this will load .env
app = fastapi.FastAPI(title='BioIndex', docs_url=None)

app.include_router(api.bio.router, tags=['bio'])
app.include_router(api.portal.router, tags=['portal'])

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
    SPA page.
    """
    return FileResponse('web/index.html')
