import logging

import fastapi
from sqlalchemy import text

from ..lib.portal_registry import get_registry

# create web server
router = fastapi.APIRouter()


@router.get('/health', response_class=fastapi.responses.ORJSONResponse)
async def api_health():
    """
    Liveness. The process answering at all is the whole check.
    """
    return {'status': 'ok'}


@router.get('/ready', response_class=fastapi.responses.ORJSONResponse)
async def api_ready(response: fastapi.Response):
    """
    Readiness. Query every portal's index schema, and report per portal.
    A single unreachable portal leaves the task in rotation - the others
    are still servable - so this only fails when they all do.
    """
    portals = {}

    for ctx in get_registry():
        try:
            with ctx.engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            portals[ctx.name] = 'ok'
        except Exception as e:
            logging.warning('ready check failed for portal %s: %s', ctx.name, e)
            portals[ctx.name] = f'error: {type(e).__name__}'

    if portals and all(v != 'ok' for v in portals.values()):
        response.status_code = 503
        return {'status': 'unhealthy', 'portals': portals}

    return {'status': 'ok', 'portals': portals}
