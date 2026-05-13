import logging
from typing import Dict

import fastapi
import sqlalchemy

from ..lib.portal_registry import get_registry


router = fastapi.APIRouter()


@router.get("/health")
def health():
    """Liveness check. Process up means OK."""
    return {"status": "ok"}


@router.get("/ready")
def ready(response: fastapi.Response):
    """
    Readiness check. Iterates the registry and runs SELECT 1 against each
    portal's bio engine. The task is unhealthy only if ALL portals fail —
    one bad portal does not pull the task out of rotation.
    """
    registry = get_registry()
    portals: Dict[str, str] = {}
    all_failed = True

    for name in registry.names():
        ctx = registry.get(name)
        assert ctx is not None  # name came from registry.names()
        try:
            with ctx.engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
            portals[name] = "ok"
            all_failed = False
        except Exception as e:
            logging.warning("ready check failed for portal %s: %s", name, e)
            portals[name] = f"error: {type(e).__name__}"

    if all_failed and portals:
        response.status_code = 503
        return {"status": "unhealthy", "portals": portals}
    return {"status": "ok", "portals": portals}
