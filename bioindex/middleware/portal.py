from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from ..lib.portal_registry import get_registry


class PortalResolveMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, reserved_prefixes=()):
        super().__init__(app)
        self._reserved = set(reserved_prefixes)

    async def dispatch(self, request, call_next):
        head, _, rest = request.url.path.lstrip("/").partition("/")
        if head and head not in self._reserved:
            ctx = get_registry().get(head)
            if ctx is None:
                return JSONResponse(
                    {"detail": f"Unknown portal '{head}'", "valid_portals": get_registry().names()},
                    status_code=404)
            request.state.portal_ctx = ctx
            ctx.touch()
            remainder = "/" + rest
            request.scope["path"] = remainder
            raw = request.scope.get("raw_path") or request.url.path.encode()
            prefix = ("/" + head).encode()
            request.scope["raw_path"] = (raw[len(prefix):] or b"/") if raw.startswith(prefix) else remainder.encode()
        return await call_next(request)


def get_portal_ctx(request):
    ctx = getattr(request.state, "portal_ctx", None)
    if ctx is None:
        raise RuntimeError("portal_ctx not set — PortalResolveMiddleware missing or path was reserved")
    return ctx
