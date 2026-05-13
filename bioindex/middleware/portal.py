from typing import Iterable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from ..lib.portal_registry import get_registry


class PortalResolveMiddleware(BaseHTTPMiddleware):
    """
    Read the first path segment as the portal name, look up the matching
    PortalContext from the registry, attach it to request.state.portal_ctx,
    then rewrite the request path to strip the prefix so existing routers
    work unchanged. Reserved prefixes (e.g. health, ready, metrics) bypass
    resolution.
    """
    def __init__(self, app: ASGIApp, reserved_prefixes: Iterable[str] = ()):
        super().__init__(app)
        self._reserved = set(reserved_prefixes)

    async def dispatch(self, request, call_next):
        path = request.url.path
        # Strip leading slash; first segment is the portal (or reserved)
        segments = path.lstrip("/").split("/", 1)
        head = segments[0]

        if head in self._reserved or head == "":
            return await call_next(request)

        registry = get_registry()
        ctx = registry.get(head)
        if ctx is None:
            return JSONResponse(
                {
                    "detail": f"Unknown portal '{head}'",
                    "valid_portals": registry.names(),
                },
                status_code=404,
            )

        # Stash on request.state for downstream handlers
        request.state.portal_ctx = ctx
        ctx.touch()

        # Rewrite scope path so existing routers don't need to know about prefixes.
        # rewrite scope["path"] from the already-decoded path
        remainder = "/" + segments[1] if len(segments) > 1 else "/"
        request.scope["path"] = remainder
        # rewrite scope["raw_path"] by stripping the portal prefix bytes from the
        # ORIGINAL raw_path, preserving any percent-encoding present in the sub-path
        raw = request.scope.get("raw_path") or path.encode("utf-8")
        prefix_bytes = ("/" + head).encode("utf-8")
        if raw.startswith(prefix_bytes):
            request.scope["raw_path"] = raw[len(prefix_bytes):] or b"/"
        else:
            # safety fallback if raw_path doesn't have the expected prefix
            request.scope["raw_path"] = remainder.encode("utf-8")

        return await call_next(request)


def get_portal_ctx(request: Request):
    """
    Return the PortalContext attached to a request by PortalResolveMiddleware.
    Raises RuntimeError if the middleware did not run (e.g., the request was
    to a reserved/non-portal path).
    """
    ctx = getattr(request.state, "portal_ctx", None)
    if ctx is None:
        raise RuntimeError(
            "request.state.portal_ctx not set — PortalResolveMiddleware "
            "did not match this request (reserved path or middleware not installed)"
        )
    return ctx
