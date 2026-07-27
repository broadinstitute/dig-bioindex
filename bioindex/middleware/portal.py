from urllib.parse import urlsplit, urlunsplit

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from ..lib.portal_registry import get_registry


class PortalResolveMiddleware(BaseHTTPMiddleware):
    """
    Resolve the leading path segment to a portal, stash the context on the
    request, and strip the segment so the routers match their paths as-is.
    """

    def __init__(self, app, reserved_prefixes=()):
        super().__init__(app)
        self._reserved = set(reserved_prefixes)

    async def dispatch(self, request, call_next):
        head, _, rest = request.url.path.lstrip('/').partition('/')

        if not head or head in self._reserved:
            return await call_next(request)

        ctx = get_registry().get(head)
        if ctx is None:
            return JSONResponse(
                {'detail': f"Unknown portal '{head}'", 'valid_portals': get_registry().names()},
                status_code=404,
            )

        request.state.portal_ctx = ctx

        # strip the portal prefix from the path the routers will see
        prefix = '/' + head
        request.scope['path'] = '/' + rest
        raw_path = request.scope.get('raw_path')
        if raw_path and raw_path.startswith(prefix.encode()):
            request.scope['raw_path'] = raw_path[len(prefix):] or b'/'

        response = await call_next(request)

        # redirects (e.g. trailing slash) are built from the stripped path, so
        # put the portal back on the front or the client lands on a 404
        location = response.headers.get('location')
        if location:
            url = urlsplit(location)
            if url.path.startswith('/') and url.netloc in ('', request.url.netloc):
                response.headers['location'] = urlunsplit(url._replace(path=prefix + url.path))

        return response


def get_portal_ctx(request):
    """
    The portal context the middleware resolved for this request.
    """
    ctx = getattr(request.state, 'portal_ctx', None)
    if ctx is None:
        raise RuntimeError('portal_ctx not set; PortalResolveMiddleware missing or path was reserved')

    return ctx
