import ipaddress
import logging
import os
import re
import time
import uuid
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from ..lib.portal_registry import get_registry

access_log = logging.getLogger('bioindex.access')

# query parameters whose values are secrets rather than search terms
REDACTED_PARAMS = frozenset({'token', 'access_token'})

# a caller-supplied request id we are willing to log back verbatim
REQUEST_ID = re.compile(r'[A-Za-z0-9._-]{1,128}\Z')


def _request_id(header):
    return header if header and REQUEST_ID.match(header) else uuid.uuid4().hex


def _query(query):
    """
    The query string with secret values replaced. <redacted> is left
    unescaped so it stays greppable in the logs.
    """
    return '&'.join(
        quote(k, safe='') + '=' + ('<redacted>' if k.lower() in REDACTED_PARAMS else quote(v, safe=''))
        for k, v in parse_qsl(query, keep_blank_values=True)
    )


def _client_ip(request):
    """
    The client address, coarsened to a /24 (v4) or /48 (v6) before it is
    logged. nginx sets X-Real-IP to the peer it saw and the load balancer
    forwards it untouched; failing that, take the leftmost forwarded hop.
    """
    ip = request.headers.get('x-real-ip') or request.headers.get('x-forwarded-for', '').split(',')[0]
    ip = ip.strip() or (request.client.host if request.client else '')

    try:
        version = ipaddress.ip_address(ip).version
    except ValueError:
        return None

    return str(ipaddress.ip_network(f'{ip}/{24 if version == 4 else 48}', strict=False).network_address)


class PortalResolveMiddleware(BaseHTTPMiddleware):
    """
    Resolve the leading path segment to a portal, stash the context on the
    request, and strip the segment so the routers match their paths as-is.
    Every request also leaves one line on the bioindex.access logger.
    """

    def __init__(self, app, reserved_prefixes=()):
        super().__init__(app)
        self._reserved = set(reserved_prefixes)

    async def dispatch(self, request, call_next):
        start = time.monotonic()
        request.state.request_id = _request_id(request.headers.get('x-request-id'))

        head, _, rest = request.url.path.lstrip('/').partition('/')
        portal = None

        if head and head not in self._reserved:
            ctx = get_registry().get(head)
            if ctx is None:
                response = JSONResponse(
                    {'detail': f"Unknown portal '{head}'", 'valid_portals': get_registry().names()},
                    status_code=404,
                )
                self._log(request, start, response)
                return response

            request.state.portal_ctx = ctx
            portal = ctx.name

            # strip the portal prefix from the path the routers will see
            prefix = f'/{head}'.encode()
            request.scope['path'] = '/' + rest
            raw_path = request.scope.get('raw_path')
            if raw_path and raw_path.startswith(prefix):
                request.scope['raw_path'] = raw_path[len(prefix):] or b'/'

        try:
            response = await call_next(request)
        except Exception:
            # own the 500 so the access line carries the traceback, rather
            # than uvicorn logging a second, context-free copy of it
            self._log(request, start, None, portal=portal)
            return JSONResponse(
                {'detail': 'Internal server error', 'request_id': request.state.request_id},
                status_code=500,
            )

        if portal:
            # the router redirects a trailing slash using the stripped path,
            # which would send the client to a portal-less URL and 404; put
            # the portal back on. Only that redirect is rewritten - anything
            # a handler builds already knows which portal it is serving.
            location = response.headers.get('location')
            if location:
                url = urlsplit(location)
                stripped = request.scope['path']
                if (url.netloc in ('', request.url.netloc)
                        and url.path in (stripped.rstrip('/'), stripped + '/')):
                    response.headers['location'] = urlunsplit(url._replace(path=f'/{portal}{url.path}'))

        self._log(request, start, response, portal=portal)
        return response

    def _log(self, request, start, response, portal=None):
        """
        One structured access record. No response means the handler raised,
        so the record carries the traceback and a synthetic 500.
        """
        failed = response is None
        length = None if failed else response.headers.get('content-length')

        access_log.log(
            logging.ERROR if failed else logging.INFO,
            'request',
            exc_info=failed,
            extra={
                'portal': portal,
                'request_id': request.state.request_id,
                'method': request.method,
                'route': getattr(request.scope.get('route'), 'path', None),
                'path': request.scope['path'],
                'query': _query(request.url.query),
                'status': 500 if failed else response.status_code,
                'response_bytes': int(length) if length and length.isdigit() else 0,
                'latency_ms': int((time.monotonic() - start) * 1000),
                'worker_pid': os.getpid(),
                'client_ip': _client_ip(request),
                'user_agent': request.headers.get('user-agent', '')[:256],
            },
        )


def get_portal_ctx(request):
    """
    The portal context the middleware resolved for this request.
    """
    ctx = getattr(request.state, 'portal_ctx', None)
    if ctx is None:
        raise RuntimeError('portal_ctx not set; PortalResolveMiddleware missing or path was reserved')

    return ctx
