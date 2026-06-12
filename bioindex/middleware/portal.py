import ipaddress
import logging
import os
import re
import time
import uuid
from typing import Iterable
from urllib.parse import parse_qsl, quote

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from ..lib.portal_registry import get_registry


_access_log = logging.getLogger("bioindex.access")


_REDACTED_QUERY_PARAMS = frozenset({"token", "access_token"})


def _scrub_query(query: str) -> str:
    """
    Replace values of sensitive query parameters with <redacted>.
    Returns the re-encoded query string. Preserves non-sensitive params.
    The literal string ``<redacted>`` is written as-is (not percent-encoded)
    so it remains grep-friendly in structured logs.
    """
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    parts = []
    for k, v in pairs:
        if k.lower() in _REDACTED_QUERY_PARAMS:
            parts.append(f"{quote(k, safe='')}=<redacted>")
        else:
            parts.append(f"{quote(k, safe='')}={quote(v, safe='')}")
    return "&".join(parts)


def _truncate_ip(ip):
    """
    Coarsen a client IP for privacy before logging: IPv4 -> /24, IPv6 -> /48.
    Returns the network address as a string, or None if ``ip`` isn't a valid
    address (e.g. a placeholder peer like ``testclient``).
    """
    if not ip:
        return None
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return None
    prefix = 24 if addr.version == 4 else 48
    return str(ipaddress.ip_network(f"{ip}/{prefix}", strict=False).network_address)


def _client_ip(request):
    """
    Best-effort real client IP, truncated for privacy. nginx sets
    ``X-Real-IP = $remote_addr`` (single value the ALB forwards untouched);
    fall back to the leftmost ``X-Forwarded-For`` hop, then the direct peer.
    """
    xri = request.headers.get("x-real-ip")
    raw = xri.strip() if xri else None
    if not raw:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            raw = xff.split(",", 1)[0].strip()
    if not raw:
        raw = request.client.host if request.client else None
    return _truncate_ip(raw)


_REQUEST_ID_RE = re.compile(r"\A[A-Za-z0-9._\-]{1,128}\Z")


def _safe_request_id(header_value):
    if header_value and _REQUEST_ID_RE.match(header_value):
        return header_value
    return uuid.uuid4().hex


class PortalResolveMiddleware(BaseHTTPMiddleware):
    """
    Read the first path segment as the portal name, look up the matching
    PortalContext from the registry, attach it to request.state.portal_ctx,
    then rewrite the request path to strip the prefix so existing routers
    work unchanged. Reserved prefixes (e.g. health, ready, metrics) bypass
    resolution.

    Also emits one structured JSON access log line per HTTP request via
    the ``bioindex.access`` logger.
    """
    def __init__(self, app: ASGIApp, reserved_prefixes: Iterable[str] = ()):
        super().__init__(app)
        self._reserved = set(reserved_prefixes)

    async def dispatch(self, request, call_next):
        start = time.time()
        request_id = _safe_request_id(request.headers.get("x-request-id"))
        request.state.request_id = request_id

        original_path = request.url.path
        segments = original_path.lstrip("/").split("/", 1)
        head = segments[0]

        portal_name = None
        if head not in self._reserved and head != "":
            registry = get_registry()
            ctx = registry.get(head)
            if ctx is None:
                response = JSONResponse(
                    {
                        "detail": f"Unknown portal '{head}'",
                        "valid_portals": registry.names(),
                    },
                    status_code=404,
                )
                self._log(
                    request, response, start, request_id,
                    portal=None, route=None, path=original_path,
                )
                return response

            # Stash on request.state for downstream handlers
            request.state.portal_ctx = ctx
            ctx.touch()
            portal_name = ctx.name

            # Rewrite scope path so existing routers don't need to know about prefixes.
            remainder = "/" + segments[1] if len(segments) > 1 else "/"
            request.scope["path"] = remainder
            # rewrite scope["raw_path"] by stripping the portal prefix bytes from the
            # ORIGINAL raw_path, preserving any percent-encoding present in the sub-path
            raw = request.scope.get("raw_path") or original_path.encode("utf-8")
            prefix_bytes = ("/" + head).encode("utf-8")
            if raw.startswith(prefix_bytes):
                request.scope["raw_path"] = raw[len(prefix_bytes):] or b"/"
            else:
                # safety fallback if raw_path doesn't have the expected prefix
                request.scope["raw_path"] = remainder.encode("utf-8")

        response = await call_next(request)

        # Pull route template from the matched route, if any
        route_template = None
        matched = request.scope.get("route")
        if matched is not None and hasattr(matched, "path"):
            route_template = matched.path

        self._log(
            request, response, start, request_id,
            portal=portal_name,
            route=route_template,
            path=request.scope.get("path") or original_path,
        )
        return response

    def _log(self, request, response, start, request_id, *, portal, route, path):
        response_bytes = 0
        cl = response.headers.get("content-length")
        if cl:
            try:
                response_bytes = int(cl)
            except ValueError:
                response_bytes = 0
        _access_log.info(
            "request",
            extra={
                "portal": portal,
                "request_id": request_id,
                "method": request.method,
                "route": route,
                "path": path,
                "query": _scrub_query(request.url.query),
                "status": response.status_code,
                "response_bytes": response_bytes,
                "latency_ms": int((time.time() - start) * 1000),
                "worker_pid": os.getpid(),
                "client_ip": _client_ip(request),
                "user_agent": (request.headers.get("user-agent") or "")[:256],
            },
        )


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
