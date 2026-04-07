import dataclasses
import os
import secrets
import threading
import time
from multiprocessing.managers import BaseManager

from .utils import nonce


# ---------------------------------------------------------------------------
# Manager server — started in the main process by start_manager_server()
# before uvicorn forks workers. Workers connect lazily on first use.
# ---------------------------------------------------------------------------

_shared_state: dict = {}


class _ContinuationManager(BaseManager):
    pass


_ContinuationManager.register('get_cont_map', callable=lambda: _shared_state)

# Retained reference to the server-side manager (main process only).
# Must be kept alive to prevent the manager subprocess from being GC'd.
_manager_server = None

# Worker-side proxy references (initialized lazily)
_cont_map = None
_init_lock = threading.Lock()


def start_manager_server() -> tuple[int, str]:
    """
    Start the Manager server process and return (port, authkey_hex).
    Call this once in the main process before starting uvicorn workers.
    The port and authkey are written to env vars so workers can connect.
    """
    global _manager_server
    authkey = secrets.token_bytes(32)
    manager = _ContinuationManager(address=('127.0.0.1', 0), authkey=authkey)
    manager.start()
    _manager_server = manager  # keep alive; prevents the subprocess from being GC'd
    port = manager.address[1]
    authkey_hex = authkey.hex()

    os.environ['BIOINDEX_MANAGER_PORT'] = str(port)
    os.environ['BIOINDEX_MANAGER_AUTHKEY'] = authkey_hex

    return port, authkey_hex


def _get_map():
    """
    Return the shared continuation map, connecting to the Manager server
    on first call. Falls back to a plain thread-safe dict when running
    with a single worker (no env vars set).
    """
    global _cont_map
    if _cont_map is not None:
        return _cont_map

    with _init_lock:
        if _cont_map is not None:
            return _cont_map

        port_str = os.environ.get('BIOINDEX_MANAGER_PORT')
        authkey_hex = os.environ.get('BIOINDEX_MANAGER_AUTHKEY')

        if port_str and authkey_hex:
            m = _ContinuationManager(
                address=('127.0.0.1', int(port_str)),
                authkey=bytes.fromhex(authkey_hex),
            )
            m.connect()
            _cont_map = m.get_cont_map()
        else:
            # Single-worker mode: plain dict (thread-safe via _map_lock below)
            _cont_map = {}

        return _cont_map


# Used only in single-worker mode where _cont_map is a plain dict
_map_lock = threading.RLock()


def _map_lock_ctx(d):
    """Return the lock only when d is a plain dict (single-worker mode)."""
    return _map_lock if isinstance(d, dict) else _NullContext()


class _NullContext:
    def __enter__(self): return self
    def __exit__(self, *_): pass


# ---------------------------------------------------------------------------
# Continuation state — fully serializable, no closures
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class ContState:
    """
    Serializable snapshot of everything needed to resume a paginated query.

    type == 'fetch': resume a query.fetch() — re-runs SQL to get sources,
                     seeks to source_index / skip_count.
    type == 'all':   resume a query.fetch_all() — re-scans S3 prefix,
                     seeks to source_index / skip_count.
    type == 'match': resume a query.match() — re-runs match query and
                     skips keys already returned (via last_key).
    """
    type: str
    index_name: str
    index_arity: int
    qs: list
    fmt: str = None
    restricted: dict = None   # dict[str, set] — picklable
    page: int = 1
    # fetch / all resume point
    source_index: int = 0
    skip_count: int = 0
    # match resume point
    last_key: str = None
    limit: int = None
    expiration: float = dataclasses.field(default_factory=lambda: time.time() + 60)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def make_continuation(**kwargs) -> str:
    """
    Store a ContState and return an opaque token.
    """
    state = ContState(**kwargs)
    token = nonce()
    d = _get_map()
    with _map_lock_ctx(d):
        d[token] = state
    return token


def lookup_and_remove_continuation(token: str) -> ContState:
    """
    Atomically fetch and delete a continuation. Raises KeyError if missing
    or expired.
    """
    d = _get_map()
    with _map_lock_ctx(d):
        state = d.pop(token, None)

    if state is None or time.time() > state.expiration:
        raise KeyError(token)

    return state


# ---------------------------------------------------------------------------
# Background cleanup
# ---------------------------------------------------------------------------

def _cleanup_continuations():
    while True:
        time.sleep(60)
        try:
            d = _get_map()
            now = time.time()
            with _map_lock_ctx(d):
                expired = [t for t, s in list(d.items()) if now > s.expiration]
                for t in expired:
                    d.pop(t, None)
        except Exception:
            pass  # don't crash the cleanup thread


threading.Thread(target=_cleanup_continuations, daemon=True).start()
