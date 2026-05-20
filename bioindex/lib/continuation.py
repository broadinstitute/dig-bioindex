import dataclasses
import time
from typing import List, Optional


@dataclasses.dataclass
class ContState:
    """
    Serializable snapshot of everything needed to resume a paginated query.

    type == 'fetch': resume a query.fetch() — re-runs SQL to get sources,
                     seeks to source_index / byte_offset.
    type == 'all':   resume a query.fetch_all() — re-scans S3 prefix,
                     seeks to source_index / byte_offset.
    type == 'match': resume a query.match() — re-runs match query and
                     skips keys already returned (via last_key).

    portal_name is bound at issue time so a token issued under one portal
    cannot be replayed against another. Empty string means "legacy /
    unbound" and should be rejected when a portal context is required.

    NOTE: `restricted` is intentionally NOT stored here. The set of
    restricted phenotypes is re-derived from the requesting identity on
    every /cont call so that tokens cannot carry stale or borrowed
    authorization.
    """
    type: str
    index_name: str
    index_arity: int
    qs: List
    portal_name: str = ""             # bound at issue time; empty means "legacy"
    fmt: Optional[str] = None
    page: int = 1
    # fetch / all resume point
    source_index: int = 0
    byte_offset: int = 0       # bytes consumed within source[source_index], measured from source.start
    # match resume point
    last_key: Optional[str] = None
    limit: Optional[int] = None
    expiration: float = dataclasses.field(default_factory=lambda: time.time() + 900)
