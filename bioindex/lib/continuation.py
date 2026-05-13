import dataclasses
import time


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
