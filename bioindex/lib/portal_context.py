import dataclasses
import time
from typing import Any, Dict, Optional, Tuple


@dataclasses.dataclass
class PortalContext:
    """
    Per-portal in-process state: config, DB engines, index metadata cache,
    GraphQL schema. ``config``, ``engine``, ``portal``, and ``gql_schema``
    are set once at startup and not expected to change. ``indexes`` may be
    refreshed at runtime when a query references an index that wasn't
    present at startup (atomic dict reassignment). ``last_used`` is mutated
    by ``touch()``.
    """
    name: str
    config: Any                  # bioindex.lib.config.Config
    engine: Any                  # sqlalchemy.Engine — bio schema pool
    portal: Optional[Any]        # sqlalchemy.Engine | None — portal schema pool
    indexes: Dict[Tuple[str, int], Any]  # (name, arity) -> Index
    gql_schema: Optional[Any]    # graphql.GraphQLSchema | None
    last_used: float = dataclasses.field(default_factory=time.time)

    def touch(self) -> None:
        self.last_used = time.time()
