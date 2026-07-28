import dataclasses
from typing import Any, Dict, Optional, Tuple


@dataclasses.dataclass
class PortalContext:
    """
    Everything a request handler needs to serve a single portal. All of it
    is built once at startup, except `indexes`, which is reassigned when a
    query names an index that wasn't in the database at the time.
    """
    name: str
    config: Any
    engine: Any
    indexes: Dict[Tuple[str, int], Any]
    portal: Optional[Any] = None
    gql_schema: Optional[Any] = None
