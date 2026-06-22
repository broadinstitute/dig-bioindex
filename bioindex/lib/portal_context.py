import dataclasses
import time
from typing import Any, Dict, Optional, Tuple


@dataclasses.dataclass
class PortalContext:
    name: str
    config: Any
    engine: Any
    indexes: Dict[Tuple[str, int], Any]
    portal: Optional[Any] = None
    gql_schema: Optional[Any] = None
    last_used: float = dataclasses.field(default_factory=time.time)

    def touch(self) -> None:
        self.last_used = time.time()
