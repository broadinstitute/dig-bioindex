from typing import Iterable, List, Optional

from .portal_context import PortalContext


class PortalRegistry:
    """
    Immutable-after-init map of portal name -> PortalContext. Built once
    in each worker process at startup.
    """
    def __init__(self, contexts: Iterable[PortalContext]):
        self._by_name = {c.name: c for c in contexts}

    def get(self, name: str) -> Optional[PortalContext]:
        return self._by_name.get(name)

    def names(self) -> List[str]:
        return sorted(self._by_name.keys())

    def __len__(self) -> int:
        return len(self._by_name)


_registry: Optional[PortalRegistry] = None


def init_registry(contexts: Iterable[PortalContext]) -> PortalRegistry:
    global _registry
    _registry = PortalRegistry(contexts)
    return _registry


def get_registry() -> PortalRegistry:
    if _registry is None:
        raise RuntimeError("PortalRegistry not initialized")
    return _registry
