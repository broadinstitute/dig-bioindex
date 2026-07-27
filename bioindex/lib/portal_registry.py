import logging


class PortalRegistry:
    """
    Map of portal name to PortalContext, built once per worker at startup
    and not modified afterwards.
    """

    def __init__(self, contexts):
        self._by_name = {}

        for ctx in contexts:
            if ctx.name in self._by_name:
                logging.warning('duplicate portal name %r; later definition overwrites earlier', ctx.name)
            self._by_name[ctx.name] = ctx

    def get(self, name):
        return self._by_name.get(name)

    def names(self):
        return sorted(self._by_name.keys())

    def __iter__(self):
        return iter(self._by_name.values())

    def __len__(self):
        return len(self._by_name)


_registry = None


def init_registry(contexts):
    global _registry

    _registry = PortalRegistry(contexts)
    return _registry


def get_registry():
    if _registry is None:
        raise RuntimeError('PortalRegistry not initialized')

    return _registry
