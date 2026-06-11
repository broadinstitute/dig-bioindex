import threading
from collections import OrderedDict


class ResponseCache:
    """Per-process byte-bounded LRU. Thread-safe (workers use a 20-thread pool)."""

    def __init__(self, max_bytes: int):
        self._max = max_bytes
        self._cur = 0
        self._d: "OrderedDict[str, tuple[object, int]]" = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            item = self._d.get(key)
            if item is None:
                return None
            self._d.move_to_end(key)
            return item[0]

    def set(self, key, value, size: int):
        if size > self._max:
            return
        with self._lock:
            if key in self._d:
                self._cur -= self._d[key][1]
                del self._d[key]
            self._d[key] = (value, size)
            self._cur += size
            while self._cur > self._max and self._d:
                _, (_, sz) = self._d.popitem(last=False)
                self._cur -= sz
