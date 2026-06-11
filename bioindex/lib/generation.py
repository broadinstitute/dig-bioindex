import hashlib
import threading
import time

from sqlalchemy import text

_CACHE: dict[str, tuple[float, str]] = {}   # index_name -> (expires_at, fingerprint)
_CACHE_LOCK = threading.Lock()


def _fingerprint(key_version_pairs) -> str:
    h = hashlib.sha256()
    for key, version in sorted(key_version_pairs):
        h.update(key.encode()); h.update(b"\x00")
        h.update((version or "").encode()); h.update(b"\x00")
    return h.hexdigest()[:16]


def _read(engine, index_name: str) -> str:
    sql = text("SELECT `key`, `version` FROM `__Keys` WHERE `index` = :i AND `built` IS NOT NULL")
    with engine.connect() as conn:
        rows = conn.execute(sql, {"i": index_name}).fetchall()
    return _fingerprint((r[0], r[1]) for r in rows)


def index_generation(engine, index_name: str, ttl: int = 30) -> str:
    now = time.time()
    with _CACHE_LOCK:
        hit = _CACHE.get(index_name)
        if hit and hit[0] > now:
            return hit[1]
    fp = _read(engine, index_name)
    with _CACHE_LOCK:
        # re-check under the lock in case another thread already populated the entry
        hit = _CACHE.get(index_name)
        if not (hit and hit[0] > now):
            if ttl > 0:
                _CACHE[index_name] = (now + ttl, fp)
    return fp
