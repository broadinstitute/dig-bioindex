from bioindex.lib.response_cache import ResponseCache

def test_get_set_roundtrip():
    c = ResponseCache(max_bytes=1000)
    assert c.get("k") is None
    c.set("k", {"data": [1, 2, 3]}, size=10)
    assert c.get("k") == {"data": [1, 2, 3]}

def test_lru_evicts_when_over_budget():
    c = ResponseCache(max_bytes=100)
    c.set("a", {"v": "a"}, size=60)
    c.set("b", {"v": "b"}, size=60)   # evicts "a"
    assert c.get("a") is None
    assert c.get("b") == {"v": "b"}

def test_get_refreshes_recency():
    c = ResponseCache(max_bytes=120)
    c.set("a", 1, size=60); c.set("b", 2, size=60)
    assert c.get("a") == 1             # touch "a"
    c.set("c", 3, size=60)             # should evict "b", not "a"
    assert c.get("a") == 1
    assert c.get("b") is None
