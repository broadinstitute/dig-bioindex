import sqlalchemy as sa
from bioindex.lib.generation import index_generation, _fingerprint


BUILT_TS = "2026-01-01 00:00:00"  # non-null sentinel matching real DateTime semantics


def _engine_with_keys(rows):
    eng = sa.create_engine("sqlite://")
    with eng.begin() as c:
        c.exec_driver_sql("CREATE TABLE `__Keys` (id INTEGER PRIMARY KEY, `index` TEXT, `key` TEXT, version TEXT, built TEXT)")
        for i, (idx, key, ver, built) in enumerate(rows, 1):
            c.exec_driver_sql("INSERT INTO `__Keys` VALUES (?,?,?,?,?)", (i, idx, key, ver, built))
    return eng


def test_generation_changes_when_a_version_changes():
    e1 = _engine_with_keys([("bio", "a.json", "v1", BUILT_TS), ("bio", "b.json", "v1", BUILT_TS)])
    e2 = _engine_with_keys([("bio", "a.json", "v1", BUILT_TS), ("bio", "b.json", "v2", BUILT_TS)])
    assert index_generation(e1, "bio", ttl=0) != index_generation(e2, "bio", ttl=0)


def test_generation_ignores_other_indexes_and_unbuilt_keys():
    # built=BUILT_TS (non-null timestamp) → built; built=None (NULL) → unbuilt
    e = _engine_with_keys([
        ("bio", "a.json", "v1", BUILT_TS),   # built bio key — must be included
        ("other", "z.json", "v9", BUILT_TS), # wrong index — must be excluded
        ("bio", "c.json", "v1", None),        # unbuilt (NULL) — must be excluded
    ])
    only_bio_built = _fingerprint([("a.json", "v1")])
    assert index_generation(e, "bio", ttl=0) == only_bio_built


def test_generation_ttl_cache_returns_same_value():
    """Within the TTL window, repeated calls return the cached fingerprint."""
    e1 = _engine_with_keys([("bio", "a.json", "v1", BUILT_TS)])
    # First call populates cache with ttl=60
    g1 = index_generation(e1, "bio-ttl-test", ttl=60)
    # Second call with a different engine but same index name → must return cached
    e2 = _engine_with_keys([("bio-ttl-test", "z.json", "v99", BUILT_TS)])
    g2 = index_generation(e2, "bio-ttl-test", ttl=60)
    assert g1 == g2
