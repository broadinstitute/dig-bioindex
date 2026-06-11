import sqlalchemy as sa
from bioindex.lib.generation import index_generation, _fingerprint


def _engine_with_keys(rows):
    eng = sa.create_engine("sqlite://")
    with eng.begin() as c:
        c.exec_driver_sql("CREATE TABLE `__Keys` (id INTEGER PRIMARY KEY, `index` TEXT, `key` TEXT, version TEXT, built INT)")
        for i, (idx, key, ver, built) in enumerate(rows, 1):
            c.exec_driver_sql("INSERT INTO `__Keys` VALUES (?,?,?,?,?)", (i, idx, key, ver, built))
    return eng


def test_generation_changes_when_a_version_changes():
    e1 = _engine_with_keys([("bio", "a.json", "v1", 1), ("bio", "b.json", "v1", 1)])
    e2 = _engine_with_keys([("bio", "a.json", "v1", 1), ("bio", "b.json", "v2", 1)])
    assert index_generation(e1, "bio", ttl=0) != index_generation(e2, "bio", ttl=0)


def test_generation_ignores_other_indexes_and_unbuilt_keys():
    e = _engine_with_keys([("bio", "a.json", "v1", 1), ("other", "z.json", "v9", 1), ("bio", "c.json", "v1", 0)])
    only_bio_built = _fingerprint([("a.json", "v1")])
    assert index_generation(e, "bio", ttl=0) == only_bio_built
