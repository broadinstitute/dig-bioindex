import sqlalchemy as sa
import pytest
from sqlalchemy import text

from bioindex.lib.index import Index


def _engine():
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.exec_driver_sql(
            "CREATE TABLE `__Indexes` (id INTEGER PRIMARY KEY, name TEXT, `table` TEXT, "
            "prefix TEXT, `schema` TEXT, built TEXT, compressed INT)"
        )
        c.exec_driver_sql(
            "CREATE TABLE `__Keys` (id INTEGER PRIMARY KEY, `index` TEXT, `key` TEXT, "
            "version TEXT, built TEXT, UNIQUE(`index`, `key`))"
        )
    return e


def _seed(e, temp_built="2026-06-29"):
    with e.begin() as c:
        c.exec_driver_sql(
            "INSERT INTO `__Indexes` (name,`table`,prefix,`schema`,built,compressed) "
            "VALUES ('vda','VDA','associations/variant/','varId=$chr:$pos','2026-05-01',1)"
        )
        c.exec_driver_sql(
            "INSERT INTO `__Indexes` (name,`table`,prefix,`schema`,built,compressed) "
            "VALUES ('vda__rebuild','VDA_v2','associations/variant/',"
            "'varId=$chr:$pos;locus_step=250',?,1)", (temp_built,)
        )
        c.exec_driver_sql(
            "INSERT INTO `__Keys` (`index`,`key`,version,built) "
            "VALUES ('vda','associations/variant/part-0.json.gz','OLD','2026-05-01')"
        )
        c.exec_driver_sql(
            "INSERT INTO `__Keys` (`index`,`key`,version,built) "
            "VALUES ('vda__rebuild','associations/variant/part-0.json.gz','NEW','2026-06-29')"
        )


def test_swap_into_repoints_metadata():
    e = _engine()
    _seed(e)

    old = Index.swap_into(e, "vda__rebuild", "vda")
    assert old == "VDA"

    with e.connect() as c:
        idx = c.execute(text("SELECT name,`table`,`schema` FROM `__Indexes`")).fetchall()
        assert len(idx) == 1
        assert idx[0][0] == "vda"
        assert idx[0][1] == "VDA_v2"
        assert idx[0][2] == "varId=$chr:$pos;locus_step=250"

        keys = c.execute(text("SELECT `index`,version FROM `__Keys`")).fetchall()
        assert len(keys) == 1
        assert keys[0][0] == "vda"
        assert keys[0][1] == "NEW"


def test_swap_into_refuses_unbuilt_temp():
    e = _engine()
    _seed(e, temp_built=None)
    with pytest.raises(ValueError):
        Index.swap_into(e, "vda__rebuild", "vda")


def test_swap_into_missing_index_raises():
    e = _engine()
    _seed(e)
    with pytest.raises(KeyError):
        Index.swap_into(e, "nope", "vda")
