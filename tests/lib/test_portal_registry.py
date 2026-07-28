import pytest

from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import get_registry, init_registry


def _make_ctx(name):
    return PortalContext(name=name, config=object(), engine=object(), indexes={})


def test_get_by_name():
    init_registry([_make_ctx("a"), _make_ctx("b")])
    reg = get_registry()
    assert reg.get("a").name == "a"


def test_get_missing():
    init_registry([_make_ctx("a"), _make_ctx("b")])
    reg = get_registry()
    assert reg.get("missing") is None


def test_names_sorted():
    init_registry([_make_ctx("b"), _make_ctx("a")])
    reg = get_registry()
    names = reg.names()
    assert "a" in names and "b" in names
    assert names == sorted(names)


def test_len():
    init_registry([_make_ctx("a"), _make_ctx("b")])
    assert len(get_registry()) == 2


def test_get_registry_raises_before_init(monkeypatch):
    import bioindex.lib.portal_registry as pr
    monkeypatch.setattr(pr, "_registry", None)
    with pytest.raises(RuntimeError):
        pr.get_registry()
