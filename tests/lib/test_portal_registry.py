import pytest
from bioindex.lib.portal_registry import (
    init_registry, get_registry, PortalRegistry,
)
from bioindex.lib.portal_context import PortalContext


def _stub_ctx(name):
    return PortalContext(
        name=name, config=object(), engine=object(),
        portal=None, indexes={}, gql_schema=None,
    )


def test_init_registry_sets_global_singleton():
    reg = init_registry([_stub_ctx("a"), _stub_ctx("b")])
    assert isinstance(reg, PortalRegistry)
    assert get_registry() is reg


def test_registry_get_returns_context_by_name():
    init_registry([_stub_ctx("cfde")])
    assert get_registry().get("cfde").name == "cfde"


def test_registry_get_unknown_returns_none():
    init_registry([_stub_ctx("cfde")])
    assert get_registry().get("nope") is None


def test_registry_names_returns_sorted_list():
    init_registry([_stub_ctx("b"), _stub_ctx("a"), _stub_ctx("c")])
    assert get_registry().names() == ["a", "b", "c"]


def test_get_registry_before_init_raises():
    with pytest.raises(RuntimeError, match="not initialized"):
        get_registry()


def test_registry_warns_on_duplicate_names(caplog):
    import logging
    caplog.set_level(logging.WARNING)
    init_registry([_stub_ctx("dup"), _stub_ctx("dup")])
    assert any(
        "duplicate portal name" in r.message and "dup" in r.message
        for r in caplog.records
    )
    # later wins
    assert get_registry().get("dup") is not None
    assert len(get_registry()) == 1
