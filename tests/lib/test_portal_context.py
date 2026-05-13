import time
from bioindex.lib.portal_context import PortalContext


def test_portal_context_can_be_constructed_with_engines_and_indexes():
    ctx = PortalContext(
        name="example",
        config=object(),       # opaque to this test
        engine=object(),
        portal=None,
        indexes={},
        gql_schema=None,
    )
    assert ctx.name == "example"
    assert ctx.engine is not None
    assert ctx.last_used > 0


def test_portal_context_touch_updates_last_used():
    ctx = PortalContext(
        name="example",
        config=object(), engine=object(),
        portal=None, indexes={}, gql_schema=None,
    )
    old = ctx.last_used
    time.sleep(0.001)
    ctx.touch()
    assert ctx.last_used > old
