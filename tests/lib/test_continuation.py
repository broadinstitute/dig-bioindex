import dataclasses
from bioindex.lib.continuation import ContState


def test_contstate_has_generation_and_no_expiration():
    fields = {f.name for f in dataclasses.fields(ContState)}
    assert "generation" in fields
    assert "expiration" not in fields


def test_contstate_is_deterministic():
    a = ContState(type="fetch", index_name="i", index_arity=1, qs=["x"], generation="g1")
    b = ContState(type="fetch", index_name="i", index_arity=1, qs=["x"], generation="g1")
    assert dataclasses.asdict(a) == dataclasses.asdict(b)
