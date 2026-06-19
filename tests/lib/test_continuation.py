import dataclasses
from bioindex.lib.continuation import ContState

def test_contstate_defaults():
    s = ContState(type="all", index_name="g", index_arity=1, qs=[], fmt="row")
    assert s.page == 1 and s.source_index == 0 and s.byte_offset == 0
    assert s.portal_name == "" and s.last_key is None and s.issued_at == 0.0

def test_contstate_asdict_round_trips():
    s = ContState(type="match", index_name="g", index_arity=2, qs=["a", "b"], fmt="row",
                  last_key="k", page=3, issued_at=123.0)
    assert ContState(**dataclasses.asdict(s)) == s
