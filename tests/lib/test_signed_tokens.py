import pytest
from bioindex.lib.continuation import ContState
from bioindex.lib import signed_tokens

KEY = b"\x01" * 32

def _state():
    return ContState(type="fetch", index_name="genes", index_arity=1, qs=["X"], fmt="row")

def test_round_trip():
    tok = signed_tokens.encode(_state(), KEY)
    assert signed_tokens.decode(tok, KEY) == _state()

def test_tamper_payload_rejected():
    tok = signed_tokens.encode(_state(), KEY)
    bad = ("A" + tok[1:]) if tok[0] != "A" else ("B" + tok[1:])
    with pytest.raises(signed_tokens.TokenError):
        signed_tokens.decode(bad, KEY)

def test_wrong_key_rejected():
    tok = signed_tokens.encode(_state(), KEY)
    with pytest.raises(signed_tokens.TokenError):
        signed_tokens.decode(tok, b"\x02" * 32)

def test_garbage_rejected():
    with pytest.raises(signed_tokens.TokenError):
        signed_tokens.decode("not-a-token", KEY)

def test_oversize_payload_rejected():
    big = ContState(type="fetch", index_name="g", index_arity=1, qs=["x" * 200000], fmt="row")
    with pytest.raises(signed_tokens.TokenError):
        signed_tokens.encode(big, KEY)

def test_signing_key_requires_min_length(monkeypatch):
    signed_tokens.signing_key.cache_clear()
    monkeypatch.setenv("BIOINDEX_TOKEN_SIGNING_KEY", "00" * 16)  # 16 bytes < 32
    with pytest.raises(RuntimeError):
        signed_tokens.signing_key()
    signed_tokens.signing_key.cache_clear()
