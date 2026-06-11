import pytest

from bioindex.lib.signed_tokens import (
    MAX_PAYLOAD_BYTES,
    TokenError,
    _b64u_encode,
    decode,
    encode,
)
from bioindex.lib.continuation import ContState


KEY = b"\x00" * 32


def _state(**overrides):
    base = dict(
        type="fetch", index_name="x", index_arity=1, qs=["a"],
        fmt="row", page=1,
        source_index=0, byte_offset=0, last_key=None, limit=None,
    )
    base.update(overrides)
    return ContState(**base)


def test_encode_decode_round_trip():
    s = _state(page=3, byte_offset=42)
    token = encode(s, KEY)
    decoded = decode(token, KEY)
    assert decoded.page == 3
    assert decoded.byte_offset == 42
    assert decoded.index_name == "x"


def test_tampered_payload_rejected():
    token = encode(_state(), KEY)
    head, sig = token.rsplit(".", 1)
    bad = head + "A.0" + sig
    with pytest.raises(TokenError, match="invalid"):
        decode(bad, KEY)


def test_wrong_key_rejected():
    token = encode(_state(), KEY)
    with pytest.raises(TokenError):
        decode(token, b"\x01" * 32)


def test_token_is_deterministic_for_same_state():
    det = ContState(type="fetch", index_name="i", index_arity=1, qs=["x"], generation="g1")
    assert encode(det, KEY) == encode(det, KEY)


def test_decode_never_expires():
    det = ContState(type="fetch", index_name="i", index_arity=1, qs=["x"], generation="g1")
    tok = encode(det, KEY)
    assert decode(tok, KEY).generation == "g1"


def test_oversized_state_returns_error():
    huge = _state(qs=["x"] * 100_000)  # large payload
    with pytest.raises(TokenError, match="too large"):
        encode(huge, KEY)


def test_encode_decode_with_complex_payload():
    """Regression — earlier version of ContState carried a dict[str, set]
    which json.dumps couldn't serialize. Make sure encode works with the
    full ContState shape."""
    s = _state(qs=["a", "b", "c"], source_index=5, byte_offset=100, page=4)
    token = encode(s, KEY)
    decoded = decode(token, KEY)
    assert decoded.qs == ["a", "b", "c"]
    assert decoded.source_index == 5
    assert decoded.byte_offset == 100
    assert decoded.page == 4


def test_oversized_token_rejected_on_decode():
    huge_b64 = _b64u_encode(b"x" * (MAX_PAYLOAD_BYTES + 1))
    fake_sig = _b64u_encode(b"\x00" * 32)
    token = f"{huge_b64}.{fake_sig}"
    with pytest.raises(TokenError, match="too large"):
        decode(token, KEY)


def test_signing_key_rejects_short_key(monkeypatch):
    # bypass lru_cache by clearing
    from bioindex.lib.signed_tokens import signing_key
    signing_key.cache_clear()
    monkeypatch.setenv("BIOINDEX_TOKEN_SIGNING_KEY", "short")
    with pytest.raises(RuntimeError, match=">=32"):
        signing_key()
    signing_key.cache_clear()
