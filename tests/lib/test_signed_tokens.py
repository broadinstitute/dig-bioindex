import pytest
from freezegun import freeze_time

from bioindex.lib.signed_tokens import encode, decode, TokenError
from bioindex.lib.continuation import ContState


KEY = b"\x00" * 32


def _state(**overrides):
    base = dict(
        type="fetch", index_name="x", index_arity=1, qs=["a"],
        fmt="row", restricted=None, page=1,
        source_index=0, skip_count=0, last_key=None, limit=None,
    )
    base.update(overrides)
    return ContState(**base)


def test_encode_decode_round_trip():
    s = _state(page=3, skip_count=42)
    token = encode(s, KEY)
    decoded = decode(token, KEY)
    assert decoded.page == 3
    assert decoded.skip_count == 42
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


def test_expired_token_rejected():
    with freeze_time("2026-01-01 00:00:00"):
        token = encode(_state(), KEY)
    with freeze_time("2026-01-01 00:05:00"):
        with pytest.raises(TokenError, match="expired"):
            decode(token, KEY)


def test_oversized_state_returns_error():
    huge = _state(qs=["x"] * 100_000)  # large payload
    with pytest.raises(TokenError, match="too large"):
        encode(huge, KEY)
