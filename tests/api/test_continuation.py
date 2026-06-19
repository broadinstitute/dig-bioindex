"""
Tests for signed continuation token minting and resumption in bio.py.

conftest.py stubs out AWS/DB calls so bio.py can be imported offline.
"""
import asyncio
import itertools
import time
import types
from unittest.mock import MagicMock, patch

import pytest

# conftest.py (same package) runs its stubs before this import
import bioindex.api.bio as bio
from bioindex.lib.continuation import ContState
from bioindex.lib import signed_tokens


# ---------------------------------------------------------------------------
# Fake reader helpers
# ---------------------------------------------------------------------------

def _make_reader(records, at_end=True, source_index=0, source_byte_offset=0,
                 bytes_read=0, bytes_total=1000, limit=None, restricted_count=0):
    """Build a MagicMock that quacks like RecordReader for bio.py."""
    r = MagicMock()
    r.records = iter(records)
    r.at_end = at_end
    r._source_index = source_index
    r._source_byte_offset = source_byte_offset
    r.bytes_read = bytes_read
    r.bytes_total = bytes_total
    r.limit = limit
    r.restricted_count = restricted_count
    # index.schema.arity used by branch _fetch_records but we stripped it; keep it anyway
    r.index.schema.arity = 1
    return r


def _make_req():
    """Minimal fake FastAPI Request (portal is None so auth is skipped)."""
    return MagicMock()


# ---------------------------------------------------------------------------
# (a) _fetch_records mints a non-null token when reader is not at end
# ---------------------------------------------------------------------------

def test_fetch_records_mints_token_when_not_at_end():
    """_fetch_records returns a non-null continuation when reader.at_end is False."""
    reader = _make_reader(
        records=[{"x": 1}, {"x": 2}],
        at_end=False,
        source_index=1,
        source_byte_offset=512,
        bytes_read=100,
        bytes_total=2000,
    )

    result = bio._fetch_records(reader, "myindex", ["q1"], "row", cont_type="fetch", page=1)

    assert result["continuation"] is not None, "expected a continuation token"
    assert result["page"] == 1
    assert result["count"] == 2


def test_fetch_records_no_token_when_at_end():
    """_fetch_records returns null continuation when reader.at_end is True."""
    reader = _make_reader(
        records=[{"x": 1}],
        at_end=True,
    )

    result = bio._fetch_records(reader, "myindex", ["q1"], "row")

    assert result["continuation"] is None


# ---------------------------------------------------------------------------
# (b) api_cont resumes a fetch continuation (page == 2)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_resumes_fetch():
    """
    api_cont with a valid fetch token calls query.fetch with the saved
    source_index / byte_offset and returns page 2.
    """
    # Build a token the way bio.py would: ContState → signed_tokens.encode
    state = ContState(
        type="fetch",
        index_name="myindex",
        index_arity=1,
        qs=["q1"],
        fmt="row",
        page=2,
        source_index=1,
        byte_offset=512,
        limit=None,
        issued_at=time.time(),
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    # Fake index in INDEXES
    fake_index = MagicMock()
    fake_index.schema.arity = 1

    resume_reader = _make_reader(
        records=[{"x": 99}],
        at_end=True,
        bytes_read=200,
        bytes_total=200,
    )

    with patch.dict(bio.INDEXES, {("myindex", 1): fake_index}):
        with patch("bioindex.api.bio.query.fetch", return_value=resume_reader) as mock_fetch:
            result = await bio.api_cont(token=token, req=_make_req())

    assert result["page"] == 2
    assert result["count"] == 1
    assert result["continuation"] is None
    # verify resume params were forwarded
    _, kwargs = mock_fetch.call_args
    assert kwargs.get("start_source_index") == 1
    assert kwargs.get("start_byte_offset") == 512


# ---------------------------------------------------------------------------
# (c) Tampered token → HTTP 400
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_tampered_token_400():
    """A token with a bad signature raises HTTPException(400)."""
    import fastapi
    with pytest.raises(fastapi.HTTPException) as exc_info:
        await bio.api_cont(token="bad.token.value", req=_make_req())
    assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# (d) Expired token (issued_at too old) → HTTP 400
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_expired_token_400():
    """A token whose issued_at is older than CONT_TTL raises HTTPException(400)."""
    import fastapi

    old_issued_at = time.time() - (bio.CONT_TTL + 120)
    state = ContState(
        type="fetch",
        index_name="myindex",
        index_arity=1,
        qs=["q1"],
        fmt="row",
        page=2,
        source_index=0,
        byte_offset=0,
        limit=None,
        issued_at=old_issued_at,
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await bio.api_cont(token=token, req=_make_req())
    assert exc_info.value.status_code == 400
    assert "expired" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# (e) Match pagination: _match_keys mints a token; api_cont resumes via dropwhile
# ---------------------------------------------------------------------------

def test_match_keys_mints_token_at_limit():
    """_match_keys mints a continuation when exactly MATCH_LIMIT keys are returned."""
    # We need exactly MATCH_LIMIT keys so the condition triggers
    limit_count = bio.MATCH_LIMIT
    keys = iter(range(limit_count))

    result = bio._match_keys(keys, "myindex", ["q1"], None, page=1)

    assert result["continuation"] is not None
    assert result["count"] == limit_count
    assert result["page"] == 1


def test_match_keys_no_token_below_limit():
    """_match_keys returns null continuation when fewer than MATCH_LIMIT keys returned."""
    keys = iter(["a", "b"])

    result = bio._match_keys(keys, "myindex", ["q1"], None, page=1)

    assert result["continuation"] is None


@pytest.mark.asyncio
async def test_api_cont_resumes_match_no_repeats():
    """
    api_cont with a match token calls query.match and dropwhile(k <= last_key)
    so no previously-seen keys are repeated.
    """
    # Page 1 ended at last_key = "key_002"
    last_key = "key_002"
    state = ContState(
        type="match",
        index_name="myindex",
        index_arity=1,
        qs=["q1"],
        fmt=None,
        page=2,
        last_key=last_key,
        limit=None,
        issued_at=time.time(),
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    fake_index = MagicMock()
    # query.match returns all keys including ones already seen
    all_keys = ["key_001", "key_002", "key_003", "key_004"]

    with patch.dict(bio.INDEXES, {("myindex", 1): fake_index}):
        with patch("bioindex.api.bio.query.match", return_value=iter(all_keys)):
            result = await bio.api_cont(token=token, req=_make_req())

    # Only keys strictly after last_key should appear
    assert "key_001" not in result["data"]
    assert "key_002" not in result["data"]
    assert "key_003" in result["data"]
    assert "key_004" in result["data"]
    assert result["page"] == 2


# ---------------------------------------------------------------------------
# (f) /all continuation carries the index's real arity (regression)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_resumes_all_for_nonzero_arity_index():
    """
    On the /all path qs is None, so a naive len(qs or []) would mint
    index_arity=0. INDEXES is keyed by (name, schema_arity), so resuming
    such a token would miss the index and 400. The token must carry the
    index's real arity, and api_cont must resume via query.fetch_all.
    """
    mint_reader = _make_reader(records=[{"x": 1}], at_end=False,
                               source_index=0, source_byte_offset=256)
    mint_reader.index.schema.arity = 2  # a non-zero-arity index, as /all serves

    page1 = bio._fetch_records(mint_reader, "myindex", None, "row",
                               cont_type="all", page=1)
    token = page1["continuation"]
    assert token is not None
    # The token must NOT carry arity 0.
    assert signed_tokens.decode(token, signed_tokens.signing_key()).index_arity == 2

    fake_index = MagicMock()
    resume_reader = _make_reader(records=[{"x": 2}], at_end=True)

    with patch.dict(bio.INDEXES, {("myindex", 2): fake_index}):
        with patch("bioindex.api.bio.query.fetch_all",
                   return_value=resume_reader) as mock_all:
            result = await bio.api_cont(token=token, req=_make_req())

    assert result["page"] == 2   # resumed cleanly, not a 400
    assert result["count"] == 1
    _, kwargs = mock_all.call_args
    assert kwargs.get("start_source_index") == 0
    assert kwargs.get("start_byte_offset") == 256
