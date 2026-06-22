"""
Tests for signed continuation token minting and resumption in bio.py.

conftest.py stubs out AWS/DB calls so bio.py can be imported offline.
"""
import asyncio
import itertools
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

    result = bio._fetch_records(reader, "myindex", ["q1"], "row", page=1)

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


def test_fetch_records_minted_token_carries_generation():
    """A minted token must carry the stubbed generation value from conftest."""
    reader = _make_reader(
        records=[{"x": 1}],
        at_end=False,
        source_index=0,
        source_byte_offset=128,
    )
    result = bio._fetch_records(reader, "myindex", ["q1"], "row", page=1)
    token = result["continuation"]
    assert token is not None
    state = signed_tokens.decode(token, signed_tokens.signing_key())
    assert state.generation == "gen-test-fixed"


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
        generation="gen-test-fixed",
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
# (d) Stale generation (index was rebuilt) → HTTP 409
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_stale_generation_409():
    """
    A token whose generation does not match the current index generation
    raises HTTPException(409) — the index was rebuilt; client must re-run.
    """
    import fastapi

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
        generation="old-gen",  # does NOT match the stubbed "gen-test-fixed"
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    fake_index = MagicMock()
    fake_index.schema.arity = 1

    with patch.dict(bio.INDEXES, {("myindex", 1): fake_index}):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await bio.api_cont(token=token, req=_make_req())

    assert exc_info.value.status_code == 409
    assert "stale" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# (e) Match pagination: _match_keys mints a token; api_cont resumes via dropwhile
# ---------------------------------------------------------------------------

def test_match_keys_mints_token_at_limit():
    """_match_keys mints a continuation when query.match fills the page."""
    limit_count = bio.MATCH_LIMIT
    fake_index = MagicMock()
    fake_index.name = "myindex"
    with patch("bioindex.api.bio.query.match", return_value=list(range(limit_count))):
        result = bio._match_keys(fake_index, ["q1"], None, page=1)

    assert result["continuation"] is not None
    assert result["count"] == limit_count
    assert result["page"] == 1


def test_match_keys_no_token_below_limit():
    """_match_keys returns null continuation when query.match returns a partial page."""
    fake_index = MagicMock()
    fake_index.name = "myindex"
    with patch("bioindex.api.bio.query.match", return_value=["a", "b"]):
        result = bio._match_keys(fake_index, ["q1"], None, page=1)

    assert result["continuation"] is None


@pytest.mark.asyncio
async def test_api_cont_resumes_match_via_keyset_cursor():
    """
    api_cont resumes a match by passing after=last_key to query.match (the
    keyset cursor), so the SQL returns only keys strictly after last_key — no
    Python dropwhile, no repeats.
    """
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
        generation="gen-test-fixed",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    fake_index = MagicMock()
    fake_index.name = "myindex"
    next_keys = ["key_003", "key_004"]   # keyset query.match returns only post-cursor

    with patch.dict(bio.INDEXES, {("myindex", 1): fake_index}):
        with patch("bioindex.api.bio.query.match", return_value=next_keys) as mock_match:
            result = await bio.api_cont(token=token, req=_make_req())

    # query.match called as (config, engine, index, qs, after, page_size)
    assert mock_match.call_args.args[4] == last_key
    assert result["data"] == ["key_003", "key_004"]
    assert "key_002" not in result["data"]
    assert result["page"] == 2


# ---------------------------------------------------------------------------
# (f) /all continuation carries the index's real arity (regression)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# (g) Fix 2: limit in continuation token is the REMAINING budget, not original
# ---------------------------------------------------------------------------

def test_limit_is_decremented_into_continuation_token():
    """
    When page 1 returns `count` records and reader.at_end is False,
    the minted token's limit must be (original_limit - count), not original_limit.
    Otherwise /cont re-applies the full N, so a limit=10 query could return 10*pages records.
    """
    reader = _make_reader(records=[{"x": i} for i in range(4)], at_end=False,
                          limit=10, source_index=0, source_byte_offset=128)
    result = bio._fetch_records(reader, "myindex", ["q1"], "row", page=1)
    token = result["continuation"]
    assert token is not None
    state = signed_tokens.decode(token, signed_tokens.signing_key())
    assert state.limit == 6   # 10 original - 4 returned this page; NOT 10


@pytest.mark.asyncio
async def test_match_limit_capped_across_continuation_pages(monkeypatch):
    """Regression (#3/#4): /match?limit=N caps TOTAL keys across pages. With
    MATCH_LIMIT=2 and limit=3: page1 returns 2 (+token carrying remaining=1),
    page2 returns only 1 more and no further token. Total == 3."""
    monkeypatch.setattr(bio, "MATCH_LIMIT", 2)
    all_keys = [f"k{i:02d}" for i in range(10)]

    def fake_match(config, engine, index, q, after=None, limit=None):
        ks = [k for k in all_keys if after is None or k > after]
        return ks[:limit] if limit is not None else ks

    monkeypatch.setattr(bio.query, "match", fake_match)
    fake_index = MagicMock()
    fake_index.name = "myindex"

    # page 1: limit=3 -> page_size=min(2,3)=2 -> ["k00","k01"], remaining=1
    page1 = bio._match_keys(fake_index, ["q1"], 3, page=1)
    assert page1["count"] == 2
    assert page1["data"] == ["k00", "k01"]
    token = page1["continuation"]
    assert token is not None
    assert signed_tokens.decode(token, signed_tokens.signing_key()).limit == 1

    # page 2 via /cont: budget 1, after="k01" -> page_size=min(2,1)=1 -> ["k02"]
    with patch.dict(bio.INDEXES, {("myindex", 1): fake_index}):
        page2 = await bio.api_cont(token=token, req=_make_req())
    assert page2["count"] == 1
    assert page2["data"] == ["k02"]
    assert page2["continuation"] is None


