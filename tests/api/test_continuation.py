"""
Tests for signed continuation token minting and resumption in bio.py.

conftest.py stubs out AWS/DB calls so bio.py can be imported offline.
"""
import asyncio
import types
from unittest.mock import MagicMock, patch

import orjson
import pytest

import bioindex.api.bio as bio
from bioindex.lib.continuation import ContState
from bioindex.lib import signed_tokens


# ---------------------------------------------------------------------------
# Fake ctx / reader helpers
# ---------------------------------------------------------------------------

def _make_ctx(name="testportal", indexes=None):
    ctx = types.SimpleNamespace(
        name=name,
        engine=None,
        portal=None,
        indexes=indexes or {},
    )
    cfg = types.SimpleNamespace(
        match_limit=100,
        response_limit=10_000_000,
        response_limit_max=20_000_000,
    )
    ctx.config = cfg
    return ctx


def _make_reader(records, at_end=True, source_index=0, source_byte_offset=0,
                 bytes_read=0, bytes_total=1000, limit=None, restricted_count=0):
    r = MagicMock()
    r.records = iter(records)
    r.at_end = at_end
    r._source_index = source_index
    r._source_byte_offset = source_byte_offset
    r.bytes_read = bytes_read
    r.bytes_total = bytes_total
    r.limit = limit
    r.restricted_count = restricted_count
    r.index.schema.arity = 1
    return r


def _make_req(portal_ctx=None):
    req = MagicMock()
    if portal_ctx is not None:
        req.state.portal_ctx = portal_ctx
    return req


# ---------------------------------------------------------------------------
# (a) _fetch_records mints a non-null token when reader is not at end
# ---------------------------------------------------------------------------

def test_fetch_records_mints_token_when_not_at_end():
    ctx = _make_ctx()
    reader = _make_reader(
        records=[{"x": 1}, {"x": 2}],
        at_end=False,
        source_index=1,
        source_byte_offset=512,
        bytes_read=100,
        bytes_total=2000,
    )
    result = bio._fetch_records(ctx, reader, "myindex", ["q1"], "row",
                                generation="gen-test-fixed", page=1)
    assert result["continuation"] is not None
    assert result["page"] == 1
    assert result["count"] == 2


def test_fetch_records_no_token_when_at_end():
    ctx = _make_ctx()
    reader = _make_reader(records=[{"x": 1}], at_end=True)
    result = bio._fetch_records(ctx, reader, "myindex", ["q1"], "row",
                                generation="gen-test-fixed")
    assert result["continuation"] is None


def test_fetch_records_minted_token_carries_generation():
    ctx = _make_ctx()
    reader = _make_reader(
        records=[{"x": 1}],
        at_end=False,
        source_index=0,
        source_byte_offset=128,
    )
    result = bio._fetch_records(ctx, reader, "myindex", ["q1"], "row",
                                generation="gen-test-fixed", page=1)
    token = result["continuation"]
    assert token is not None
    state = signed_tokens.decode(token, signed_tokens.signing_key())
    assert state.generation == "gen-test-fixed"


# ---------------------------------------------------------------------------
# (b) api_cont resumes a fetch continuation (page == 2)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_resumes_fetch():
    ctx = _make_ctx(name="testportal")
    fake_index = MagicMock()
    fake_index.schema.arity = 1

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
        portal_name="testportal",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    ctx.indexes = {("myindex", 1): fake_index}
    resume_reader = _make_reader(
        records=[{"x": 99}],
        at_end=True,
        bytes_read=200,
        bytes_total=200,
    )

    req = _make_req(portal_ctx=ctx)
    with patch("bioindex.api.bio.query.fetch", return_value=resume_reader) as mock_fetch:
        result = await bio.api_cont(token=token, req=req)

    body = result.body
    data = orjson.loads(body)
    assert data["page"] == 2
    assert data["count"] == 1
    assert data["continuation"] is None
    _, kwargs = mock_fetch.call_args
    assert kwargs.get("start_source_index") == 1
    assert kwargs.get("start_byte_offset") == 512


# ---------------------------------------------------------------------------
# (c) Tampered token → HTTP 400
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_tampered_token_400():
    import fastapi
    ctx = _make_ctx()
    req = _make_req(portal_ctx=ctx)
    with pytest.raises(fastapi.HTTPException) as exc_info:
        await bio.api_cont(token="bad.token.value", req=req)
    assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# (d) Stale generation (index was rebuilt) → HTTP 409
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_cont_stale_generation_409():
    import fastapi
    ctx = _make_ctx(name="testportal")
    fake_index = MagicMock()
    fake_index.schema.arity = 1
    ctx.indexes = {("myindex", 1): fake_index}

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
        generation="old-gen",
        portal_name="testportal",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())
    req = _make_req(portal_ctx=ctx)

    # conftest stubs index_generation to return "gen-test-fixed" which != "old-gen"
    with pytest.raises(fastapi.HTTPException) as exc_info:
        await bio.api_cont(token=token, req=req)

    assert exc_info.value.status_code == 409
    assert "stale" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# (e) Match pagination: _match_keys mints a token; api_cont resumes via keyset
# ---------------------------------------------------------------------------

def test_match_keys_mints_token_at_limit():
    ctx = _make_ctx()
    ctx.config.match_limit = 10
    fake_index = MagicMock()
    fake_index.name = "myindex"
    with patch("bioindex.api.bio.query.match", return_value=list(range(10))):
        result = bio._match_keys(ctx, fake_index, ["q1"], None,
                                 generation="gen-test-fixed", page=1)
    assert result["continuation"] is not None
    assert result["count"] == 10
    assert result["page"] == 1


def test_match_keys_no_token_below_limit():
    ctx = _make_ctx()
    ctx.config.match_limit = 100
    fake_index = MagicMock()
    fake_index.name = "myindex"
    with patch("bioindex.api.bio.query.match", return_value=["a", "b"]):
        result = bio._match_keys(ctx, fake_index, ["q1"], None,
                                 generation="gen-test-fixed", page=1)
    assert result["continuation"] is None


@pytest.mark.asyncio
async def test_api_cont_resumes_match_via_keyset_cursor():
    last_key = "key_002"
    ctx = _make_ctx(name="testportal")
    fake_index = MagicMock()
    fake_index.name = "myindex"
    ctx.indexes = {("myindex", 1): fake_index}

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
        portal_name="testportal",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())
    next_keys = ["key_003", "key_004"]

    req = _make_req(portal_ctx=ctx)
    with patch("bioindex.api.bio.query.match", return_value=next_keys) as mock_match:
        result = await bio.api_cont(token=token, req=req)

    data = orjson.loads(result.body)
    # query.match called as (config, engine, index, qs, after, page_size)
    assert mock_match.call_args.args[4] == last_key
    assert data["data"] == ["key_003", "key_004"]
    assert "key_002" not in data["data"]
    assert data["page"] == 2


# ---------------------------------------------------------------------------
# (f) limit in continuation token is the REMAINING budget, not original
# ---------------------------------------------------------------------------

def test_limit_is_decremented_into_continuation_token():
    ctx = _make_ctx()
    reader = _make_reader(records=[{"x": i} for i in range(4)], at_end=False,
                          limit=10, source_index=0, source_byte_offset=128)
    result = bio._fetch_records(ctx, reader, "myindex", ["q1"], "row",
                                generation="gen-test-fixed", page=1)
    token = result["continuation"]
    assert token is not None
    state = signed_tokens.decode(token, signed_tokens.signing_key())
    assert state.limit == 6   # 10 original - 4 returned this page


@pytest.mark.asyncio
async def test_match_limit_capped_across_continuation_pages(monkeypatch):
    """Regression: /match?limit=N caps TOTAL keys across pages."""
    ctx = _make_ctx(name="testportal")
    ctx.config.match_limit = 2
    all_keys = [f"k{i:02d}" for i in range(10)]

    def fake_match(config, engine, index, q, after=None, limit=None):
        ks = [k for k in all_keys if after is None or k > after]
        return ks[:limit] if limit is not None else ks

    monkeypatch.setattr(bio.query, "match", fake_match)
    fake_index = MagicMock()
    fake_index.name = "myindex"

    # page 1: limit=3 -> page_size=min(2,3)=2 -> ["k00","k01"], remaining=1
    page1 = bio._match_keys(ctx, fake_index, ["q1"], 3, generation="gen-test-fixed", page=1)
    assert page1["count"] == 2
    assert page1["data"] == ["k00", "k01"]
    token = page1["continuation"]
    assert token is not None
    assert signed_tokens.decode(token, signed_tokens.signing_key()).limit == 1

    # page 2 via /cont: budget 1, after="k01" -> page_size=min(2,1)=1 -> ["k02"]
    ctx.indexes = {("myindex", 1): fake_index}
    req = _make_req(portal_ctx=ctx)
    page2_resp = await bio.api_cont(token=token, req=req)
    page2 = orjson.loads(page2_resp.body)
    assert page2["count"] == 1
    assert page2["data"] == ["k02"]
    assert page2["continuation"] is None


# ---------------------------------------------------------------------------
# (g) Portal enforcement: token minted for portal "a" rejected under portal "b"
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cont_portal_enforcement():
    """Token minted with portal_name='a', resumed under ctx named 'b' → 400."""
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
        generation="gen-test-fixed",
        portal_name="a",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    ctx_b = _make_ctx(name="b")
    req = _make_req(portal_ctx=ctx_b)
    with pytest.raises(fastapi.HTTPException) as exc_info:
        await bio.api_cont(token=token, req=req)

    assert exc_info.value.status_code == 400
    assert "different portal" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_cont_empty_portal_name_rejected():
    """Token with portal_name='' must be rejected under any portal → 400."""
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
        generation="gen-test-fixed",
        portal_name="",
    )
    token = signed_tokens.encode(state, signed_tokens.signing_key())

    ctx = _make_ctx(name="anyportal")
    req = _make_req(portal_ctx=ctx)
    with pytest.raises(fastapi.HTTPException) as exc_info:
        await bio.api_cont(token=token, req=req)

    assert exc_info.value.status_code == 400
    assert "different portal" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# (h) 3-page resume-of-resume: match walks page1 -> page2 -> page3
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_match_three_page_resume_of_resume(monkeypatch):
    """
    Walk three match pages via api_cont. Verifies:
    - page-3 data is correct
    - page-2 continuation token decodes to a valid ContState
    - total items across all pages <= limit
    """
    ctx = _make_ctx(name="testportal")
    ctx.config.match_limit = 2  # force 2 keys per page
    all_keys = [f"k{i:02d}" for i in range(10)]

    def fake_match(config, engine, index, q, after=None, limit=None):
        ks = [k for k in all_keys if after is None or k > after]
        return ks[:limit] if limit is not None else ks

    monkeypatch.setattr(bio.query, "match", fake_match)
    fake_index = MagicMock()
    fake_index.name = "myindex"
    ctx.indexes = {("myindex", 1): fake_index}

    # page 1: limit=5, match_limit=2 -> page_size=2 -> ["k00","k01"], remaining=3
    page1 = bio._match_keys(ctx, fake_index, ["q1"], 5, generation="gen-test-fixed", page=1)
    assert page1["count"] == 2
    assert page1["data"] == ["k00", "k01"]
    token1 = page1["continuation"]
    assert token1 is not None

    # page 2 via api_cont: budget=3, after="k01" -> page_size=min(2,3)=2 -> ["k02","k03"], remaining=1
    req = _make_req(portal_ctx=ctx)
    page2_resp = await bio.api_cont(token=token1, req=req)
    page2 = orjson.loads(page2_resp.body)
    assert page2["count"] == 2
    assert page2["data"] == ["k02", "k03"]
    token2 = page2["continuation"]
    assert token2 is not None

    # verify page-2 token decodes to a valid ContState with correct remaining budget
    state2 = signed_tokens.decode(token2, signed_tokens.signing_key())
    assert isinstance(state2, ContState)
    assert state2.type == "match"
    assert state2.index_name == "myindex"
    assert state2.last_key == "k03"
    assert state2.limit == 1  # 5 - 2 - 2 = 1 remaining

    # page 3 via api_cont: budget=1, after="k03" -> page_size=min(2,1)=1 -> ["k04"]
    page3_resp = await bio.api_cont(token=token2, req=req)
    page3 = orjson.loads(page3_resp.body)
    assert page3["count"] == 1
    assert page3["data"] == ["k04"]
    assert page3["continuation"] is None

    # total across all pages <= limit=5
    total = page1["count"] + page2["count"] + page3["count"]
    assert total <= 5
    assert total == 5
