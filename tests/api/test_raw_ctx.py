"""
Tests that raw.py and portal.py handlers resolve ctx via get_portal_ctx
and serve against it (engine/s3/portal mocked).
"""
import types
from io import BytesIO
from unittest.mock import MagicMock, patch

import fastapi
import pytest

import bioindex.api.raw as raw
import bioindex.api.portal as portal_api


# ---------------------------------------------------------------------------
# Helpers (mirror test_continuation.py style)
# ---------------------------------------------------------------------------

def _make_ctx(name="testportal", portal_engine=None):
    cfg = types.SimpleNamespace(
        s3_bucket="test-bucket",
        s3_path=lambda p: f"prefix/{p}",
        match_limit=100,
        response_limit=10_000_000,
        response_limit_max=20_000_000,
    )
    ctx = types.SimpleNamespace(
        name=name,
        config=cfg,
        engine=MagicMock(name="engine"),
        portal=portal_engine,
        indexes={},
        gql_schema=None,
    )
    return ctx


def _make_req(portal_ctx):
    req = MagicMock()
    req.state.portal_ctx = portal_ctx
    return req


# ---------------------------------------------------------------------------
# raw.py — plot/file handlers resolve ctx
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_raw_file_uses_ctx_config():
    ctx = _make_ctx()
    req = _make_req(ctx)
    fake_body = b"binary-data"

    with patch("bioindex.api.raw.s3.read_object", return_value=BytesIO(fake_body)) as mock_s3:
        resp = await raw.api_raw_file(file="some/file.bin", req=req)

    mock_s3.assert_called_once_with("test-bucket", "prefix/raw/some/file.bin")
    assert resp.body == fake_body


@pytest.mark.asyncio
async def test_raw_file_404_when_s3_returns_none():
    ctx = _make_ctx()
    req = _make_req(ctx)

    with patch("bioindex.api.raw.s3.read_object", return_value=None):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await raw.api_raw_file(file="missing.png", req=req)

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_raw_plot_dataset_401_when_permission_denied():
    ctx = _make_ctx()
    req = _make_req(ctx)

    with patch("bioindex.api.raw.verify_permissions", return_value=False):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await raw.api_raw_plot_dataset(dataset="ds1", file="plot.png", req=req)

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_raw_plot_dataset_serves_image():
    ctx = _make_ctx()
    req = _make_req(ctx)
    fake_body = b"\x89PNG\r\n"

    with patch("bioindex.api.raw.verify_permissions", return_value=True), \
         patch("bioindex.api.raw.s3.read_object", return_value=BytesIO(fake_body)):
        resp = await raw.api_raw_plot_dataset(dataset="ds1", file="plot.png", req=req)

    assert resp.media_type == "image/png"
    assert resp.body == fake_body


@pytest.mark.asyncio
async def test_raw_plot_phenotype_uses_ctx_portal():
    ctx = _make_ctx(portal_engine=MagicMock(name="portal_engine"))
    req = _make_req(ctx)

    with patch("bioindex.api.raw.verify_permissions", return_value=False) as mock_vp:
        with pytest.raises(fastapi.HTTPException):
            await raw.api_raw_plot_phenotype(phenotype="T2D", file="plot.png", req=req)

    # verify_permissions was called with ctx.portal
    mock_vp.assert_called_once_with(ctx.portal, req, phenotype="T2D")


# ---------------------------------------------------------------------------
# portal.py — handlers resolve ctx and raise 501 when portal is None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_portal_groups_501_when_no_portal():
    ctx = _make_ctx(portal_engine=None)
    req = _make_req(ctx)

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await portal_api.api_portal_groups(req=req)

    assert exc_info.value.status_code == 501


@pytest.mark.asyncio
async def test_portal_restrictions_501_when_no_portal():
    ctx = _make_ctx(portal_engine=None)
    req = _make_req(ctx)

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await portal_api.api_portal_restrictions(req=req)

    assert exc_info.value.status_code == 501


@pytest.mark.asyncio
async def test_portal_groups_uses_ctx_portal():
    mock_portal = MagicMock(name="portal_engine")
    mock_conn = MagicMock()
    mock_conn.execute.return_value = iter([])
    mock_portal.connect.return_value.__enter__ = lambda s: mock_conn
    mock_portal.connect.return_value.__exit__ = MagicMock(return_value=False)

    ctx = _make_ctx(portal_engine=mock_portal)
    req = _make_req(ctx)

    import orjson
    resp = await portal_api.api_portal_groups(req=req)
    body = orjson.loads(resp.body)

    assert "data" in body
    assert "nonce" in body
    assert resp.headers.get("cache-control") == "no-store"


@pytest.mark.asyncio
async def test_portal_phenotypes_501_when_no_portal():
    ctx = _make_ctx(portal_engine=None)
    req = _make_req(ctx)

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await portal_api.api_portal_phenotypes(req=req)

    assert exc_info.value.status_code == 501
