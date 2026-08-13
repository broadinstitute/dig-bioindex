"""
The enrichr endpoint proxies a gene list to the public Enrichr service
and reshapes its row-array response into a list of dicts.  Both upstream
calls are mocked; no network.
"""
from unittest.mock import MagicMock, patch

import pytest

from bioindex.lib.portal_context import PortalContext
from bioindex.lib.portal_registry import init_registry

import bioindex.server as server

from fastapi.testclient import TestClient

client = TestClient(server.app)

_STUB_CTX = PortalContext(
    name="p",
    config=object(),
    engine=MagicMock(name="stub_engine"),
    indexes={},
)


@pytest.fixture(autouse=True)
def _registry():
    init_registry([_STUB_CTX])


def _upstream(add_list=None, enrich=None):
    """
    Mocked responses for the two Enrichr calls: POST /addList then
    GET /enrich.
    """
    add_resp = MagicMock(ok=True)
    add_resp.json.return_value = add_list if add_list is not None else {"userListId": 42}

    enrich_resp = MagicMock(ok=True)
    enrich_resp.json.return_value = enrich if enrich is not None else {}

    return add_resp, enrich_resp


def test_enrich_reshapes_rows_into_dicts():
    row = [1, "Insulin signaling", 0.001, 2.5, 12.3, ["INS", "IRS1"], 0.01, 0.002, 0.02]
    add_resp, enrich_resp = _upstream(enrich={"KEGG_2019": [row]})

    with patch("bioindex.api.enrichr.requests.post", return_value=add_resp) as post, \
            patch("bioindex.api.enrichr.requests.get", return_value=enrich_resp) as get:
        resp = client.post("/p/api/enrichr/enrichr", json={
            "gene_set_library": "KEGG_2019",
            "gene_list": ["INS", "IRS1"],
            "gene_list_desc": "test genes",
        })

    assert resp.status_code == 200
    assert resp.json() == [{
        "Rank": 1,
        "Term name": "Insulin signaling",
        "P-value": 0.001,
        "Odds ratio": 2.5,
        "Combined score": 12.3,
        "Overlapping genes": ["INS", "IRS1"],
        "Adjusted p-value": 0.01,
        "Old p-value": 0.002,
        "Old adjusted p-value": 0.02,
    }]

    # the gene list is newline-joined into Enrichr's addList form
    (add_url,), add_kwargs = post.call_args
    assert add_url.endswith("/addList")
    assert add_kwargs["files"]["list"] == "INS\nIRS1"
    assert add_kwargs["files"]["description"] == "test genes"
    assert add_kwargs["timeout"]

    # the returned list id and requested library select the enrichment
    (enrich_url,), enrich_kwargs = get.call_args
    assert "userListId=42" in enrich_url
    assert "backgroundType=KEGG_2019" in enrich_url
    assert enrich_kwargs["timeout"]


def test_failed_add_list_is_502():
    add_resp = MagicMock(ok=False)

    with patch("bioindex.api.enrichr.requests.post", return_value=add_resp):
        resp = client.post("/p/api/enrichr/enrichr", json={
            "gene_list": ["INS"],
            "gene_list_desc": "test genes",
        })

    assert resp.status_code == 502
    assert "gene list" in resp.json()["detail"]


def test_failed_enrich_is_502_and_library_defaults():
    add_resp, enrich_resp = _upstream()
    enrich_resp.ok = False

    with patch("bioindex.api.enrichr.requests.post", return_value=add_resp), \
            patch("bioindex.api.enrichr.requests.get", return_value=enrich_resp) as get:
        resp = client.post("/p/api/enrichr/enrichr", json={
            "gene_list": ["INS"],
            "gene_list_desc": "test genes",
        })

    assert resp.status_code == 502
    assert "enrichment" in resp.json()["detail"]

    # gene_set_library was omitted, so the default is requested upstream
    (enrich_url,), _ = get.call_args
    assert "backgroundType=KEGG_2015" in enrich_url
