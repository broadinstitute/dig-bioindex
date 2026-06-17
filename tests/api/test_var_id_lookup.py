from unittest.mock import MagicMock, patch

import fastapi
import pytest

from bioindex.api import bio


async def test_lookup_unknown_rsid_raises_404():
    req = MagicMock()
    with patch.object(bio, "get_portal_ctx") as gpc, \
         patch.object(bio.aws, "look_up_var_id", return_value=None):
        gpc.return_value.config.variant_dynamodb_table = "tbl"
        with pytest.raises(fastapi.HTTPException) as ei:
            await bio.api_lookup_variant_for_rs_id("rs999999", req)
    assert ei.value.status_code == 404


async def test_lookup_known_rsid_returns_data():
    req = MagicMock()
    with patch.object(bio, "get_portal_ctx") as gpc, \
         patch.object(bio.aws, "look_up_var_id", return_value={"varId": "1:2:A:G"}):
        gpc.return_value.config.variant_dynamodb_table = "tbl"
        result = await bio.api_lookup_variant_for_rs_id("rs1", req)
    assert result["data"] == {"varId": "1:2:A:G"}
    assert result["q"] == "rs1"
