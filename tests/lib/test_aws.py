from unittest.mock import MagicMock

from bioindex.lib import aws


def test_look_up_var_id_returns_none_when_no_items(monkeypatch):
    fake = MagicMock()
    fake.Table.return_value.query.return_value = {"Items": []}
    monkeypatch.setattr(aws, "dynamo_client", fake)
    assert aws.look_up_var_id("rs999999", "tbl") is None


def test_look_up_var_id_returns_first_item(monkeypatch):
    fake = MagicMock()
    fake.Table.return_value.query.return_value = {"Items": [{"varId": "1:2:A:G"}]}
    monkeypatch.setattr(aws, "dynamo_client", fake)
    assert aws.look_up_var_id("rs1", "tbl") == {"varId": "1:2:A:G"}
