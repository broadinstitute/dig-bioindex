import json
import logging

from bioindex.log_config import JsonFormatter


def _record(**extra):
    rec = logging.LogRecord(
        name="bioindex.access", level=logging.INFO, pathname=__file__,
        lineno=1, msg="request", args=(), exc_info=None,
    )
    for k, v in extra.items():
        setattr(rec, k, v)
    return rec


def test_json_formatter_serializes_client_ip():
    out = json.loads(JsonFormatter().format(_record(client_ip="203.0.113.0")))
    assert out.get("client_ip") == "203.0.113.0"


def test_json_formatter_serializes_user_agent():
    out = json.loads(JsonFormatter().format(_record(user_agent="curl/8.0")))
    assert out.get("user_agent") == "curl/8.0"
