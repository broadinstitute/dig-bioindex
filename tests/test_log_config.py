import json
import logging
import logging.config

import pytest

from bioindex.log_config import LOGGING_CONFIG, JsonFormatter


def _format(exc_info=None, **extra):
    record = logging.LogRecord(
        name="bioindex.access", level=logging.INFO, pathname=__file__,
        lineno=1, msg="request", args=(), exc_info=exc_info,
    )
    for k, v in extra.items():
        setattr(record, k, v)

    return json.loads(JsonFormatter().format(record))


def test_base_fields_are_always_present():
    out = _format()
    assert out["level"] == "INFO"
    assert out["logger"] == "bioindex.access"
    assert out["msg"] == "request"
    assert out["time"]


def test_access_fields_are_carried_through():
    out = _format(portal="p", status=200, latency_ms=12, client_ip="203.0.113.0",
                  user_agent="curl/8.0", response_bytes=41)
    assert out["portal"] == "p"
    assert out["status"] == 200
    assert out["latency_ms"] == 12
    assert out["client_ip"] == "203.0.113.0"
    assert out["user_agent"] == "curl/8.0"
    assert out["response_bytes"] == 41


def test_unset_access_fields_are_omitted_rather_than_nulled():
    assert "portal" not in _format()


def test_traceback_is_folded_into_the_object():
    try:
        raise RuntimeError("kaboom")
    except RuntimeError:
        import sys
        out = _format(exc_info=sys.exc_info())

    assert "kaboom" in out["exc"]
    # a traceback spans lines; the record must not
    assert "\n" not in json.dumps(out)


def test_config_is_usable_by_dictconfig():
    logging.config.dictConfig(LOGGING_CONFIG)

    assert isinstance(logging.getLogger().handlers[0].formatter, JsonFormatter)
    assert logging.getLogger("uvicorn.access").level == logging.WARNING


@pytest.fixture(autouse=True)
def _restore_logging():
    yield
    logging.config.dictConfig({"version": 1, "disable_existing_loggers": False})
