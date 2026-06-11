"""
Uvicorn / Python logging config that emits structured JSON to stdout.
Used in place of the rotating-file access log from bioindex/main.py.
"""
import json
import logging
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for attr in (
            "portal", "request_id",
            "method", "route", "path", "query",
            "status", "response_bytes",
            "latency_ms", "worker_pid", "client_ip",
        ):
            if hasattr(record, attr):
                payload[attr] = getattr(record, attr)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {"()": "bioindex.log_config.JsonFormatter"},
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "json",
        },
    },
    "root": {"level": "INFO", "handlers": ["stdout"]},
    "loggers": {
        "uvicorn":         {"handlers": ["stdout"], "level": "INFO", "propagate": False},
        "uvicorn.access":  {"handlers": ["stdout"], "level": "WARNING", "propagate": False},
        "uvicorn.error":   {"handlers": ["stdout"], "level": "INFO", "propagate": False},
    },
}
