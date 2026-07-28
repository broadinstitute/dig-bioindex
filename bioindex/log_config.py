import json
import logging

# fields the access middleware attaches to its records; anything else the
# app logs still comes through, just without them
ACCESS_FIELDS = (
    'portal',
    'request_id',
    'method',
    'route',
    'path',
    'query',
    'status',
    'response_bytes',
    'latency_ms',
    'worker_pid',
    'client_ip',
    'user_agent',
)


class JsonFormatter(logging.Formatter):
    """
    One JSON object per line, which is what CloudWatch wants to see.
    """

    def format(self, record):
        payload = {
            'time': self.formatTime(record, '%Y-%m-%dT%H:%M:%S'),
            'level': record.levelname,
            'logger': record.name,
            'msg': record.getMessage(),
        }

        for field in ACCESS_FIELDS:
            if hasattr(record, field):
                payload[field] = getattr(record, field)

        if record.exc_info:
            payload['exc'] = self.formatException(record.exc_info)

        return json.dumps(payload)


# uvicorn's own access log is off (the middleware emits the canonical one),
# and existing loggers stay enabled so bioindex.access survives dictConfig
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'json': {'()': 'bioindex.log_config.JsonFormatter'},
    },
    'handlers': {
        'stdout': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'formatter': 'json',
        },
    },
    'root': {'level': 'INFO', 'handlers': ['stdout']},
    'loggers': {
        'uvicorn': {'level': 'INFO', 'handlers': ['stdout'], 'propagate': False},
        'uvicorn.error': {'level': 'INFO', 'handlers': ['stdout'], 'propagate': False},
        'uvicorn.access': {'level': 'WARNING', 'handlers': ['stdout'], 'propagate': False},
    },
}
