import os
from unittest.mock import MagicMock

os.environ.setdefault("BIOINDEX_S3_BUCKET", "test-bucket")
os.environ.setdefault("BIOINDEX_BIO_SCHEMA", "test_schema")
os.environ.setdefault("BIOINDEX_RDS_INSTANCE", "test-rds")
os.environ.setdefault("BIOINDEX_RDS_USERNAME", "u")
os.environ.setdefault("BIOINDEX_RDS_PASSWORD", "p")
os.environ.setdefault("BIOINDEX_TOKEN_SIGNING_KEY", "00" * 32)  # 32 bytes (hex)

import bioindex.lib.config as _config
_config.describe_rds_instance = lambda name: {"host": "localhost", "port": 3306, "name": name}

import bioindex.api.utils as _utils
_utils.connect_to_bio = lambda config: MagicMock(name="bio_engine")
_utils.connect_to_portal = lambda config: None

import bioindex.lib.index as _index
_index.Index.list_indexes = staticmethod(lambda *a, **k: [])
