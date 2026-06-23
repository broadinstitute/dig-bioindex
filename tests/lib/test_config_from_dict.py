import pytest
from unittest.mock import patch
from bioindex.lib.config import Config

MINIMAL = {
    'BIOINDEX_S3_BUCKET': 'test-bucket',
    'BIOINDEX_RDS_INSTANCE': 'test-instance',
    'BIOINDEX_RDS_USERNAME': 'user',
    'BIOINDEX_RDS_PASSWORD': 'pass',
    'BIOINDEX_BIO_SCHEMA': 'testbio',
    'BIOINDEX_RESPONSE_LIMIT': '512',
    'BIOINDEX_MATCH_LIMIT': '42',
}


def _make(extra=None):
    d = dict(MINIMAL)
    if extra:
        d.update(extra)
    with patch('bioindex.lib.config.describe_rds_instance', return_value={'host': 'h', 'port': 3306, 'name': 'test-instance'}):
        return Config.from_dict(d)


def test_from_dict_s3_bucket():
    cfg = _make()
    assert cfg.s3_bucket == 'test-bucket'


def test_from_dict_bio_schema():
    cfg = _make()
    assert cfg.bio_schema == 'testbio'


def test_from_dict_response_limit():
    cfg = _make()
    assert cfg.response_limit == 512


def test_from_dict_match_limit():
    cfg = _make()
    assert cfg.match_limit == 42


def test_from_dict_does_not_read_environ(monkeypatch):
    monkeypatch.delenv('BIOINDEX_S3_BUCKET', raising=False)
    monkeypatch.delenv('BIOINDEX_BIO_SCHEMA', raising=False)
    monkeypatch.delenv('BIOINDEX_MATCH_LIMIT', raising=False)
    cfg = _make()
    assert cfg.s3_bucket == 'test-bucket'
    assert cfg.bio_schema == 'testbio'
    assert cfg.match_limit == 42


def test_from_dict_sentinel_not_environ(monkeypatch):
    monkeypatch.setenv('BIOINDEX_S3_BUCKET', 'env-bucket')
    cfg = _make()
    assert cfg.s3_bucket == 'test-bucket'


def test_from_dict_omitted_key_does_not_bleed_env(monkeypatch):
    monkeypatch.setenv('BIOINDEX_BIO_SCHEMA', 'env-leak')
    d = dict(MINIMAL)
    del d['BIOINDEX_BIO_SCHEMA']
    with patch('bioindex.lib.config.describe_rds_instance', return_value={'host': 'h', 'port': 3306, 'name': 'test-instance'}):
        cfg = Config.from_dict(d)
    assert cfg.bio_schema == 'bio'
