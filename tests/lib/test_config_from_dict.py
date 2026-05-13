import pytest

from bioindex.lib.config import Config


@pytest.fixture(autouse=True)
def _no_aws(monkeypatch):
    """
    Stub out AWS calls so Config construction never reaches the network.

    Config.__init__ asserts truthiness of `self.rds_config`, which (when
    BIOINDEX_RDS_INSTANCE is set) calls describe_rds_instance under the hood.
    """
    def _stub_describe_rds_instance(name):
        return {
            "name": name,
            "engine": "mysql",
            "host": f"{name}.example.com",
            "port": 3306,
        }

    def _stub_secret_lookup(name):
        return {}

    monkeypatch.setattr(
        "bioindex.lib.config.describe_rds_instance",
        _stub_describe_rds_instance,
    )
    monkeypatch.setattr(
        "bioindex.lib.config.secret_lookup",
        _stub_secret_lookup,
    )


def test_from_dict_builds_config_without_touching_os_environ(
    sample_portal_dict, monkeypatch
):
    monkeypatch.delenv("BIOINDEX_S3_BUCKET", raising=False)
    cfg = Config.from_dict(sample_portal_dict)
    assert cfg.s3_bucket == "test-bucket"

    import os
    assert "BIOINDEX_S3_BUCKET" not in os.environ


def test_from_dict_two_configs_do_not_leak_into_each_other(monkeypatch):
    monkeypatch.delenv("BIOINDEX_S3_BUCKET", raising=False)
    a = Config.from_dict({
        "BIOINDEX_S3_BUCKET": "a-bucket",
        "BIOINDEX_RDS_INSTANCE": "a-rds",
        "BIOINDEX_RDS_USERNAME": "u",
        "BIOINDEX_RDS_PASSWORD": "p",
        "BIOINDEX_BIO_SCHEMA": "a",
    })
    b = Config.from_dict({
        "BIOINDEX_S3_BUCKET": "b-bucket",
        "BIOINDEX_RDS_INSTANCE": "b-rds",
        "BIOINDEX_RDS_USERNAME": "u",
        "BIOINDEX_RDS_PASSWORD": "p",
        "BIOINDEX_BIO_SCHEMA": "b",
    })
    assert a.s3_bucket == "a-bucket"
    assert b.s3_bucket == "b-bucket"


def test_from_dict_missing_required_fields_raises(monkeypatch):
    monkeypatch.delenv("BIOINDEX_S3_BUCKET", raising=False)
    monkeypatch.delenv("BIOINDEX_RDS_INSTANCE", raising=False)
    monkeypatch.delenv("BIOINDEX_RDS_SECRET", raising=False)
    monkeypatch.delenv("BIOINDEX_BIO_SCHEMA", raising=False)

    # missing s3 bucket
    with pytest.raises((AssertionError, SystemExit)):
        Config.from_dict({"BIOINDEX_BIO_SCHEMA": "x"})
