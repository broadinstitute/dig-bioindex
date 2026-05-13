import os

import pytest

from bioindex.lib.config import Config


@pytest.fixture(autouse=True)
def _no_aws(monkeypatch):
    """
    Stub out AWS calls so Config construction never reaches the network.

    Config.__init__ asserts truthiness of `self.rds_config`, which (when
    BIOINDEX_RDS_INSTANCE is set) calls describe_rds_instance under the hood.

    Also clear BIOINDEX_ENVIRONMENT so a developer's polluted shell can't
    drag the legacy-mode secret_lookup path into our tests.
    """
    monkeypatch.delenv("BIOINDEX_ENVIRONMENT", raising=False)

    def _stub_describe_rds_instance(name):
        return {
            "name": name,
            "engine": "mysql",
            "host": f"{name}.example.com",
            "port": 3306,
        }

    def _stub_secret_lookup(name):
        # belt-and-suspenders: return non-empty so even if legacy path runs,
        # the `assert secret` in Config.__init__ doesn't fire.
        return {
            "name": "stub",
            "engine": "mysql",
            "host": "stub.example.com",
            "port": 3306,
            "username": "u",
            "password": "p",
        }

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
    # has rds + schema, but missing s3 bucket -- should fail on the s3 assertion
    with pytest.raises((AssertionError, SystemExit)):
        Config.from_dict({
            "BIOINDEX_RDS_INSTANCE": "rds",
            "BIOINDEX_RDS_USERNAME": "u",
            "BIOINDEX_RDS_PASSWORD": "p",
            "BIOINDEX_BIO_SCHEMA": "schema",
        })
