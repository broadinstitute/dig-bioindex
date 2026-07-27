import types
from unittest.mock import patch

import pytest

import bioindex.server as server


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("BIOINDEX_CONFIG_DIR", raising=False)
    monkeypatch.delenv("BIOINDEX_ENV", raising=False)
    monkeypatch.delenv("BIOINDEX_PORTAL_NAME", raising=False)


# ---------------------------------------------------------------------------
# multi-portal: BIOINDEX_CONFIG_DIR is set
# ---------------------------------------------------------------------------

def test_init_registry_requires_env(monkeypatch, tmp_path):
    monkeypatch.setenv("BIOINDEX_CONFIG_DIR", str(tmp_path))
    with pytest.raises(RuntimeError, match="BIOINDEX_ENV"):
        server._init_registry_from_env()


def test_init_registry_rejects_missing_config_dir(monkeypatch):
    monkeypatch.setenv("BIOINDEX_ENV", "qa")
    monkeypatch.setenv("BIOINDEX_CONFIG_DIR", "/no/such/bioindex/dir")
    with pytest.raises(RuntimeError, match="BIOINDEX_CONFIG_DIR"):
        server._init_registry_from_env()


def test_init_registry_fails_loudly_on_zero_portals(monkeypatch, tmp_path):
    monkeypatch.setenv("BIOINDEX_ENV", "qa")
    monkeypatch.setenv("BIOINDEX_CONFIG_DIR", str(tmp_path))
    with pytest.raises(RuntimeError, match="[Nn]o portals"):
        server._init_registry_from_env()


# ---------------------------------------------------------------------------
# single portal from the environment: no BIOINDEX_CONFIG_DIR (--env-file)
# ---------------------------------------------------------------------------

def _fake_ctx(config, name):
    return types.SimpleNamespace(name=name, config=config, engine=object(),
                                 indexes={}, portal=None, gql_schema=None)


@pytest.fixture
def env_file_settings(monkeypatch):
    """The settings a local .bioindex would put into the environment."""
    monkeypatch.setenv("BIOINDEX_S3_BUCKET", "from-env-file")
    monkeypatch.setenv("BIOINDEX_BIO_SCHEMA", "bio")
    monkeypatch.setenv("BIOINDEX_RDS_INSTANCE", "local-rds")
    monkeypatch.setenv("BIOINDEX_RDS_USERNAME", "u")
    monkeypatch.setenv("BIOINDEX_RDS_PASSWORD", "p")
    monkeypatch.setattr("bioindex.lib.config.describe_rds_instance",
                        lambda name: {"host": "h", "port": 3306, "name": name})


def test_no_config_dir_serves_one_portal_from_the_environment(env_file_settings):
    with patch("bioindex.server.build_portal_context", _fake_ctx):
        server._init_registry_from_env()

    registry = server.get_registry()
    assert registry.names() == ["local"]
    # configured from the process environment, not from yaml
    assert registry.get("local").config.s3_bucket == "from-env-file"


def test_portal_name_is_overridable(env_file_settings, monkeypatch):
    monkeypatch.setenv("BIOINDEX_PORTAL_NAME", "amp")

    with patch("bioindex.server.build_portal_context", _fake_ctx):
        server._init_registry_from_env()

    assert server.get_registry().names() == ["amp"]
