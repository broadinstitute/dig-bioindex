import pytest

import bioindex.server as server


def test_init_registry_requires_env(monkeypatch):
    monkeypatch.delenv("BIOINDEX_ENV", raising=False)
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
