import pytest
from pathlib import Path

from bioindex.lib.portal_context import PortalContext


def _make_configs(tmp_path: Path, portal_yaml: str, env_yaml: str, env: str = "qa"):
    (tmp_path / "portals").mkdir()
    (tmp_path / "envs").mkdir()
    (tmp_path / "portals" / "p.yaml").write_text(portal_yaml)
    (tmp_path / "envs" / f"{env}.yaml").write_text(env_yaml)
    return tmp_path


# ── load_portal_dicts ─────────────────────────────────────────────────────────

def test_load_portal_dicts_merges_env(tmp_path):
    from bioindex.lib.portal_loader import load_portal_dicts

    cfg = _make_configs(
        tmp_path,
        portal_yaml=(
            "name: myportal\n"
            "envs:\n"
            "  qa:\n"
            "    BIOINDEX_S3_BUCKET: portal-bucket\n"
        ),
        env_yaml="BIOINDEX_RESPONSE_LIMIT: 1048576\n",
    )

    result = load_portal_dicts(cfg, "qa")
    assert len(result) == 1
    d = result[0]
    assert d["name"] == "myportal"
    assert d["env"]["BIOINDEX_S3_BUCKET"] == "portal-bucket"
    assert d["env"]["BIOINDEX_RESPONSE_LIMIT"] == 1048576


def test_load_portal_dicts_portal_overrides_env(tmp_path):
    from bioindex.lib.portal_loader import load_portal_dicts

    cfg = _make_configs(
        tmp_path,
        portal_yaml=(
            "name: override\n"
            "envs:\n"
            "  qa:\n"
            "    BIOINDEX_RESPONSE_LIMIT: 9999\n"
        ),
        env_yaml="BIOINDEX_RESPONSE_LIMIT: 1048576\n",
    )

    result = load_portal_dicts(cfg, "qa")
    assert result[0]["env"]["BIOINDEX_RESPONSE_LIMIT"] == 9999


def test_load_portal_dicts_skips_missing_env(tmp_path):
    from bioindex.lib.portal_loader import load_portal_dicts

    cfg = _make_configs(
        tmp_path,
        portal_yaml=(
            "name: prod-only\n"
            "envs:\n"
            "  prod:\n"
            "    BIOINDEX_S3_BUCKET: prod-bucket\n"
        ),
        env_yaml="BIOINDEX_RESPONSE_LIMIT: 1048576\n",
    )

    result = load_portal_dicts(cfg, "qa")
    assert result == []


def test_load_portal_dicts_name_from_stem(tmp_path):
    from bioindex.lib.portal_loader import load_portal_dicts

    (tmp_path / "portals").mkdir()
    (tmp_path / "envs").mkdir()
    (tmp_path / "portals" / "stemname.yaml").write_text(
        "envs:\n  qa:\n    BIOINDEX_S3_BUCKET: b\n"
    )
    (tmp_path / "envs" / "qa.yaml").write_text("")

    result = load_portal_dicts(tmp_path, "qa")
    assert result[0]["name"] == "stemname"


def test_load_portal_dicts_empty_portals_dir(tmp_path):
    from bioindex.lib.portal_loader import load_portal_dicts

    (tmp_path / "portals").mkdir()
    (tmp_path / "envs").mkdir()
    (tmp_path / "envs" / "qa.yaml").write_text("")

    assert load_portal_dicts(tmp_path, "qa") == []


def test_load_portal_dicts_missing_portals_dir(tmp_path):
    from bioindex.lib.portal_loader import load_portal_dicts

    (tmp_path / "envs").mkdir()
    (tmp_path / "envs" / "qa.yaml").write_text("")

    assert load_portal_dicts(tmp_path, "qa") == []


# ── build_portal_contexts ─────────────────────────────────────────────────────

def test_build_portal_contexts_returns_portal_context(tmp_path, monkeypatch):
    import bioindex.lib.portal_loader as pl
    from bioindex.lib import config as config_mod

    cfg_dir = _make_configs(
        tmp_path,
        portal_yaml=(
            "name: mportal\n"
            "envs:\n"
            "  qa:\n"
            "    BIOINDEX_S3_BUCKET: bucket\n"
            "    BIOINDEX_BIO_SCHEMA: bio\n"
        ),
        env_yaml="",
    )

    fake_config = object()
    fake_bio = object()
    fake_portal = None
    fake_indexes = {"idx": object()}
    fake_gql = None

    monkeypatch.setattr(config_mod.Config, "from_dict", staticmethod(lambda d: fake_config))
    monkeypatch.setattr(pl, "_build_engines", lambda c: (fake_bio, fake_portal))
    monkeypatch.setattr(pl, "_load_indexes", lambda e: fake_indexes)
    monkeypatch.setattr(pl, "_load_gql_schema", lambda c, e: fake_gql)

    result = pl.build_portal_contexts(cfg_dir, "qa")

    assert len(result) == 1
    ctx = result[0]
    assert isinstance(ctx, PortalContext)
    assert ctx.name == "mportal"
    assert ctx.config is fake_config
    assert ctx.engine is fake_bio
    assert ctx.indexes is fake_indexes
    assert ctx.portal is None
    assert ctx.gql_schema is None
