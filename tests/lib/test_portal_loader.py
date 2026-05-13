import textwrap
import pytest

from bioindex.lib.portal_loader import load_portal_dicts


@pytest.fixture
def configs_dir(tmp_path):
    (tmp_path / "envs").mkdir()
    (tmp_path / "portals").mkdir()
    (tmp_path / "envs" / "qa.yaml").write_text(textwrap.dedent("""
        BIOINDEX_RESPONSE_LIMIT_MAX: 50000000
    """))
    (tmp_path / "envs" / "prod.yaml").write_text(textwrap.dedent("""
        BIOINDEX_RESPONSE_LIMIT_MAX: 100000000
    """))
    (tmp_path / "portals" / "cfde.yaml").write_text(textwrap.dedent("""
        name: cfde
        envs:
          qa:
            BIOINDEX_S3_BUCKET: cfde-qa
            BIOINDEX_RDS_SECRET: cfde-qa-secret
            BIOINDEX_BIO_SCHEMA: cfde_qa
          prod:
            BIOINDEX_S3_BUCKET: cfde
            BIOINDEX_RDS_SECRET: cfde-prod-secret
            BIOINDEX_BIO_SCHEMA: cfde
    """))
    (tmp_path / "portals" / "qa_only.yaml").write_text(textwrap.dedent("""
        name: qa_only
        envs:
          qa:
            BIOINDEX_S3_BUCKET: qa-only-bucket
            BIOINDEX_RDS_SECRET: qa-only-secret
            BIOINDEX_BIO_SCHEMA: qa_only_schema
    """))
    return tmp_path


def test_load_returns_one_dict_per_portal_for_env(configs_dir):
    dicts = load_portal_dicts(configs_dir, env="prod")
    names = {d["name"] for d in dicts}
    assert names == {"cfde"}  # qa_only is absent in prod


def test_load_qa_includes_qa_only_portal(configs_dir):
    dicts = load_portal_dicts(configs_dir, env="qa")
    names = {d["name"] for d in dicts}
    assert names == {"cfde", "qa_only"}


def test_load_merges_env_defaults_with_portal_overrides(configs_dir):
    dicts = load_portal_dicts(configs_dir, env="prod")
    cfde = next(d for d in dicts if d["name"] == "cfde")
    assert cfde["env"]["BIOINDEX_RESPONSE_LIMIT_MAX"] == 100000000
    assert cfde["env"]["BIOINDEX_S3_BUCKET"] == "cfde"


def test_load_unknown_env_returns_empty(configs_dir):
    dicts = load_portal_dicts(configs_dir, env="staging")
    assert dicts == []


def test_load_missing_envs_dir_is_ok(tmp_path):
    (tmp_path / "portals").mkdir()
    (tmp_path / "portals" / "x.yaml").write_text(textwrap.dedent("""
        name: x
        envs:
          qa:
            BIOINDEX_S3_BUCKET: b
            BIOINDEX_RDS_SECRET: s
            BIOINDEX_BIO_SCHEMA: x
    """))
    dicts = load_portal_dicts(tmp_path, env="qa")
    assert len(dicts) == 1
