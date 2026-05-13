import textwrap
from unittest.mock import patch

from bioindex.lib.portal_loader import build_portal_contexts


def test_build_portal_contexts_returns_one_per_portal(tmp_path):
    (tmp_path / "portals").mkdir()
    (tmp_path / "portals" / "p.yaml").write_text(textwrap.dedent("""
        name: p
        envs:
          qa:
            BIOINDEX_S3_BUCKET: b
            BIOINDEX_RDS_INSTANCE: r
            BIOINDEX_RDS_USERNAME: u
            BIOINDEX_RDS_PASSWORD: p
            BIOINDEX_BIO_SCHEMA: s
    """))

    # Patch the expensive bits: engine creation, index loading, gql schema.
    with patch("bioindex.lib.portal_loader._build_engines") as build_e, \
         patch("bioindex.lib.portal_loader._load_indexes") as load_i, \
         patch("bioindex.lib.portal_loader._load_gql_schema") as load_g, \
         patch("bioindex.lib.config.describe_rds_instance") as desc_rds:
        build_e.return_value = (object(), None)
        load_i.return_value = {}
        load_g.return_value = None
        desc_rds.return_value = {
            "name": "r", "engine": "mysql",
            "host": "r.example.com", "port": 3306,
        }

        ctxs = build_portal_contexts(tmp_path, env="qa")

    assert len(ctxs) == 1
    assert ctxs[0].name == "p"
    assert ctxs[0].config.s3_bucket == "b"
