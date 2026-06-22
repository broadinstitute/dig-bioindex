import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml

from .config import Config
from .portal_context import PortalContext


def _read_yaml(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    with open(path) as fp:
        return yaml.safe_load(fp) or {}


def load_portal_dicts(config_dir: Union[str, os.PathLike], env: str) -> List[Dict]:
    config_dir = Path(config_dir)
    env_defaults = _read_yaml(config_dir / "envs" / f"{env}.yaml") or {}

    portals_dir = config_dir / "portals"
    if not portals_dir.exists():
        return []

    result = []
    for portal_yaml in sorted(portals_dir.glob("*.yaml")):
        raw = _read_yaml(portal_yaml)
        if not raw:
            continue
        name = raw.get("name") or portal_yaml.stem
        env_block = (raw.get("envs") or {}).get(env)
        if env_block is None:
            logging.info("portal %s has no '%s' block; skipping in this env", name, env)
            continue
        if not isinstance(env_block, dict):
            logging.error(
                "portal %s: 'envs.%s' must be a mapping, got %s (in %s); skipping",
                name, env, type(env_block).__name__, portal_yaml,
            )
            continue
        merged = {**env_defaults, **env_block}
        result.append({"name": name, "env": merged})
    return result


def _build_engines(config):
    from .aws import connect_to_db
    bio = connect_to_db(**config.rds_config, schema=config.bio_schema)
    portal = None
    if config.portal_schema:
        portal = connect_to_db(**config.portal_rds_config, schema=config.portal_schema)
    return bio, portal


def _load_indexes(engine):
    from .index import Index
    indexes = Index.list_indexes(engine, filter_built=False)
    return {(i.name, int(i.schema.arity)): i for i in indexes}


def _load_gql_schema(config, engine):
    if not config.graphql_schema:
        return None
    from . import ql
    return ql.load_schema(config, engine, config.graphql_schema)


def build_portal_contexts(config_dir: Union[str, os.PathLike], env: str) -> List[PortalContext]:
    descriptors = load_portal_dicts(config_dir, env)
    contexts = []
    for desc in descriptors:
        cfg = Config.from_dict(desc["env"])
        bio_engine, portal_engine = _build_engines(cfg)
        indexes = _load_indexes(bio_engine)
        gql = _load_gql_schema(cfg, bio_engine)
        contexts.append(PortalContext(
            name=desc["name"], config=cfg, engine=bio_engine,
            portal=portal_engine, indexes=indexes, gql_schema=gql,
        ))
    return contexts
