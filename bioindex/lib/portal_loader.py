import logging
from pathlib import Path

import yaml

from .aws import connect_to_db
from .config import Config
from .index import Index
from .portal_context import PortalContext
from . import ql


def _read_yaml(path):
    """
    Parse a yaml file, or None if it doesn't exist.
    """
    if not path.exists():
        return None

    with open(path) as fp:
        return yaml.safe_load(fp) or {}


def load_portal_dicts(config_dir, env):
    """
    Read <config_dir>/portals/*.yaml and merge each portal's `envs.<env>`
    block over the defaults in <config_dir>/envs/<env>.yaml. Portals with
    no block for this environment are skipped.
    """
    config_dir = Path(config_dir)
    env_defaults = _read_yaml(config_dir / 'envs' / f'{env}.yaml') or {}

    portals_dir = config_dir / 'portals'
    if not portals_dir.exists():
        return []

    portals = []
    for portal_yaml in sorted(portals_dir.glob('*.yaml')):
        raw = _read_yaml(portal_yaml)
        if not raw:
            continue

        name = raw.get('name') or portal_yaml.stem
        env_block = (raw.get('envs') or {}).get(env)

        if env_block is None:
            logging.info("portal %s has no '%s' block; skipping in this env", name, env)
            continue
        if not isinstance(env_block, dict):
            logging.error("portal %s: 'envs.%s' must be a mapping, got %s; skipping",
                          name, env, type(env_block).__name__)
            continue

        portals.append({'name': name, 'env': {**env_defaults, **env_block}})

    return portals


def build_portal_context(config, name):
    """
    Connect a single portal to its databases and cache its indexes.
    """
    # connect to the index schema, and optionally the portal/metadata schema
    engine = connect_to_db(**config.rds_config, schema=config.bio_schema)
    portal_engine = None
    if config.portal_schema:
        portal_engine = connect_to_db(**config.portal_rds_config, schema=config.portal_schema)

    indexes = Index.list_indexes(engine, filter_built=False)
    gql_schema = None
    if config.graphql_schema:
        gql_schema = ql.load_schema(config, engine, config.graphql_schema)

    return PortalContext(
        name=name,
        config=config,
        engine=engine,
        indexes=dict(((i.name, int(i.schema.arity)), i) for i in indexes),
        portal=portal_engine,
        gql_schema=gql_schema,
    )


def build_portal_contexts(config_dir, env):
    """
    Load every portal defined for `env` and connect it to its databases.
    """
    return [build_portal_context(Config.from_dict(portal['env']), portal['name'])
            for portal in load_portal_dicts(config_dir, env)]
