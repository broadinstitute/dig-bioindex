import logging
from pathlib import Path
from typing import Dict, List, Optional

import yaml


def _read_yaml(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    with open(path) as fp:
        return yaml.safe_load(fp) or {}


def load_portal_dicts(config_dir, env: str) -> List[Dict]:
    """
    Walk a configs directory and return a list of portal descriptor dicts
    for the given env. Each dict has shape:

        {"name": <portal name>, "env": {<merged env-var dict>}}

    Portals lacking an entry for `env` are silently skipped.
    """
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
            logging.info(
                "portal %s has no '%s' block; skipping in this env",
                name, env,
            )
            continue
        merged = {**env_defaults, **env_block}
        result.append({"name": name, "env": merged})
    return result
