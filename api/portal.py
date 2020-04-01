import fastapi
import json

import lib.config
import lib.secrets

from lib.profile import profile


# load dot files and configuration
config = lib.config.Config()

# create flask app; this will load .env
router = fastapi.APIRouter()

# connect to database
engine = lib.secrets.connect_to_mysql(config.rds_instance, schema='portal')


@router.get('/groups')
async def api_portal_groups():
    """
    Returns the list of portals available.
    """
    sql = (
        'SELECT DISTINCT `name`, `description`, `default`, `docs` FROM DiseaseGroups'
    )

    # run the query
    resp, query_s = profile(engine.execute, sql)
    disease_groups = []

    # transform response
    for name, desc, default, docs in resp:
        disease_groups.append({
            'name': name,
            'default': default != 0,
            'description': desc,
            'docs': docs,
        })

    return {
        'profile': {
            'query': query_s,
        },
        'data': disease_groups,
        'count': len(disease_groups),
    }


@router.get('/phenotypes')
async def api_group_phenotypes(q: str = None):
    """
    Returns all available phenotypes or just those for a given
    portal group.
    """
    sql = (
        'SELECT '
        '   Phenotypes.`name`, '
        '   Phenotypes.`description`, '
        '   Phenotypes.`group`, '
        '   Phenotypes.`dichotomous` '
        'FROM Phenotypes'
    )

    # optional filter by portal
    if q and q == '':
        q = None

    # update query for just the portal
    if q is not None:
        sql += (
            ', DiseaseGroups WHERE DiseaseGroups.`name` = %s AND ('
            '   DiseaseGroups.`groups` IS NULL OR FIND_IN_SET(Phenotypes.`group`, DiseaseGroups.`groups`) '
            ')'
        )

    # run the query
    resp, query_s = profile(engine.execute, sql, q) if q else profile(engine.execute, sql)
    phenotypes = []

    # transform response
    for name, desc, group, dichotomous in resp:
        phenotypes.append({
            'name': name,
            'description': desc,
            'group': group,
            'dichotomous': dichotomous,
        })

    return {
        'profile': {
            'query': query_s,
        },
        'data': phenotypes,
        'count': len(phenotypes),
    }
