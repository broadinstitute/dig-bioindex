import fastapi

import lib.config
import lib.secrets

from lib.profile import profile
from lib.utils import nonce


# load dot files and configuration
config = lib.config.Config()

# create web server
router = fastapi.APIRouter()

# connect to database
engine = lib.secrets.connect_to_mysql(config.rds_instance, schema=config.portal_schema)


@router.get('/groups', response_class=fastapi.responses.ORJSONResponse)
async def api_portal_groups():
    """
    Returns the list of portals available.
    """
    sql = 'SELECT `name`, `description`, `default`, `memberCMD` FROM DiseaseGroups'

    # run the query
    resp, query_s = profile(engine.execute, sql)
    disease_groups = []

    # transform response
    for name, desc, default, member_cmd in resp:
        disease_groups.append({
            'name': name,
            'default': default != 0,
            'description': desc,
            'memberCMD': member_cmd != 0,
        })

    return {
        'profile': {
            'query': query_s,
        },
        'data': disease_groups,
        'count': len(disease_groups),
        'nonce': nonce(),
    }


@router.get('/phenotypes', response_class=fastapi.responses.ORJSONResponse)
async def api_group_phenotypes(q: str = None):
    """
    Returns all available phenotypes or just those for a given
    disease group.
    """
    sql = 'SELECT `name`, `description`, `group`, `dichotomous` FROM Phenotypes'

    # groups to match
    groups = None

    # optionally filter by disease group
    if q and q != '':
        resp = engine.execute('SELECT `groups` FROM DiseaseGroups WHERE `name` = %s', q)
        groups = resp.fetchone()

        # groups are a comma-separated set
        if groups:
            groups = groups[0] and groups[0].split(',')

    # collect phenotype groups by union
    if groups is not None:
        sql = ' UNION '.join(f'({sql} WHERE FIND_IN_SET(%s, Phenotypes.`group`))' for _ in groups)

    # run the query
    resp, query_s = profile(engine.execute, sql, *groups) if groups else profile(engine.execute, sql)
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
        'nonce': nonce(),
    }


@router.get('/documentation', response_class=fastapi.responses.ORJSONResponse)
async def api_documentation(q: str, group: str = None):
    """
    Returns all available phenotypes or just those for a given
    portal group.
    """
    sql = 'SELECT `group`, `content` FROM Documentation WHERE `name` = %s '
    params = [q]

    # additionally get the the group
    if group is not None:
        sql += 'AND `group` = %s '
        params.append(group)

    # run the query
    resp, query_s = profile(engine.execute, sql, *params)

    # transform results
    data = [{'group': group, 'content': content} for group, content in resp.fetchall()]

    return {
        'profile': {
            'query': query_s,
        },
        'data': data,
        'count': len(data),
        'nonce': nonce(),
    }
