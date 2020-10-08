import fastapi

from ..lib import aws
from ..lib import config

from ..lib.auth import restrictions
from ..lib.profile import profile
from ..lib.utils import nonce


# load dot files and configuration
CONFIG = config.Config()

# create web server
router = fastapi.APIRouter()

# connect to database
portal = aws.connect_to_rds(CONFIG.rds_instance, schema=CONFIG.portal_schema)


@router.get('/groups', response_class=fastapi.responses.ORJSONResponse)
async def api_portal_groups():
    """
    Returns the list of portals available.
    """
    sql = 'SELECT `name`, `description`, `default`, `memberCMD` FROM DiseaseGroups'

    # run the query
    resp, query_s = profile(portal.execute, sql)
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


@router.get('/restrictions', response_class=fastapi.responses.ORJSONResponse)
async def api_portal_restrictions(req: fastapi.Request):
    """
    Returns all restrictions for the current user.
    """
    keyword_restrictions, query_s = profile(restrictions, portal, req)

    return {
        'profile': {
            'query': query_s,
        },
        'data': keyword_restrictions,
        'nonce': nonce(),
    }



@router.get('/phenotypes', response_class=fastapi.responses.ORJSONResponse)
async def api_portal_phenotypes(q: str = None):
    """
    Returns all available phenotypes or just those for a given
    disease group.
    """
    sql = 'SELECT `name`, `description`, `group`, `dichotomous` FROM Phenotypes'

    # groups to match
    groups = None

    # optionally filter by disease group
    if q and q != '':
        resp = portal.execute('SELECT `groups` FROM DiseaseGroups WHERE `name` = %s', q)
        groups = resp.fetchone()

        # groups are a comma-separated set
        if groups:
            groups = groups[0] and groups[0].split(',')

    # collect phenotype groups by union
    if groups is not None:
        sql = ' UNION '.join(f'({sql} WHERE FIND_IN_SET(%s, Phenotypes.`group`))' for _ in groups)

    # run the query
    resp, query_s = profile(portal.execute, sql, *groups) if groups else profile(portal.execute, sql)
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


@router.get('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def api_portal_datasets(req: fastapi.Request, q: str=None):
    """
    Returns all available datasets for a given disease group.
    """
    resp = await api_portal_phenotypes(q)

    # map all the phenotypes for this portal group
    phenotypes = set(p['name'] for p in resp['data'])
    query_p = resp['profile']['query']

    # query for datasets
    sql = (
        'SELECT `name`, '
        '       `description`, '
        '       `community`, '
        '       `phenotypes`, '
        '       `ancestry`, '
        '       `tech`, '
        '       `subjects`, '
        '       `access`, '
        '       `new` '
        'FROM Datasets'
    )

    # get all datasets
    resp, query_s = profile(portal.execute, sql)
    datasets = []

    # filter all the datasets
    for r in resp:
        ps = [p for p in r[3].split(',') if p in phenotypes]

        dataset = {
            'name': r[0],
            'description': r[1],
            'community': r[2],
            'phenotypes': ps,
            'ancestry': r[4],
            'tech': r[5],
            'subjects': r[6],
            'access': r[7],
            'new': r[8] != 0,
        }

        if len(ps) > 0:
            datasets.append(dataset)

    return {
        'profile': {
            'query': query_p + query_s,
        },
        'data': datasets,
        'count': len(datasets),
        'nonce': nonce(),
    }


@router.get('/documentation', response_class=fastapi.responses.ORJSONResponse)
async def api_portal_documentation(q: str, group: str = None):
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
    resp, query_s = profile(portal.execute, sql, *params)

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


@router.get('/links', response_class=fastapi.responses.ORJSONResponse)
async def api_portal_links(q: str = None, group: str = None):
    """
    Returns one - or all - redirect links.
    """
    sql = 'SELECT `path`, `group`, `redirect`, `description` FROM Links '
    tests = []
    data = []

    # create conditionals
    if q:
        tests += [('`path` = %s', q)]
    if group:
        tests += [('`group` = %s', group)]

    # add all the tests
    if tests:
        sql += f'WHERE {" AND ".join(test[0] for test in tests)}'

    # run the query
    resp, query_s = profile(portal.execute, sql, *[test[1] for test in tests])

    # transform results
    for path, group, redirect, description in resp:
        data.append({
            'path': path,
            'group': group,
            'redirect': redirect,
            'description': description,
        })

    return {
        'profile': {
            'query': query_s,
        },
        'data': data,
        'count': len(data),
        'nonce': nonce(),
    }
