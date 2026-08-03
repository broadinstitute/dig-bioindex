import functools
import os

import fastapi
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text

from ..lib import http_cache
from ..lib.auth import restrictions
from ..lib.utils import nonce, profile
from ..middleware.portal import get_portal_ctx

# create web server
router = fastapi.APIRouter()

# How long a client may reuse portal metadata without asking again. These
# answers change when someone edits the portal database, which is rare, and
# the same handful of them is served tens of thousands of times a day.
MAX_AGE = int(os.environ.get('BIOINDEX_PORTAL_MAX_AGE', '300'))


def _require_portal(ctx):
    """
    The portal/metadata schema is optional; not every portal defines one.
    """
    if ctx.portal is None:
        raise fastapi.HTTPException(
            status_code=501,
            detail='Portal metadata schema not configured for this portal',
        )

    return ctx.portal


def _cache_headers(etag):
    """
    Reusable for MAX_AGE seconds, and revalidatable after that. Sent on the
    304 as well, so a client that revalidates starts a fresh window instead
    of asking again on the next request.
    """
    return {'ETag': etag, 'Cache-Control': f'public, max-age={MAX_AGE}, must-revalidate'}


def revalidatable(handler):
    """
    Tag a metadata response so clients and any shared cache can reuse it.

    The handler keeps returning a plain dict; this turns it into a response
    with a validator, and answers 304 when the client already has that
    version. Only for responses that are the same for everyone - see
    api_portal_restrictions, which is not.
    """
    @functools.wraps(handler)
    async def wrapper(req, *args, **kwargs):
        payload = await handler(req, *args, **kwargs)

        # encode first: the envelope holds dates and other types that are not
        # JSON on their own, and returning a Response skips the encoding
        # FastAPI would otherwise have done for us
        content = jsonable_encoder(payload)
        etag = http_cache.etag_for(content)

        if http_cache.if_none_match(req.headers.get('if-none-match'), etag):
            return fastapi.Response(status_code=304, headers=_cache_headers(etag))

        return fastapi.responses.ORJSONResponse(content, headers=_cache_headers(etag))

    return wrapper


@router.get("/groups", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_groups(req: fastapi.Request):
    """
    Returns the list of portals available.
    """
    portal = _require_portal(get_portal_ctx(req))
    sql = "SELECT `name`, `title`, `description`, `default`, `portalGroup` FROM DiseaseGroups"

    # run the query
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql))
        disease_groups = []

        # transform response
        for name, title, desc, default, portalGroup in resp:
            disease_groups.append(
                {
                    "name": name,
                    "default": default != 0,
                    "description": desc,
                    "title": title,
                    "portalGroup": portalGroup,
                }
            )

    return {
        "profile": {
            "query": query_s,
        },
        "data": disease_groups,
        "count": len(disease_groups),
        "nonce": nonce(),
    }


@router.get("/restrictions", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_restrictions(req: fastapi.Request, response: fastapi.Response):
    """
    Returns all restrictions for the current user.
    """
    portal = _require_portal(get_portal_ctx(req))
    keyword_restrictions, query_s = profile(restrictions, portal, req)

    # deliberately not @revalidatable: this one is per-user. It reads
    # x-bioindex-access-token and answers differently depending on who is
    # asking, so a shared cache holding it would hand one user's
    # restrictions to another.
    response.headers['Cache-Control'] = 'private, no-store'

    return {
        "profile": {
            "query": query_s,
        },
        "data": keyword_restrictions,
        "nonce": nonce(),
    }


def fetch_added_phenotypes(portal, include: list):
    """
    Returns named phenotypes specified by include
    """
    escaped_param_names = [name.replace(' ', '').replace('-', '_') for name in include]
    format_strings = ','.join([f":{name}" for name in escaped_param_names])
    sql = f"SELECT `name`, `description`, `group`, `dichotomous` FROM Phenotypes where `name` in ({format_strings})"

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql), dict(zip(escaped_param_names, include)))
        phenotypes = []

        # transform response
        for name, desc, group, dichotomous in resp:
            phenotypes.append(
                {
                    "name": name,
                    "description": desc,
                    "group": group,
                    "dichotomous": dichotomous,
                }
            )

        return phenotypes


def query_phenotypes(portal, q=None):
    """
    The phenotypes for a disease group, or all of them when no group is
    given. Returns the list and the time the query took; an unknown group
    is an empty list and no query at all.
    """
    sql = "SELECT `name`, `description`, `group`, `dichotomous` FROM Phenotypes"

    # groups to match
    groups = None
    include = None
    exclude = None

    with portal.connect() as conn:

        # optionally filter by disease group
        if q and q != "":
            resp = conn.execute(text("SELECT `groups`, include, exclude FROM DiseaseGroups WHERE `name` = :name"),
                                {"name": q})
            rows = resp.fetchone()

            if rows is None:
                return [], ""

            # groups are a comma-separated set
            groups = rows[0].split(",")
            include = rows[1].split(",") if rows[1] else None
            exclude = rows[2].split(",") if rows[2] else None

        # collect phenotype groups by union
        group_params = []
        if groups is not None and groups[0] != '':
            group_params = [f"{group.replace(' ', '').replace('-', '_')}" for group in groups]
            sql = f"({sql} WHERE `group` in ({','.join([':' + param for param in group_params])}))"

        # run the query
        resp, query_s = (
            profile(conn.execute, text(sql), dict(zip(group_params, groups)))
            if groups
            else profile(conn.execute, text(sql))
        )
        phenotypes = []

        # transform response
        for name, desc, group, dichotomous in resp:
            if exclude and name in exclude:
                continue
            phenotypes.append(
                {
                    "name": name,
                    "description": desc,
                    "group": group,
                    "dichotomous": dichotomous,
                }
            )
        if include:
            phenotypes.extend(fetch_added_phenotypes(portal, include))

        return phenotypes, query_s


@router.get("/phenotypes", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_phenotypes(req: fastapi.Request, q: str = None):
    """
    Returns all available phenotypes or just those for a given
    disease group.
    """
    portal = _require_portal(get_portal_ctx(req))
    phenotypes, query_s = query_phenotypes(portal, q)

    return {
        "profile": {
            "query": query_s,
        },
        "data": phenotypes,
        "count": len(phenotypes),
        "nonce": nonce(),
    }


@router.get("/complications", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_complications(req: fastapi.Request, q: str = None):
    """
    Returns all available complication phenotype pairs.
    """
    portal = _require_portal(get_portal_ctx(req))
    sql = (
        "SELECT Complications.`name`, Phenotypes.`group`, Complications.`phenotype`, Complications.`withComplication` "
        "FROM Complications "
        "JOIN Phenotypes "
        "ON Phenotypes.`name` = Complications.`name` "
    )

    # groups to match
    groups = None

    with portal.connect() as conn:
        # optionally filter by disease group
        if q and q != "":
            resp = portal.execute("SELECT `groups` FROM DiseaseGroups WHERE `name` = :name", {"name": q})
            rows = resp.fetchone() or [""]

            # groups are a comma-separated set
            groups = rows[0].split(",")
            escaped_param_names = [group.replace(' ', '').replace('-', '_') for group in groups]

        # collect phenotype groups by union
        if groups is not None:
            sql = " UNION ".join(
                f"({sql} WHERE FIND_IN_SET(:{group}, Phenotypes.`group`))" for group in escaped_param_names
            )

        # run the query
        if sql:
            resp, query_s = (
                profile(conn.execute, text(sql), dict(zip(escaped_param_names, groups)))
                if groups
                else profile(conn.execute, text(sql))
            )

        # distinct complications
        complications = {}

        # collect all complication phenotypes together into a dict
        for name, _, phenotype, with_complication in resp:
            complications.setdefault(name, dict())[phenotype] = with_complication

        return {
            "profile": {
                "query": query_s,
            },
            "data": [{"name": k, "phenotypes": v} for k, v in complications.items()],
            "count": len(complications),
            "nonce": nonce(),
        }


@router.get("/datasets", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_datasets(req: fastapi.Request, q: str = None):
    """
    Returns all available datasets for a given disease group.
    """
    portal = _require_portal(get_portal_ctx(req))

    # map all the phenotypes for this portal group. Goes to the query rather
    # than to the route handler, which now returns a tagged response.
    phenotype_rows, query_p = query_phenotypes(portal, q)
    phenotypes = set(p["name"] for p in phenotype_rows)

    # query for datasets
    sql = (
        "SELECT `name`, "
        "       `description`, "
        "       `community`, "
        "       `phenotypes`, "
        "       `ancestry`, "
        "       `ancestry_name`, "
        "       `tech`, "
        "       `subjects`, "
        "       `access`, "
        "       `new`, "
        "       `pmid`, "
        "       `added` "
        "FROM Datasets"
    )

    # get all datasets
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql))
        datasets = []

        # filter all the datasets
        for r in resp:
            ps = [p for p in r[3].split(",") if p in phenotypes]

            dataset = {
                "name": r[0],
                "description": r[1],
                "community": r[2],
                "phenotypes": ps,
                "ancestry": r[4],
                "ancestry_name": r[5],
                "tech": r[6],
                "subjects": r[7],
                "access": r[8],
                "new": r[9] != 0,
                "pmid": r[10],
                "added": r[11],
            }

            if len(ps) > 0:
                datasets.append(dataset)

        return {
            "profile": {
                "query": query_s if not isinstance(query_p, float) else query_p + query_s,
            },
            "data": datasets,
            "count": len(datasets),
            "nonce": nonce(),
        }


@router.get("/documentation", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_documentation(req: fastapi.Request, q: str, group: str = None):
    """
    Returns all available phenotypes or just those for a given
    portal group.
    """
    portal = _require_portal(get_portal_ctx(req))
    sql = "SELECT `group`, `content` FROM Documentation WHERE `name` = :name "
    params = {'name': q}

    # additionally get the the group
    if group is not None:
        sql += "AND `group` = :group "
        params.update({'group': group})

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql), params)

        # transform response
        data = [{"group": group, "content": content} for group, content in resp.fetchall()]

        return {
            "profile": {
                "query": query_s,
            },
            "data": data,
            "count": len(data),
            "nonce": nonce(),
        }


# Returns all documentations for a given group, and any modification to default group md
@router.get("/documentations", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_documentations(req: fastapi.Request, q: str):
    portal = _require_portal(get_portal_ctx(req))
    sql = "SELECT `group`, `name`, `content` FROM Documentation "

    # if q is not equal to md, then add md to group, else add q to group
    if q != "md":
        sql += "WHERE `group` IN (:q, 'md')"
    else:
        sql += "WHERE `group` IN (:q)"

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql).bindparams(q=q))

        # transform results
        data = [
            {"group": group, "name": name, "content": content}
            for group, name, content in resp.fetchall()
        ]

        return {
            "profile": {
                "query": query_s,
            },
            "data": data,
            "count": len(data),
            "nonce": nonce(),
        }


@router.get("/systems", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_systems(req: fastapi.Request):
    """
    Returns system-disease-phenotype for all systems.
    """
    portal = _require_portal(get_portal_ctx(req))

    # fetch all systems, join to diseases and phenotype groups
    sql = """
        SELECT s.system, s.portals, d.disease, g.group, p.name as phenotype
            FROM SystemToDisease stod
            JOIN DiseaseToGroup dtog ON stod.diseaseId = dtog.diseaseId
            JOIN GroupToPhenotype gtop ON dtog.groupId = gtop.groupId
            JOIN Systems s ON s.id = stod.systemId
            JOIN Diseases d ON d.id = stod.diseaseId
            JOIN PhenotypeGroups g ON g.id = dtog.groupId
            JOIN Phenotypes p ON p.id = gtop.phenotypeId
        ORDER BY s.system, d.disease, g.group, p.name
        """

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql))
        # get all systems
        systems = []

        # filter all the systems
        for r in resp:
            system = {
                "system": r[0],
                "portals": r[1],
                "disease": r[2],
                "group": r[3],
                "phenotype": r[4],
            }

            systems.append(system)

        return {
            "profile": {
                "query": query_s,
            },
            "data": systems,
            "count": len(systems),
            "nonce": nonce(),
        }


@router.get("/links", response_class=fastapi.responses.ORJSONResponse)
@revalidatable
async def api_portal_links(req: fastapi.Request, q: str = None, group: str = None):
    """
    Returns one - or all - redirect links.
    """
    portal = _require_portal(get_portal_ctx(req))
    sql = "SELECT `path`, `group`, `redirect`, `description` FROM Links "
    tests = []
    data = []
    sql_params = {}

    # create conditionals
    if q:
        tests.append(text(":path LIKE `path`"))
        sql_params['path'] = q
    if group:
        tests.append(text("`group` = :group"))
        sql_params['group'] = group

    # add all the tests
    if tests:
        sql += f'WHERE {" AND ".join(str(test) for test in tests)}'

    # run the query
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql).bindparams(**sql_params))

        # transform results
        for path, group, redirect, description in resp:
            data.append(
                {
                    "path": path,
                    "group": group,
                    "redirect": redirect,
                    "description": description,
                }
            )

        return {
            "profile": {
                "query": query_s,
            },
            "data": data,
            "count": len(data),
            "nonce": nonce(),
        }
