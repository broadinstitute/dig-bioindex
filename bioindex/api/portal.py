import fastapi
from sqlalchemy import text

from .bio import _finalize

from ..lib.auth import restrictions
from ..lib.utils import profile
from ..middleware.portal import get_portal_ctx

# create web server
router = fastapi.APIRouter()


def _require_portal(ctx):
    """Raise 501 if this portal has no portal/metadata schema configured."""
    if ctx.portal is None:
        raise fastapi.HTTPException(
            status_code=501,
            detail='Portal metadata schema not configured for this portal',
        )
    return ctx.portal


@router.get("/groups", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_groups(req: fastapi.Request):
    """
    Returns the list of portals available.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    sql = "SELECT `name`, `title`, `description`, `default`, `portalGroup` FROM DiseaseGroups"

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql))
        disease_groups = []

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

    return _finalize({
        "profile": {"query": query_s},
        "data": disease_groups,
        "count": len(disease_groups),
    })


@router.get("/restrictions", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_restrictions(req: fastapi.Request):
    """
    Returns all restrictions for the current user.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    keyword_restrictions, query_s = profile(restrictions, portal, req)

    return _finalize({
        "profile": {"query": query_s},
        "data": keyword_restrictions,
    })


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


def _fetch_phenotypes(portal, q) -> dict:
    """Query Phenotypes (optionally filtered by DiseaseGroup q); return plain body dict."""
    sql = "SELECT `name`, `description`, `group`, `dichotomous` FROM Phenotypes"
    groups = None
    include = None
    exclude = None

    with portal.connect() as conn:
        if q and q != "":
            resp = conn.execute(text("SELECT `groups`, include, exclude FROM DiseaseGroups WHERE `name` = :name"),
                                {"name": q})
            rows = resp.fetchone()

            if rows is None:
                return {"profile": {"query": ""}, "data": [], "count": 0}

            groups = rows[0].split(",")
            include = rows[1].split(",") if rows[1] else None
            exclude = rows[2].split(",") if rows[2] else None

        group_params = []
        if groups is not None and groups[0] != '':
            group_params = [f"{group.replace(' ', '').replace('-', '_')}" for group in groups]
            sql = f"({sql} WHERE `group` in ({','.join([':' + param for param in group_params])}))"

        resp, query_s = (
            profile(conn.execute, text(sql), dict(zip(group_params, groups)))
            if groups
            else profile(conn.execute, text(sql))
        )
        phenotypes = []

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

        return {"profile": {"query": query_s}, "data": phenotypes, "count": len(phenotypes)}


@router.get("/phenotypes", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_phenotypes(req: fastapi.Request, q: str = None):
    """
    Returns all available phenotypes or just those for a given
    disease group.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    return _finalize(_fetch_phenotypes(portal, q))


@router.get("/complications", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_complications(req: fastapi.Request, q: str = None):
    """
    Returns all available complication phenotype pairs.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    sql = (
        "SELECT Complications.`name`, Phenotypes.`group`, Complications.`phenotype`, Complications.`withComplication` "
        "FROM Complications "
        "JOIN Phenotypes "
        "ON Phenotypes.`name` = Complications.`name` "
    )

    groups = None

    with portal.connect() as conn:
        if q and q != "":
            resp = portal.execute("SELECT `groups` FROM DiseaseGroups WHERE `name` = :name", {"name": q})
            rows = resp.fetchone() or [""]

            groups = rows[0].split(",")
            escaped_param_names = [group.replace(' ', '').replace('-', '_') for group in groups]

        if groups is not None:
            sql = " UNION ".join(
                f"({sql} WHERE FIND_IN_SET(:{group}, Phenotypes.`group`))" for group in escaped_param_names
            )

        if sql:
            resp, query_s = (
                profile(conn.execute, text(sql), dict(zip(escaped_param_names, groups)))
                if groups
                else profile(conn.execute, text(sql))
            )

        complications = {}

        for name, _, phenotype, with_complication in resp:
            complications.setdefault(name, dict())[phenotype] = with_complication

        return _finalize({
            "profile": {"query": query_s},
            "data": [{"name": k, "phenotypes": v} for k, v in complications.items()],
            "count": len(complications),
        })


@router.get("/datasets", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_datasets(req: fastapi.Request, q: str = None):
    """
    Returns all available datasets for a given disease group.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    resp_data = _fetch_phenotypes(portal, q)
    phenotypes = set(p["name"] for p in resp_data["data"])
    query_p = resp_data["profile"]["query"]

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

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql))
        datasets = []

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

        return _finalize({
            "profile": {
                "query": query_s if not isinstance(query_p, float) else query_p + query_s,
            },
            "data": datasets,
            "count": len(datasets),
        })


@router.get("/documentation", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_documentation(req: fastapi.Request, q: str, group: str = None):
    """
    Returns all available phenotypes or just those for a given
    portal group.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    sql = "SELECT `group`, `content` FROM Documentation WHERE `name` = :name "
    params = {'name': q}

    if group is not None:
        sql += "AND `group` = :group "
        params.update({'group': group})

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql), params)

        data = [{"group": group, "content": content} for group, content in resp.fetchall()]

        return _finalize({
            "profile": {"query": query_s},
            "data": data,
            "count": len(data),
        })


@router.get("/documentations", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_documentations(req: fastapi.Request, q: str):
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    sql = "SELECT `group`, `name`, `content` FROM Documentation "

    if q != "md":
        sql += "WHERE `group` IN (:q, 'md')"
    else:
        sql += "WHERE `group` IN (:q)"

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql).bindparams(q=q))

        data = [
            {"group": group, "name": name, "content": content}
            for group, name, content in resp.fetchall()
        ]

        return _finalize({
            "profile": {"query": query_s},
            "data": data,
            "count": len(data),
        })


@router.get("/systems", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_systems(req: fastapi.Request):
    """
    Returns system-disease-phenotype for all systems.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)

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
        systems = []

        for r in resp:
            system = {
                "system": r[0],
                "portals": r[1],
                "disease": r[2],
                "group": r[3],
                "phenotype": r[4],
            }

            systems.append(system)

        return _finalize({
            "profile": {"query": query_s},
            "data": systems,
            "count": len(systems),
        })


@router.get("/links", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_links(req: fastapi.Request, q: str = None, group: str = None):
    """
    Returns one - or all - redirect links.
    """
    ctx = get_portal_ctx(req)
    portal = _require_portal(ctx)
    sql = "SELECT `path`, `group`, `redirect`, `description` FROM Links "
    tests = []
    data = []
    sql_params = {}

    if q:
        tests.append(text(":path LIKE `path`"))
        sql_params['path'] = q
    if group:
        tests.append(text("`group` = :group"))
        sql_params['group'] = group

    if tests:
        sql += f'WHERE {" AND ".join(str(test) for test in tests)}'

    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, text(sql).bindparams(**sql_params))

        for path, group, redirect, description in resp:
            data.append(
                {
                    "path": path,
                    "group": group,
                    "redirect": redirect,
                    "description": description,
                }
            )

        return _finalize({
            "profile": {"query": query_s},
            "data": data,
            "count": len(data),
        })
