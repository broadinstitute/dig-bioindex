import fastapi
import sqlalchemy

from .utils import *

from ..lib import config
from ..lib.auth import restrictions
from ..lib.utils import nonce, profile

# load dot files and configuration
CONFIG = config.Config()

# create web server
router = fastapi.APIRouter()

# optionally connect to the portal/metadata schema
portal = connect_to_portal(CONFIG)

# if there is no portal schema defined, then patch the router
if not portal:
    monkey_patch_router(router)


@router.get("/groups", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_groups():
    """
    Returns the list of portals available.
    """
    sql = "SELECT `name`, `title`, `description`, `default`, `portalGroup` FROM DiseaseGroups"

    # run the query
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, sqlalchemy.text(sql))
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
async def api_portal_restrictions(req: fastapi.Request):
    """
    Returns all restrictions for the current user.
    """
    keyword_restrictions, query_s = profile(restrictions, portal, req)

    return {
        "profile": {
            "query": query_s,
        },
        "data": keyword_restrictions,
        "nonce": nonce(),
    }


@router.get("/phenotypes", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_phenotypes(q: str = None):
    """
    Returns all available phenotypes or just those for a given
    disease group.
    """
    sql = "SELECT `name`, `description`, `group`, `dichotomous` FROM Phenotypes"

    # groups to match
    groups = None

    # optionally filter by disease group
    if q and q != "":
        with portal.connect() as conn:
            group_sql = sqlalchemy.text("SELECT `groups` FROM DiseaseGroups WHERE `name` = :name")
            resp = conn.execute(group_sql, {'name': q})
            rows = resp.fetchone() or [""]

        # groups are a comma-separated set
        groups = rows[0].split(",")

    # run the query
    with portal.connect() as conn:
        if groups is not None:
            group_dict = {f'group_{i}': group for i, group in enumerate(groups)}
            sql = " UNION ".join(
                f"({sql} WHERE FIND_IN_SET(:group_{i}, Phenotypes.`group`))" for i in range(len(groups))
            )
            resp, query_s = profile(conn.execute, sqlalchemy.text(sql), group_dict)
        else:
            resp, query_s = profile(conn.execute, sqlalchemy.text(sql))

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

    phenotypes.append(
        {
            "name": "T2D_quant",
            "description": "Type 2 diabetes (T2D) risk factor",
            "group": "GLYCEMIC",
            "dichotomous": 0
        }
    )
    phenotypes.append(
        {
            "name": "T2DadjBMI_quant",
            "description": "Type 2 diabetes (T2D) risk factor adjusted for BMI",
            "group": "GLYCEMIC",
            "dichotomous": 0
        }
    )

    return {
        "profile": {
            "query": query_s,
        },
        "data": phenotypes,
        "count": len(phenotypes),
        "nonce": nonce(),
    }


@router.get("/complications", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_complications(q: str = None):
    """
    Returns all available complication phenotype pairs.
    """
    sql = (
        "SELECT Complications.`name`, Phenotypes.`group`, Complications.`phenotype`, Complications.`withComplication` "
        "FROM Complications "
        "JOIN Phenotypes "
        "ON Phenotypes.`name` = Complications.`name` "
    )

    # groups to match
    groups = None

    # optionally filter by disease group
    if q and q != "":
        resp = portal.execute("SELECT `groups` FROM DiseaseGroups WHERE `name` = %s", q)
        rows = resp.fetchone() or [""]

        # groups are a comma-separated set
        groups = rows[0].split(",")

    # collect phenotype groups by union
    if groups is not None:
        sql = " UNION ".join(
            f"({sql} WHERE FIND_IN_SET(%s, Phenotypes.`group`))" for _ in groups
        )

    # run the query
    if sql:
        sql_text = sqlalchemy.text(sql)
        with portal.connect() as conn:
            resp, query_s = (
                profile(conn.execute, sql_text, *groups)
                if groups
                else profile(conn.execute, sql_text)
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
async def api_portal_datasets(req: fastapi.Request, q: str = None):
    """
    Returns all available datasets for a given disease group.
    """
    resp = await api_portal_phenotypes(q)

    # map all the phenotypes for this portal group
    phenotypes = set(p["name"] for p in resp["data"])
    query_p = resp["profile"]["query"]

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
        resp, query_s = profile(conn.execute, sqlalchemy.text(sql))
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
            "query": query_p + query_s,
        },
        "data": datasets,
        "count": len(datasets),
        "nonce": nonce(),
    }


@router.get("/documentation", response_class=fastapi.responses.ORJSONResponse)
async def api_portal_documentation(q: str, group: str = None):
    """
    Returns all available phenotypes or just those for a given
    portal group.
    """
    sql = "SELECT `group`, `content` FROM Documentation WHERE `name` = :name "
    params = {'name': q}

    # additionally get the the group
    if group is not None:
        sql += "AND `group` = :group "
        params['group'] = group

    # run the query
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, sqlalchemy.text(sql), params)

    # transform results
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
async def api_portal_documentations(q: str):
    sql = "SELECT `group`, `name`, `content` FROM Documentation "

    # if q is not equal to md, then add md to group, else add q to group
    if q != "md":
        sql += "WHERE `group` IN ({0}, 'md')".format("%s")
    else:
        sql += "WHERE `group` IN ({0})".format("%s")

    params = [q]
    # params.append(q)

    # run the query
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, sqlalchemy.text(sql), *params)

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
async def api_portal_systems(req: fastapi.Request):
    """
    Returns system-disease-phenotype for all systems.
    """

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

    # get all systems
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, sqlalchemy.text(sql))
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
async def api_portal_links(q: str = None, group: str = None):
    """
    Returns one - or all - redirect links.
    """
    sql = "SELECT `path`, `group`, `redirect`, `description` FROM Links "
    tests = []
    data = []

    # create conditionals
    if q:
        tests += [("%s LIKE `path`", q)]
    if group:
        tests += [("`group` = %s", group)]

    # add all the tests
    if tests:
        sql += f'WHERE {" AND ".join(test[0] for test in tests)}'

    # run the query
    with portal.connect() as conn:
        resp, query_s = profile(conn.execute, sqlalchemy.text(sql), *[test[1] for test in tests])

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
