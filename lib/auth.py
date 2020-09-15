import orjson
import requests


def verify_access_token(req):
    """
    Verifies a Google OAuth access token and returns the email
    address associated with it or None if invalid.
    """
    token = req.headers.get('x-bioindex-access-token') or req.query_params.get('access_token')
    if not token:
        return None

    # get the token validity from google
    url = f'https://oauth2.googleapis.com/tokeninfo?access_token={token}'
    resp = requests.get(url)

    # fail if the request is invalid
    if resp.status_code != 200:
        return None

    return resp.json().get('email')


def restrictions(engine, req):
    """
    Returns a list of restriction groups after removing the set
    of restrictions accessible by the user.
    """
    email = verify_access_token(req)

    # find all restrictions this user doesn't have access to
    sql = 'SELECT `keywords` FROM `Restrictions` '

    # get all restrictions or just those for the user
    if email:
        sql += (
            'LEFT JOIN `Users` '
            'ON `email` = %s '
            'AND ( '
            '  (FIND_IN_SET("*", `restrictions`) > 0) OR '
            '  (FIND_IN_SET(Restrictions.`name`, `restrictions`) > 0) '
            ') '
            'WHERE `email` IS NULL '
        )

    # execute the query
    cursor = engine.execute(sql, email) if email else engine.execute(sql)

    # all restrictions
    return [orjson.loads(r[0].encode('utf-8')) for r in cursor]


def restricted_keywords(engine, req):
    """
    Returns the dictionary of restricted keywords after removing
    the set of accessible keywords the user is authorized to
    access.
    """
    restricted = dict()

    # decode the restricted keyword json
    for keys in restrictions(engine, req):
        for key, value in keys.items():
            values = restricted.setdefault(key, set())

            # merge the values together
            if isinstance(value, list):
                values.update(value)
            else:
                values.add(value)

    return restricted


def verify_permissions(engine, req, **keywords):
    """
    Helper function for looking up whether or not the user request
    has premission to view a specific set of keywords.
    """
    restricted = restricted_keywords(engine, req)

    # test every keyword passed in to see if its value is restricted
    for k, v in keywords.items():
        if v in restricted.get(k, set()):
            return False

    return True


def verify_record(record, restricted, **keymap):
    """
    Returns True if the record has no restricted keywords.
    """
    if not restricted:
        return True

    for k, values in restricted.items():
        if record.get(keymap.get(k, k)) in values:
            return False

    return True
