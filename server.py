import flask
import itertools

from lib.client import *
from lib.continuation import *
from lib.locus import *
from lib.profile import *
from lib.query import *


# create flask app; this will load .env
app = flask.Flask(__name__)

# connect to redis and get bucket from .env
client = Client()
bucket = os.getenv('S3_BUCKET')

# verify environment
assert bucket, 'S3_BUCKET not set in environment or .env'


@app.route('/keys')
def server_keys():
    """
    Query the redis database for a list of all indexed key spaces.
    """
    return client.get_table_keys()


@app.route('/count/<key>')
def server_count(key):
    """
    Query the redis database for records overlapping the region and return the
    count of them without fetching.
    """
    try:
        locus = flask.request.args.get('q')
        chromosome, start, stop = parse_locus(locus)

        # perform the query and time it
        n, query_s = profile(client.count_records, key, chromosome, start, stop)

        return {
            'cont_token': None,
            'profile': {
                'query': query_s,
            },
            'key': key,
            'locus': locus,
            'count': n,
        }
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/query/<key>')
def server_query(key):
    """
    Query the redis database for records overlapping the region and then fetch
    the records from s3.
    """
    try:
        locus = flask.request.args.get('q')
        chromosome, start, stop = parse_locus(locus)

        # parse the query parameters
        sort_col = flask.request.args.get('sort')
        limit = flask.request.args.get('limit', type=int)

        # perform the query and time it
        results, query_s = profile(query, client, key, chromosome, start, stop, bucket)
        records, fetch_s = profile(fetch_records, results, limit=limit, sort_col=sort_col)

        # optionally generate a continuation token
        cont_token = make_continuation(results=results, key=key, locus=locus) if limit else None

        return {
            'cont_token': cont_token,
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'key': key,
            'locus': locus,
            'records': records,
        }
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/next/<token>')
def server_next(token: str):
    """
    Continue to fetch records from a previous query using a continuation token.
    """
    try:
        cont = lookup_continuation(token)
        results = cont.results

        # parse the query parameters
        sort_col = flask.request.args.get('sort')
        limit = flask.request.args.get('limit', type=int)

        # load records
        records, fetch_s = profile(fetch_records, results, limit=limit, sort_col=sort_col)

        # update the continuation if records were fetched
        if len(records) > 0:
            cont.update()

        return {
            'cont_token': token if limit else None,
            'profile': {
                'fetch': fetch_s,
            },
            'key': cont.key,
            'locus': cont.locus,
            'records': records,
        }
    except (KeyError, ValueError) as e:
        flask.abort(400, str(e))


def fetch_records(results, limit=None, sort_col=None):
    """
    Download records from s3 and optionally sort them.
    """
    records = list(itertools.islice(results, limit) if limit else results)

    # optionally sort the records, null values are last
    sort_col = flask.request.args.get('sort')
    if sort_col:
        records.sort(key=lambda r: (r[sort_col] is None, r[sort_col]))

    return records
