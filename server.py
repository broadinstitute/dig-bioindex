import flask
import itertools

from lib.client import *
from lib.continuation import *
from lib.locus import *
from lib.profile import *
from lib.query import *


# create flask app; this will load .env
app = flask.Flask(__name__, static_folder='web/static')

# connect to redis and get bucket from .env
client = Client()
bucket = os.getenv('S3_BUCKET')

# verify environment
assert bucket, 'S3_BUCKET not set in environment or .env'


@app.route('/')
def index():
    """
    SPA page.
    """
    return flask.send_file('web/index.html', mimetype='text/html')


@app.route('/api/keys')
def api_keys():
    """
    Query the redis database for a list of all indexed key spaces.
    """
    keys, query_s = profile(client.get_table_keys)

    return {
        'profile': {
            'query': query_s,
        },
        'keys': keys,
    }


@app.route('/api/count/<key>')
def api_count(key):
    """
    Query the redis database for records overlapping the region and return the
    count of them without fetching.
    """
    try:
        locus = flask.request.args.get('q')
        chromosome, start, stop = parse_locus(locus, allow_ens_lookup=True)

        # perform the query and time it
        n, query_s = profile(client.count_records, key, chromosome, start, stop)

        return {
            'cont_token': None,
            'profile': {
                'query': query_s,
            },
            'key': key,
            'locus': locus,
            'n': n,
        }
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/api/query/<key>')
def api_query(key):
    """
    Query the redis database for records overlapping the region and then fetch
    the records from s3.
    """
    try:
        locus = flask.request.args.get('q')
        output_format = flask.request.args.get('format', 'row')
        chromosome, start, stop = parse_locus(locus, allow_ens_lookup=True)

        # parse the query parameters
        sort_col = flask.request.args.get('sort')
        limit = flask.request.args.get('limit', type=int)

        # perform the query and time it
        results, query_s = profile(query, client, key, chromosome, start, stop, bucket)
        records, fetch_s = profile(fetch_records, results, limit=limit, sort_col=sort_col, format=output_format)

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


@app.route('/api/next/<token>')
def api_next(token: str):
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


def fetch_records(results, limit=None, sort_col=None, format=None):
    """
    Download records from s3 and optionally sort them.
    """
    records = list(itertools.islice(results, limit) if limit else results)

    # optionally sort the records, null values are last
    sort_col = flask.request.args.get('sort')
    if sort_col:
        records.sort(key=lambda r: (r[sort_col] is None, r[sort_col]))

    # convert the output format
    if re.fullmatch(r'col(?:umns?)?', format, re.IGNORECASE):
        return format_columns(records)

    # format must be row or columns
    if not re.fullmatch(r'rows?', format, re.IGNORECASE):
        raise ValueError('Unknown record output format: %s (use "row" or "column")', format)

    return records


def format_columns(records):
    """
    Return an object that is the LocusZoom format of the records.
    """
    if len(records) == 0:
        return {}

    # initialize output dictionary
    columns = {k: list() for k in records[0].keys()}

    # append the value to each column
    for record in records:
        for column in columns.keys():
            columns[column].append(record.get(column))

    return columns
