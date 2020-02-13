import dotenv
import flask
import os

import lib.config
import lib.continuation
import lib.query
import lib.secrets

from lib.profile import profile


# load dot files and configuration
dotenv.load_dotenv()
config = lib.config.Config()

# create flask app; this will load .env
app = flask.Flask(__name__, static_folder='web/static')

# connect to database
engine = lib.secrets.connect_to_mysql(config.rds_instance)

# max number of records to return per request
RECORD_LIMIT = os.getenv('BIOINDEX_RECORD_LIMIT', 5000)


@app.route('/')
def index():
    """
    SPA page.
    """
    return flask.send_file('web/index.html', mimetype='text/html')


@app.route('/api/indexes')
def api_indexes():
    """
    Return all queryable tables.
    """
    return {'indexes': list(config.tables.keys())}


@app.route('/api/keys/<idx>')
def api_keys(idx):
    """
    Return all the unique keys for a value-indexed table.
    """
    try:
        schema = config.table(idx).schema
        keys, query_s = profile(lib.query.keys, engine, idx, schema)
        fetched_keys = list(keys)

        return {
            'profile': {
                'query': query_s,
            },
            'index': idx,
            'count': len(fetched_keys),
            'keys': list(fetched_keys),
        }
    except AssertionError:
        flask.abort(400, f'Index {idx} is not indexed by value')
    except KeyError:
        flask.abort(404, f'Unknown index: {idx}')
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/api/count/<idx>')
def api_count(idx):
    """
    Query the database and estimate how many records will be returned.
    """
    try:
        q = flask.request.args.get('q')
        if q is None:
            raise ValueError('Missing query parameter')

        # lookup the schema for this index and perform the query
        schema = config.table(idx).schema
        count, query_s = profile(lib.query.count, engine, config.s3_bucket, idx, schema, q)

        return {
            'profile': {
                'query': query_s,
            },
            'index': idx,
            'q': q,
            'count': count,
        }
    except KeyError:
        flask.abort(400, f'Invalid index: {idx}')
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/api/all/<idx>')
def api_all(idx):
    """
    Query the database and return ALL records for a given index.
    """
    try:
        s3_prefix = config.table(idx).prefix

        # optional parameters
        fmt = flask.request.args.get('format', 'row')
        limit = flask.request.args.get('limit', type=int)

        # validate query parameters
        if fmt.lower() not in ['r', 'row', 'c', 'col', 'column']:
            raise ValueError('Invalid output format')

        # lookup the schema for this index and perform the query
        records, query_s = profile(lib.query.fetch_all, config.s3_bucket, s3_prefix)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            records = map(lambda x: x[1], zip(range(limit), records))

        # fetch the records from S3
        fetched_records, fetch_s, needs_cont = fetch_records(records, fmt)
        count = len(fetched_records)

        # make a continuation token if there are more records left to read
        cont_token = None if not needs_cont else lib.continuation.make_continuation(
            records=records,
            count=count,
            idx=idx,
            q='*',
            fmt=fmt,
            limit=limit,
        )

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': idx,
            'q': '*',
            'count': count,
            'page': 1,
            'limit': limit,
            'data': fetched_records,
            'continuation': cont_token,
        }
    except KeyError:
        flask.abort(400, f'Invalid index: {idx}')
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/api/query/<idx>')
def api_query(idx):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    try:
        q = flask.request.args.get('q')
        fmt = flask.request.args.get('format', 'row')
        limit = flask.request.args.get('limit', type=int)

        # validate query parameters
        if q is None:
            raise ValueError('Missing query parameter')
        if fmt.lower() not in ['r', 'row', 'c', 'col', 'column']:
            raise ValueError('Invalid output format')

        # lookup the schema for this index and perform the query
        schema = config.table(idx).schema
        records, query_s = profile(lib.query.fetch, engine, config.s3_bucket, idx, schema, q)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            records = map(lambda x: x[1], zip(range(limit), records))

        # fetch the records from s3
        fetched_records, fetch_s, needs_cont = fetch_records(records, fmt)
        count = len(fetched_records)

        # make a continuation token if there are more records left to read
        cont_token = None if not needs_cont else lib.continuation.make_continuation(
            records=records,
            count=count,
            idx=idx,
            q=q,
            fmt=fmt,
            limit=limit,
        )

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': idx,
            'q': q,
            'count': count,
            'page': 1,
            'limit': limit,
            'data': fetched_records,
            'continuation': cont_token,
        }
    except KeyError:
        flask.abort(400, f'Invalid index: {idx}')
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/api/cont')
def api_cont():
    """
    Lookup a continuation token and get the next set of records.
    """
    try:
        token = flask.request.args['token']
        cont = lib.continuation.lookup_continuation(token)

        # advance the page of the continuation
        cont.page += 1

        # fetch more records from S3
        fetched_records, fetch_s, needs_cont = fetch_records(cont.records, cont.fmt)
        count = len(fetched_records)

        # check for no more records
        if not needs_cont:
            token = lib.continuation.remove_continuation(token)

        return {
            'profile': {
                'fetch': fetch_s,
            },
            'index': cont.idx,
            'q': cont.q,
            'count': count,
            'page': cont.page,
            'limit': cont.limit,
            'data': fetched_records,
            'continuation': token,
        }
    except KeyError:
        flask.abort(400, f'Invalid, expired, or missing continuation token')
    except ValueError as e:
        flask.abort(400, str(e))


def fetch_records(records, fmt):
    """
    Reads the records from S3, transforms if necessary. This will not
    return more than RECORD_LIMIT records.

    Returns the records fetched, how long it took (in seconds), and a
    flag indicating whether there are more records still left to be
    fetched.
    """
    zipped_records = map(lambda x: x[1], zip(range(RECORD_LIMIT), records))

    # profile how long it takes to fetch the records from s3
    fetched_records, fetch_s = profile(list, zipped_records)
    count = len(fetched_records)

    # transform a list of dictionaries into a dictionary of lists
    if fmt.lower() in ['c', 'col', 'column']:
        return {k: [d.get(k) for d in fetched_records] for k in fetched_records[0]}

    # return the fetched records
    return fetched_records, fetch_s, count == RECORD_LIMIT
