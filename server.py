import dotenv
import flask

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

        if q is None:
            raise ValueError('Missing query parameter')

        # lookup the schema for this index and perform the query
        schema = config.table(idx).schema
        records, query_s = profile(lib.query.fetch, engine, config.s3_bucket, idx, schema, q, limit=limit)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            records = map(lambda x: x[1], zip(range(limit), records))

        # use a zip to limit the maximum number of records returned by this request
        zipped_records = map(lambda x: x[1], zip(range(5000), records))

        # profile how long it takes to fetch the records from s3
        fetched_records, fetch_s = profile(list, zipped_records)
        count = len(fetched_records)

        # make a continuation token if there are more records left to read
        cont_token = None
        if count > 0 and (limit is None or count < limit):
            cont_token = lib.continuation.make_continuation(
                records=zipped_records,
                count=count,
                idx=idx,
                q=q,
                fmt=fmt,
            )

        # convert from list of dicts to dict of lists
        if fmt.lower() in ['c', 'col', 'column']:
            fetched_records = {k: [d[k] for d in fetched_records] for k in fetched_records[0]}

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': idx,
            'q': q,
            'count': count,
            'data': fetched_records,
            'continuation': cont_token,
        }
    except KeyError:
        flask.abort(400, f'Invalid index: {idx}')
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/api/cont')
def api_query():
    """
    Lookup a continuation token and get the next set of records.
    """
    try:
        token = flask.request.args['token']
        cont = lib.continuation.lookup_continuation(token)

        # use a zip to limit the maximum number of records returned by this request
        zipped_records = map(lambda x: x[1], zip(range(5000), cont.records))

        # profile how long it takes to fetch the records from s3
        fetched_records, fetch_s = profile(list, zipped_records)
        count = len(fetched_records)

        # check for no more records
        if count == 0:
            token = None

        # convert from list of dicts to dict of lists
        if cont.fmt.lower() in ['c', 'col', 'column']:
            fetched_records = {k: [d[k] for d in fetched_records] for k in fetched_records[0]}

        return {
            'profile': {
                'fetch': fetch_s,
            },
            'index': cont.idx,
            'q': cont.q,
            'count': count,
            'data': fetched_records,
            'continuation': token,
        }
    except KeyError:
        flask.abort(400, f'Invalid, expired, or missing continuation token')
    except ValueError as e:
        flask.abort(400, str(e))
