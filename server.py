import dotenv
import flask

import lib.config
import lib.metadata
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


@app.route('/api/keys/<index>')
def api_keys(index):
    """
    Return all the unique keys for a value-indexed table.
    """
    try:
        schema = config.table(index).schema
        keys, query_s = profile(lib.query.keys, engine, index, schema)
        fetched_keys = list(keys)

        return {
            'profile': {
                'query': query_s,
            },
            'index': index,
            'count': len(fetched_keys),
            'keys': list(fetched_keys),
        }
    except AssertionError:
        flask.abort(400, f'Index {index} is not indexed by value')
    except KeyError:
        flask.abort(404, f'Unknown index: {index}')
    except ValueError as e:
        flask.abort(400, str(e))


@app.route('/api/query/<index>')
def api_query(index):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    try:
        q = flask.request.args.get('q')
        fmt = flask.request.args.get('format', 'object')

        # lookup the schema for this index and perform the query
        schema = config.table(index).schema
        records, query_s = profile(lib.query.fetch, engine, config.s3_bucket, index, schema, q)

        # convert from list of dicts to dict of lists
        if fmt == 'array':
            records = {k: [d[k] for d in records] for k in records[0]}

        # profile collection of all the records from s3
        fetched_records, fetch_s = profile(list, records)

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': index,
            'q': q,
            'count': len(fetched_records),
            'data': fetched_records,
        }
    except KeyError:
        flask.abort(404, f'Unknown index: {index}')
    except ValueError as e:
        flask.abort(400, str(e))
