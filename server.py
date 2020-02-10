import dotenv
import flask
import os

import lib.metadata
import lib.query
import lib.secrets

from lib.profile import profile


# load dot files
dotenv.load_dotenv()

# create flask app; this will load .env
app = flask.Flask(__name__, static_folder='web/static')
bucket = os.getenv('S3_BUCKET')

# verify environment
assert bucket, 'S3_BUCKET not set in environment or .env'

# connect to database and load metadata
engine = lib.secrets.connect_to_mysql(os.getenv('RDS_INSTANCE'))
metadata = lib.metadata.load_metadata(engine)


@app.route('/')
def index():
    """
    SPA page.
    """
    return flask.send_file('web/index.html', mimetype='text/html')


@app.route('/api/tables')
def api_keys():
    """
    Return all queryable tables.
    """
    return {'tables': list(metadata.keys())}


@app.route('/api/query/<table>')
def api_query(table):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    try:
        q = flask.request.args.get('q')
        fmt = flask.request.args.get('format', 'object')

        # perform the query and time it
        records, query_s = profile(lib.query.fetch, engine, metadata, bucket, table, q)

        # convert from list of dicts to dict of lists
        if fmt == 'array':
            records = {k: [d[k] for d in records] for k in records[0]}

        return {
            'profile': query_s,
            'table': table,
            'q': q,
            'data': records,
        }
    except ValueError as e:
        flask.abort(400, str(e))
