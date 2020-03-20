import dotenv
import flask
import itertools
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
routes = flask.Blueprint('api', __name__)

# connect to database
engine = lib.secrets.connect_to_mysql(config.rds_instance, schema='bio')

# max number of bytes to return per request
RESPONSE_LIMIT = int(os.getenv('BIOINDEX_RESPONSE_LIMIT', 1 * 1024 * 1024))


@routes.route('/api/indexes')
def api_indexes():
    """
    Return all queryable tables.
    """
    indexes = config.indexes.keys()

    return {
        'count': len(indexes),
        'data': list(indexes),
    }


@routes.route('/api/keys/<index>')
def api_keys(index):
    """
    Return all the unique keys for a value-indexed table.
    """
    try:
        idx = config.index(index)

        # get the partial query parameters to apply
        q = parse_query()

        # execute the query
        keys, query_s = profile(lib.query.keys, engine, idx, q)
        fetched = list(keys)

        return {
            'profile': {
                'query': query_s,
            },
            'index': index,
            'count': len(fetched),
            'data': list(fetched),
        }
    except AssertionError:
        flask.abort(400, f'Index {index} is not indexed by value')
    except KeyError:
        flask.abort(404, f'Unknown index: {index}')
    except ValueError as e:
        flask.abort(400, str(e))


@routes.route('/api/count/<index>')
def api_count(index):
    """
    Query the database and estimate how many records will be returned.
    """
    try:
        q = parse_query(required=True)

        # lookup the schema for this index and perform the query
        idx = config.index(index)
        count, query_s = profile(lib.query.count, engine, config.s3_bucket, idx, q)

        return {
            'profile': {
                'query': query_s,
            },
            'index': index,
            'q': q,
            'count': count,
        }
    except KeyError:
        flask.abort(400, f'Invalid index: {index}')
    except ValueError as e:
        flask.abort(400, str(e))


@routes.route('/api/all/<index>')
def api_all(index):
    """
    Query the database and return ALL records for a given index.
    """
    try:
        idx = config.index(index)

        # optional parameters
        fmt = flask.request.args.get('format', 'row')
        limit = flask.request.args.get('limit', type=int)

        # validate query parameters
        if fmt.lower() not in ['r', 'row', 'c', 'col', 'column']:
            raise ValueError('Invalid output format')

        # lookup the schema for this index and perform the query
        reader, query_s = profile(lib.query.fetch_all, config.s3_bucket, idx.s3_prefix)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            reader.set_limit(limit)

        # fetch the records from S3
        fetched_records, fetch_s, count = fetch_records(reader, fmt)
        needs_cont = not reader.at_end

        # make a continuation token if there are more records left to read
        cont_token = None if not needs_cont else lib.continuation.make_continuation(
            reader=reader,
            idx=index,
            fmt=fmt,
        )

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': index,
            'count': count,
            'progress': {
                'bytes_read': reader.bytes_read,
                'bytes_total': reader.bytes_total,
            },
            'page': 1,
            'limit': reader.limit,
            'data': fetched_records,
            'continuation': cont_token,
        }
    except KeyError:
        flask.abort(400, f'Invalid index: {index}')
    except ValueError as e:
        flask.abort(400, str(e))


@routes.route('/api/query/<index>')
def api_query(index):
    """
    Query the database for records matching the query parameter and
    read the records from s3.
    """
    try:
        q = parse_query(required=True)

        # optional data format and record limit
        fmt = flask.request.args.get('format', 'row')
        limit = flask.request.args.get('limit', type=int)

        # validate query parameters
        if q is None:
            raise ValueError('Missing query parameter')
        if fmt.lower() not in ['r', 'row', 'c', 'col', 'column']:
            raise ValueError('Invalid output format')

        # lookup the schema for this index and perform the query
        idx = config.index(index)
        reader, query_s = profile(lib.query.fetch, engine, config.s3_bucket, idx, q)

        # use a zip to limit the total number of records that will be read
        if limit is not None:
            reader.set_limit(limit)

        # fetch the records from s3
        fetched_records, fetch_s, count = fetch_records(reader, fmt)
        needs_cont = not reader.at_end

        # make a continuation token if there are more records left to read
        cont_token = None if not needs_cont else lib.continuation.make_continuation(
            reader=reader,
            idx=index,
            q=q,
            fmt=fmt,
        )

        return {
            'profile': {
                'query': query_s,
                'fetch': fetch_s,
            },
            'index': index,
            'q': q,
            'count': count,
            'progress': {
                'bytes_read': reader.bytes_read,
                'bytes_total': reader.bytes_total,
            },
            'page': 1,
            'limit': reader.limit,
            'data': fetched_records,
            'continuation': cont_token,
        }
    except KeyError:
        flask.abort(400, f'Invalid index: {index}')
    except ValueError as e:
        flask.abort(400, str(e))


@routes.route('/api/cont')
def api_cont():
    """
    Lookup a continuation token and get the next set of records.
    """
    try:
        token = flask.request.args['token']
        cont = lib.continuation.lookup_continuation(token)

        # fetch more records from S3
        fetched_records, fetch_s, count = fetch_records(cont.reader, cont.fmt)
        needs_cont = not cont.reader.at_end

        # remove the continuation
        token = lib.continuation.remove_continuation(token)

        # create another continuation
        if needs_cont:
            token = lib.continuation.next_continuation(cont)

        return {
            'profile': {
                'fetch': fetch_s,
            },
            'index': cont.idx,
            'q': cont.q,
            'count': count,
            'progress': {
                'bytes_read': cont.reader.bytes_read,
                'bytes_total': cont.reader.bytes_total,
            },
            'page': cont.page,
            'limit': cont.reader.limit,
            'data': fetched_records,
            'continuation': token,
        }
    except KeyError:
        flask.abort(400, f'Invalid, expired, or missing continuation token')
    except ValueError as e:
        flask.abort(400, str(e))


def parse_query(required=False):
    """
    Get the `q` query parameter and split it by comma into query parameters
    for a schema query.
    """
    q = flask.request.args.get('q')

    # some query parameters are required
    if required and q is None:
        raise ValueError('Missing query parameter')

    # if no query parameter is provided, assume empty string
    return q.split(',') if q else []


def fetch_records(reader, fmt):
    """
    Reads up to RESPONSE_LIMIT bytes from a RecordReader and format
    them before returning.

    Returns the records, how long it took (in seconds), along with the
    count of how many were read.

    NOTE: Use the count returned and NOT len(records), since the
    length of column-major format will the number of columns and not
    the number of records!
    """
    bytes_limit = reader.bytes_read + RESPONSE_LIMIT

    # keep reading records as long as the bytes read is under the limit
    take = itertools.takewhile(lambda x: reader.bytes_read < bytes_limit, reader.records)

    # profile how long it takes to fetch the records from s3
    fetched_records, fetch_s = profile(list, take)
    count = len(fetched_records)

    # transform a list of dictionaries into a dictionary of lists
    if fmt.lower() in ['c', 'col', 'column']:
        fetched_records = {k: [r.get(k) for r in fetched_records] for k in fetched_records[0].keys()}

    # return the fetched records
    return fetched_records, fetch_s, count
