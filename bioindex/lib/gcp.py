import sqlalchemy.engine

def describe_rds_instance(instance_name):
    """
    Returns a dictionary with the engine, host, and port information
    for the requested RDS instance.
    """

    return {
        'name': '',
        'engine': '',
        'host': '',
        'port': '',

        # optional values
        'dbname': '',
    }


def secret_lookup(secret_id):
    """
    Return the contents of a secret.
    """
    return {}


def start_batch_job(index_name: str, s3_path: str, job_definition: str, additional_parameters: dict = None):
    return ""


def get_bgzip_job_status(job_id: str):
    return None


def invoke_lambda(function_name, payload):
    """
    Invokes an AWS lambda function and waits for it to complete.
    """
    return ''


def start_and_wait_for_indexer_job(file: str, index: str, arity: int, bucket: str, rds_secret: str, rds_schema: str,
                                   size: int):
    return {}


def connect_to_db(schema=None, **kwargs):
    """
    Connect to a MySQL database using keyword arguments.
    """
    if not schema:
        schema = kwargs.get('dbname')

    # build the connection uri
    uri = '{engine}+pymysql://{username}:{password}@{host}/{schema}?local_infile=1'.format(schema=schema, **kwargs)

    # create the connection pool
    engine = sqlalchemy.create_engine(uri, pool_recycle=3600)

    # test the engine by making a single connection
    with engine.connect():
        return engine