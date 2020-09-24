import base64
import boto3
import botocore.config
import orjson
import sqlalchemy.engine

# allow lots of connections and time to read
aws_config = botocore.config.Config(
    max_pool_connections=200,
    read_timeout=900,
)

# create service clients
lambda_client = boto3.client('lambda', config=aws_config)
s3_client = boto3.client('s3', config=aws_config)
secrets_client = boto3.client('secretsmanager', config=aws_config)


def secret_lookup(secret_id):
    """
    Return the contents of a secret.
    """
    response = secrets_client.get_secret_value(SecretId=secret_id)
    secret = response.get('SecretString')

    # check for encoded secret
    if not secret:
        secret = base64.b64decode(response['SecretBinary'])

    # parse it as json
    return orjson.loads(secret)


def connect_to_db(**kwargs):
    """
    Connect to a MySQL database using keyword arguments.
    """
    uri = '{engine}://{username}:{password}@{host}/{dbname}?local_infile=1'.format(**kwargs)

    # create the connection pool
    return sqlalchemy.create_engine(uri, pool_recycle=3600)


def connect_to_rds(secret_id, schema=None):
    """
    Create and return a connection to a RDS server using an AWS secret.
    """
    secret = secret_lookup(secret_id)

    return connect_to_db(
        engine = secret['engine'],
        host = secret['host'],
        port = secret['port'],
        username = secret['username'],
        password = secret['password'],
        dbname = schema or secret['dbname'],
    )


def invoke_lambda(function_name, payload):
    """
    Invokes an AWS lambda function and waits for it to complete.
    """
    payload = orjson.dumps(payload).decode('utf-8')

    # invoke and wait for response
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=payload,
    )

    # parse the response payload
    payload = orjson.loads(response['Payload'].read())

    # if a failure, then raise an exception
    if response.get('FunctionError'):
        raise RuntimeError(payload)

    return payload['body']
