import base64
import boto3
import botocore.config
import json
import sqlalchemy.engine


# create an AWS session from ~/.aws credentials
aws_config = botocore.config.Config(max_pool_connections=200)
aws_session = boto3.session.Session()

# create service clients
lambda_client = aws_session.client('lambda')
s3_client = aws_session.client('s3', config=aws_config)
secrets_client = aws_session.client('secretsmanager')


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
    return json.loads(secret)


def connect_to_rds(secret_id, schema=None):
    """
    Create and return a connection to a MySQL server.
    """
    secret = secret_lookup(secret_id)

    # extract connection settings
    host = secret['host']
    user = secret['username']
    password = secret['password']
    db = schema or secret['dbname']

    # mysql connection url
    connection_string = 'mysql://{login}:{password}@{host}/{db}?local_infile=1'.format(
        login=user,
        password=password,
        host=host,
        db=db,
    )

    # create the connection pool
    return sqlalchemy.create_engine(connection_string, pool_recycle=3600)


def invoke_lambda(function_name, payload):
    """
    Invokes an AWS lambda function and waits for it to complete.
    """
    return lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(payload),
    )
