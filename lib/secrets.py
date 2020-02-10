import base64
import boto3
import json
import sqlalchemy.engine


# create a session from ~/.aws credentials
secrets_session = boto3.session.Session()
secrets_client = secrets_session.client('secretsmanager')


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


def connect_to_mysql(secret_id):
    """
    Create and return a connection to a MySQL server.
    """
    secret = secret_lookup(secret_id)

    # extract connection settings
    host = secret['host']
    user = secret['username']
    password = secret['password']
    db = secret['dbname']

    # create the connection pool
    return sqlalchemy.create_engine(
        'mysql://{login}:{password}@{host}/{db}?local_infile=1'.format(
            login=user,
            password=password,
            host=host,
            db=db,
        )
    )
