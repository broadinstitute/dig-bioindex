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
rds_client = boto3.client('rds', config=aws_config)
secrets_client = boto3.client('secretsmanager', config=aws_config)


def get_bgzip_job_status(job_id: str):
    batch_client = boto3.client('batch')
    job_response = batch_client.describe_jobs(jobs=[job_id])
    if len(job_response['jobs']) > 0:
        return job_response['jobs'][0]['status']
    return None


def start_decompress_job(index_name: str, s3_path: str):
    return start_batch_job(index_name, s3_path, 'unbgzip-job')


def start_file_deletion_job(index_name: str, s3_path: str):
    return start_batch_job(index_name, s3_path, 'json-delete-job')


def start_compress_job(index_name: str, s3_path: str):
    return start_batch_job(index_name, s3_path, 'bgzip-job')


def start_batch_job(index_name: str, s3_path: str, job_definition: str):
    batch_client = boto3.client('batch')
    parameters = {'index': index_name, 'path': s3_path, 'bucket': 'dig-bio-index'}

    response = batch_client.submit_job(
        jobName=job_definition,
        jobQueue='bgzip-job-queue',
        jobDefinition=job_definition,
        parameters=parameters
    )
    return response['jobId']


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


def describe_rds_instance(instance_name):
    """
    Returns a dictionary with the engine, host, and port information
    for the requested RDS instance.
    """
    response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_name)
    instances = response['DBInstances']

    if not instances or len(instances) > 1:
        raise RuntimeError('Either zero or more than one RDS instance found')

    return {
        'name': instance_name,
        'engine': instances[0]['Engine'],
        'host': instances[0]['Endpoint']['Address'],
        'port': instances[0]['Endpoint']['Port'],

        # optional values
        'dbname': instances[0].get('DBName'),
    }


def connect_to_db(schema=None, **kwargs):
    """
    Connect to a MySQL database using keyword arguments.
    """
    if not schema:
        schema = kwargs.get('dbname')

    # build the connection uri
    uri = '{engine}://{username}:{password}@{host}/{schema}?local_infile=1'.format(schema=schema, **kwargs)

    # create the connection pool
    engine = sqlalchemy.create_engine(uri, pool_recycle=3600)

    # test the engine by making a single connection
    with engine.connect():
        return engine


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
