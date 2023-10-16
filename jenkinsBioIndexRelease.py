# imports
import argparse
import base64
import concurrent.futures
import json
import os
import time

import boto3
import pymysql as mdb
from botocore.exceptions import ClientError

# script variables
# get the timestamp for the build
format = "%Y%m%d%H%M%S"
timestamp = time.strftime(format)

print("time stamp is: {}".format(timestamp))

# DB settings
schema_bio_dev = "bio"
schema_bio_new = 'bioindex_' + timestamp
schema_portal_dev = "portal"
schema_portal_new = 'portal_' + timestamp

# s3 settings
s3_bucket_new = 'dig-bio-index-' + timestamp
s3_bucket_dev = 'dig-bio-index-dev'

# git settings
code_directory = '/Users/mduby/BioIndex/'
git_directory = code_directory + 'bioindex_' + timestamp
git_clone_command = "git clone git@github.com:broadinstitute/dig-bioindex.git " + git_directory

# secrets settings
secret_name_dev = "bioindex-dev"
secret_name_new = "bioindex-" + timestamp
region_name = "us-east-1"

# keys for the environment file setting
file_temp_directory = "/Users/mduby"
file_name = ".bioindex"

# get the aws client and session
s3client = boto3.client('s3')


# method to run an OS command and time it
def run_system_command(os_command, if_test=True):
    log_message = "Running command"
    exit_code = None
    start = time.time()
    if if_test:
        log_message = "Testing command"
    print("{}: {}".format(log_message, os_command), flush=True)
    if not if_test:
        exit_code = os.system(os_command)
    end = time.time()
    print("    Done in {:0.2f}s with exit code {}".format(end - start, exit_code), flush=True)


def create_setting_file(s3_bucket, aws_secret, bio_schema, portal_schema, temp_dir, bio_file, if_test=True):
    '''
    Method to create the bioindex settings file
    '''
    file_location = temp_dir + "/" + bio_file
    file_contents = "{}={}\n{}={}\n{}={}\n{}={}\n".format('BIOINDEX_S3_BUCKET', s3_bucket, \
                                                          'BIOINDEX_RDS_SECRET', aws_secret, \
                                                          'BIOINDEX_BIO_SCHEMA', bio_schema, \
                                                          'BIOINDEX_PORTAL_SCHEMA', portal_schema)
    print("the bioindex settings file contents are: \n{}".format(file_contents))
    if if_test:
        print("test creating bioindex settings file {}".format(file_location))

    else:
        text_file = open(file_location, "w")
        text_file.write(file_contents)
        print("created bioindex settings file {}".format(file_location))


def header_print(message):
    print("\n==> {}".format(message), flush=True)


# method to list the buckets based on search string
def print_s3_buckets(s3client, search_str):
    # print all the bucket names
    list_buckets_resp = s3client.list_buckets()
    for bucket in list_buckets_resp['Buckets']:
        if search_str in bucket['Name']:
            print("existing bucket name after addition: {}".format(bucket['Name']), flush=True)


# method to retrive the secrets given name and region
def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    return json.loads(secret)


# method to take in secret and return tables
def show_tables(schema, username, password, host):
    '''returns the database table list from the database specified in the secret provided'''
    db = mdb.connect(host, username, password, schema)
    sql = "show tables"
    cursor = db.cursor()
    table_list = []

    # execute
    cursor.execute(sql)

    # fetch
    for row in cursor:
        table_list.append(row[0])

    # return
    return table_list


def clone_database(schema_dev, schema_new, aws_secret):
    # get the secret data
    mysql_user = aws_secret['username']
    mysql_password = aws_secret['password']
    mysql_host = aws_secret['host']

    # create the new database
    header_print("creating the new schema {}".format(schema_new))
    mysql_command_create_schema = "mysql -u {} -p'{}' -h {} -e \"create database {}\"".format(mysql_user,
                                                                                              mysql_password,
                                                                                              mysql_host, schema_new)
    run_system_command(mysql_command_create_schema, if_test=arg_if_test)

    # clone database
    # build the mysql schema cloning command
    header_print("copying data from schema {} to the new schema {}".format(schema_dev, schema_new))
    database_table_list = show_tables(schema_dev, mysql_user, mysql_password, mysql_host)
    with concurrent.futures.ThreadPoolExecutor(3) as db_executor:
        db_futures = []
        for table in database_table_list:
            mysql_command_dump = "mysqldump -u {} -p'{}' -h {} {} {}".format(mysql_user, mysql_password, mysql_host,
                                                                             schema_dev, table)
            mysql_command_load = "mysql -u {} -p'{}' -h {} {}".format(mysql_user, mysql_password, mysql_host,
                                                                      schema_new)
            mysql_command_combined = mysql_command_dump + " | " + mysql_command_load
            db_futures.append(db_executor.submit(run_system_command, mysql_command_combined, if_test=arg_if_test))
        for my_future in concurrent.futures.as_completed(db_futures):
            my_future.result()


def print_args(arg_map):
    for key in arg_map.keys():
        print("   {} ===> {}".format(key, arg_map[key]))


if __name__ == "__main__":
    # configure argparser
    parser = argparse.ArgumentParser("script to clone the dev bioindex data to the prod machine")
    # add the arguments
    parser.add_argument('-s', '--secret', help='the secret for the bioindex', default='dig-bio-index', required=False)
    parser.add_argument('-b', '--bucket', help='the s3 bucket to copy', default='dig-bio-index', required=False)
    parser.add_argument('-k', '--bio', help='the bioindex schema to clone', default='bio', required=False)
    parser.add_argument('-p', '--portal', help='the portal schema to clone', default='portal', required=False)
    parser.add_argument('-d', '--directory', help='the temp directory to use', required=True)
    parser.add_argument('-t', '--test', help='if this is a dryrun/test', default=True, required=False)
    # get the args
    args = vars(parser.parse_args())

    # print the command line arguments
    header_print("printing arguments used")
    print_args(args)

    # need passed in args:
    arg_if_test = True

    # set the parameters
    if args['secret'] is not None:
        secret_name_dev = args['secret']
    if args['bio'] is not None:
        schema_bio_dev = args['bio']
    if args['portal'] is not None:
        schema_portal_dev = args['portal']
    if args['bucket'] is not None:
        s3_bucket_dev = args['bucket']
    if args['directory'] is not None:
        file_temp_directory = str(args['directory'])
    if args['test'] is not None:
        arg_if_test = not args['test'] == 'False'

    header_print(
        "passed in bucket is {} AWS dev secret {} and ifTest {}".format(s3_bucket_dev, secret_name_dev, arg_if_test))
    header_print("using bioindex database {} and portal database {}".format(schema_bio_dev, schema_portal_dev))

    # get the secret to use to clone
    header_print("get the secret to clone")
    bio_secret_dev = get_secret(secret_name_dev, region_name)
    print("got secret with name {}".format(bio_secret_dev['dbInstanceIdentifier']), flush=True)

    # list the existing buckets before creating the new one
    header_print("listing existing s3 buckets")
    print_s3_buckets(s3client, 'index')

    # create the new s3 busket
    header_print("creating the new s3 bucket")
    # create the s3 bucket
    if not arg_if_test:
        s3client.create_bucket(Bucket=s3_bucket_new)
        print("created new s3 bucket {}".format(s3_bucket_new), flush=True)
    else:
        print("test, so skipped creating new s3 bucket {}".format(s3_bucket_new), flush=True)

    list_buckets_resp = s3client.list_buckets()
    for bucket in list_buckets_resp['Buckets']:
        if bucket['Name'] == s3_bucket_new:
            print('(Just created) --> {} - there since {}'.format(bucket['Name'], bucket['CreationDate']), flush=True)

    # list the existing buckets before creating the new one
    header_print("listing existing s3 buckets")
    print_s3_buckets(s3client, 'index')

    # sync the new s3 buckeet with the data from the given s3 bucket
    header_print("sub folders of {} that need to be cloned".format(s3_bucket_dev))
    result = s3client.list_objects(Bucket=s3_bucket_dev, Prefix="", Delimiter='/')
    for s3object in result.get('CommonPrefixes'):
        print("-> sub folder: {}".format(s3object.get('Prefix')), flush=True)

    # log
    header_print("cloning s3 bucket {}".format(s3_bucket_dev))
    all_futures = []
    # copy the data
    with concurrent.futures.ThreadPoolExecutor(3) as executor:
        for s3object in result.get('CommonPrefixes'):
            s3_subdirectory = s3object.get('Prefix')
            s3_command = "aws s3 sync --no-progress --quiet s3://{}/{} s3://{}/{}".format(s3_bucket_dev,
                                                                                          s3_subdirectory,
                                                                                          s3_bucket_new,
                                                                                          s3_subdirectory)
            all_futures.append(executor.submit(run_system_command, s3_command, if_test=arg_if_test))

        # clone the databases concurrently with s3 sync
        all_futures.append(executor.submit(clone_database, schema_portal_dev, schema_portal_new, bio_secret_dev))
        all_futures.append(executor.submit(clone_database, schema_bio_dev, schema_bio_new, bio_secret_dev))

        for future in concurrent.futures.as_completed(all_futures):
            future.result()

    # create the settings file
    header_print("create the bioindex settings file")
    create_setting_file(s3_bucket_new, secret_name_dev, schema_bio_new, schema_portal_new, file_temp_directory,
                        file_name, arg_if_test)

    header_print("DONE\n\n\n")
