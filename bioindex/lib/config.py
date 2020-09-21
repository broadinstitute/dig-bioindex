import os

from .aws import secret_lookup


class Config:
    """
    Configuration file.
    """

    def __init__(self, **kwargs):
        """
        Loads the configuration file using environment.
        """
        self.bioindex_env = os.getenv('BIOINDEX_ENVIRONMENT', kwargs.get('BIOINDEX_ENVIRONMENT'))

        # default settings
        s3_bucket = kwargs.get('BIOINDEX_S3_BUCKET')
        rds_instance = kwargs.get('BIOINDEX_RDS_INSTANCE')
        lambda_function = kwargs.get('BIOINDEX_LAMBDA_FUNCTION')
        bio_schema = kwargs.get('BIOINDEX_BIO_SCHEMA', 'bio')
        portal_schema = kwargs.get('BIOINDEX_PORTAL_SCHEMA', 'portal')
        response_limit = kwargs.get('BIOINDEX_RESPONSE_LIMIT', 1 * 1024 * 1024)
        match_limit = kwargs.get('BIOINDEX_MATCH_LIMIT', 1000)

        # load the secret, which contains the environment
        if self.bioindex_env is not None:
            secret = secret_lookup(self.bioindex_env)

            # the secret overrides the defaults
            s3_bucket = secret.get('BIOINDEX_S3_BUCKET', s3_bucket)
            rds_instance = secret.get('BIOINDEX_RDS_INSTANCE', rds_instance)
            lambda_function = secret.get('BIOINDEX_LAMBDA_FUNCTION', lambda_function)
            response_limit = secret.get('BIOINDEX_RESPONSE_LIMIT', response_limit)
            match_limit = secret.get('BIOINDEX_MATCH_LIMIT', match_limit)
            bio_schema = secret.get('BIOINDEX_BIO_SCHEMA', bio_schema)
            portal_schema = secret.get('BIOINDEX_PORTAL_SCHEMA', portal_schema)

        # the local environment overrides the secret
        self.s3_bucket = os.getenv('BIOINDEX_S3_BUCKET', s3_bucket)
        self.rds_instance = os.getenv('BIOINDEX_RDS_INSTANCE', rds_instance)
        self.lambda_function = os.getenv('BIOINDEX_LAMBDA_FUNCTION', lambda_function)
        self.response_limit = os.getenv('BIOINDEX_RESPONSE_LIMIT', response_limit)
        self.match_limit = os.getenv('BIOINDEX_RESPONSE_LIMIT', match_limit)
        self.bio_schema = os.getenv('BIOINDEX_BIO_SCHEMA', bio_schema)
        self.portal_schema = os.getenv('BIOINDEX_PORTAL_SCHEMA', portal_schema)

        # validate required settings
        assert self.s3_bucket, 'BIOINDEX_S3_BUCKET not set in the environment'
        assert self.rds_instance, 'BIOINDEX_RDS_INSTANCE not set in the environment'
        assert self.bio_schema, 'BIOINDEX_BIO_SCHEMA not set in the environment'

        # post-init
        self.response_limit = int(response_limit)
        self.match_limit = int(match_limit)
