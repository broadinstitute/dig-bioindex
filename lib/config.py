import os

from lib.secrets import secret_lookup


class Config:
    """
    Configuration file.
    """

    def __init__(self):
        """
        Loads the configuration file using environment.
        """
        self.bioindex_env = os.getenv('BIOINDEX_ENVIRONMENT')

        # default settings
        s3_bucket = None
        rds_instance = None
        response_limit = 1 * 1024 * 1024
        match_limit = 1000

        # load the secret, which contains the environment
        if self.bioindex_env is not None:
            secret = secret_lookup(self.bioindex_env)

            # the secret overrides the defaults
            s3_bucket = secret.get('BIOINDEX_S3_BUCKET', s3_bucket)
            rds_instance = secret.get('BIOINDEX_RDS_INSTANCE', rds_instance)
            response_limit = secret.get('BIOINDEX_RESPONSE_LIMIT', response_limit)
            match_limit = secret.get('BIOINDEX_MATCH_LIMIT', match_limit)

        # the local environment overrides the secret
        self.s3_bucket = os.getenv('BIOINDEX_S3_BUCKET', s3_bucket)
        self.rds_instance = os.getenv('BIOINDEX_RDS_INSTANCE', rds_instance)
        self.response_limit = os.getenv('BIOINDEX_RESPONSE_LIMIT', response_limit)
        self.match_limit = os.getenv('BIOINDEX_RESPONSE_LIMIT', match_limit)

        # validate required settings
        assert self.s3_bucket
        assert self.rds_instance

        # post-init
        self.response_limit = int(response_limit)
        self.match_limit = int(match_limit)
