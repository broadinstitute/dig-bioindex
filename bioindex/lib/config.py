import os

from .aws import secret_lookup


def config_var(type=str, default=None):
    """
    Wrap a property that returns a string so that it reads from
    various places where that value may be set.
    """
    def decorator(f):
        def wrapper(*args):
            key = f(*args)
            val = os.environ.get(key, default)

            # cast to the appropriate type
            return val and type(val)

        return wrapper
    return decorator


class Config:
    """
    Configuration file.
    """

    def __init__(self, **kwargs):
        """
        Loads the configuration file using environment.
        """
        if self.bioindex_env is not None:
            secret = secret_lookup(self.bioindex_env)

            # set environment keys if not already set
            if secret:
                Config.set_default_env(secret)

        # use keyword arguments if environment not yet set
        Config.set_default_env(kwargs)

        # validate required settings
        assert self.s3_bucket, 'BIOINDEX_S3_BUCKET not set in the environment'
        assert self.rds_instance, 'BIOINDEX_RDS_INSTANCE not set in the environment'
        assert self.bio_schema, 'BIOINDEX_BIO_SCHEMA not set in the environment'

    @staticmethod
    def set_default_env(env):
        """
        Set environment variables, but only if not already set.
        """
        for k, v in env.items():
            if not os.getenv(k):
                os.environ[k] = v

    @property
    @config_var()
    def bioindex_env(self):
        return 'BIOINDEX_ENVIRONMENT'

    @property
    @config_var()
    def s3_bucket(self):
        return 'BIOINDEX_S3_BUCKET'

    @property
    @config_var()
    def rds_instance(self):
        return 'BIOINDEX_RDS_INSTANCE'

    @property
    @config_var()
    def lambda_function(self):
        return 'BIOINDEX_LAMBDA_FUNCTION'

    @property
    @config_var(default='bio')
    def bio_schema(self):
        return 'BIOINDEX_BIO_SCHEMA'

    @property
    @config_var(default='portal')
    def portal_schema(self):
        return 'BIOINDEX_PORTAL_SCHEMA'

    @property
    @config_var(default='schema.graphql')
    def graphql_schema(self):
        return 'BIOINDEX_GRAPHQL_SCHEMA'

    @property
    @config_var(default=1 * 1024 * 1024, type=int)
    def response_limit(self):
        return 'BIOINDEX_RESPONSE_LIMIT'

    @property
    @config_var(default=100 * 1024 * 1024, type=int)
    def response_limit_max(self):
        return 'BIOINDEX_RESPONSE_LIMIT_MAX'

    @property
    @config_var(default=100, type=int)
    def match_limit(self):
        return 'BIOINDEX_MATCH_LIMIT'

    @property
    @config_var(default=10, type=float)
    def script_timeout(self):
        return 'BIOINDEX_SCRIPT_TIMEOUT'
