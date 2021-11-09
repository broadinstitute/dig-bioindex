import functools
import logging
import os
import sys
import urllib

from .aws import describe_rds_instance, secret_lookup
from .locus import RegionLocus
from .utils import read_gff


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
        try:
            if self.bioindex_env is not None:
                secret = secret_lookup(self.bioindex_env)
                assert secret, f'Failed to lookup secret {self.bioindex_env}'

                # set environment keys if not already set
                Config.set_default_env(secret)

            # use keyword arguments if environment not yet set
            Config.set_default_env(kwargs)

            # validate required settings
            assert self.s3_bucket, 'BIOINDEX_S3_BUCKET not set in the environment'
            assert self.rds_config, 'BIOINDEX_RDS_SECRET nor BIOINDEX_RDS_INSTANCE set in the environment'
            assert self.bio_schema, 'BIOINDEX_BIO_SCHEMA not set in the environment'
        except AssertionError as ex:
            logging.error(ex)
            sys.exit(-1)

    @staticmethod
    def set_default_env(env):
        """
        Set environment variables, but only if not already set.
        """
        for k, v in env.items():
            if not os.getenv(k):
                os.environ[k] = v

    @functools.cached_property
    def rds_config(self):
        """
        Builds the RDS configuration from the environment.
        """
        if self.rds_secret:
            secret = secret_lookup(self.rds_secret)
            assert secret, f'Failed to lookup secret {self.rds_secret}'

            # set the name of the RDS instance
            secret['name'] = secret.pop('dbInstanceIdentifier')
            return secret

        # no instance specified
        if not self.rds_instance:
            return None

        # ensure the username and password are both set
        assert self.rds_username, 'BIOINDEX_RDS_USERNAME is not set in the environment'
        assert self.rds_password, 'BIOINDEX_RDS_PASSWORD is not set in the environment'

        # use the instance name to look up information
        instance = describe_rds_instance(self.rds_instance)

        # return the configuration
        return {
            'username': self.rds_username,
            'password': self.rds_password,
            **instance,
        }

    @functools.cached_property
    def genes_dict(self):
        """
        Builds a dictionary of genes.
        """
        genes = dict()
        logging.info('Building gene dictionary...')

        if not self.genes_uri:
            logging.warn('No BIOINDEX_GENES_URI; no gene dictionary built...')
            return genes

        # if a local file, just use the path, otherwise the entire uri
        uri = urllib.parse.urlparse(self.genes_uri)
        if not uri.scheme or uri.scheme == 'file':
            uri = uri.path

        # open the file, which may be remote
        for chromosome, source, typ, start, end, attributes in read_gff(uri):
            region = RegionLocus(chromosome, start, end)
            symbol = attributes.get('ID') or attributes.get('Name')
            alias = attributes.get('Alias')

            # add to the gene dictionary
            if symbol:
                genes[symbol.upper()] = region

            # add any aliases as well
            if alias:
                for symbol in alias.split(','):
                    genes[symbol.strip().upper()] = region

        return genes

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
    def rds_secret(self):
        return 'BIOINDEX_RDS_SECRET'

    @property
    @config_var()
    def rds_instance(self):
        return 'BIOINDEX_RDS_INSTANCE'

    @property
    @config_var()
    def rds_username(self):
        return 'BIOINDEX_RDS_USERNAME'

    @property
    @config_var()
    def rds_password(self):
        return 'BIOINDEX_RDS_PASSWORD'

    @property
    @config_var()
    def lambda_function(self):
        return 'BIOINDEX_LAMBDA_FUNCTION'

    @property
    @config_var(default='bio')
    def bio_schema(self):
        return 'BIOINDEX_BIO_SCHEMA'

    @property
    @config_var()
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

    @property
    @config_var(default='genes/genes.gff.gz')
    def genes_uri(self):
        return 'BIOINDEX_GENES_URI'

    @property
    @config_var(default='s3://dig-analysis-data/out/ldsc/regions/merged/')
    def regions_dir(self):
        return 'BIOINDEX_REGIONS_DIR'
