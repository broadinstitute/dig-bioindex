import json
import os
import types

from lib.schema import Schema


class Config:
    """
    Configuration file.
    """

    def __init__(self):
        """
        Loads the configuration file using environment.
        """
        with open(os.getenv('BIOINDEX_CONFIG', 'config.json')) as fp:
            self.index = json.load(fp)

    @property
    def s3_bucket(self):
        return self.index['s3_bucket']

    @property
    def rds_instance(self):
        return self.index['rds_instance']

    @property
    def tables(self):
        return self.index['tables']

    def table(self, name):
        table = self.tables[name]

        # convert the dictionary into a simple namespace object
        return types.SimpleNamespace(
            prefix=table['s3_prefix'],
            schema=Schema(table['schema']),
        )
