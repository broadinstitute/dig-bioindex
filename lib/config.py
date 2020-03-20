import json
import os
import re
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
            self.settings = json.load(fp)

            # post-load fix-up
            for name, index in self.indexes.items():
                index['schema'] = Schema(index['schema'])

                # use the index name as the table name by default
                index['table'] = index.get('table', cap_case_str(name))

    @property
    def s3_bucket(self):
        return self.settings['s3_bucket']

    @property
    def rds_instance(self):
        return self.settings['rds_instance']

    @property
    def indexes(self):
        return self.settings['indexes']

    def index(self, name):
        return types.SimpleNamespace(**self.indexes[name])


def cap_case_str(s):
    """
    Translate a string like "foo_Bar-baz  whee" and return "FooBarBazWhee".
    """
    return re.sub(r'(?:[_\-\s]+|^)(.)', lambda m: m.group(1).upper(), s)
