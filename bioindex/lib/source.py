from pqs.lib import source

from ..lib.query import fetch


class BioIndexDataSource(source.DataSource):

    def __init__(self, engine, config, indexes, restricted=None):
        """
        Initialize the source.
        """
        self.engine = engine
        self.restricted = restricted
        self.config = config
        self.indexes = indexes

    def query(self, q, table):
        qs = q.split(',')
        index = self.indexes[table]
        reader = fetch(
            self.engine,
            self.config.s3_bucket,
            index,
            qs,
            restricted=self.restricted,
        )

        return reader.records
