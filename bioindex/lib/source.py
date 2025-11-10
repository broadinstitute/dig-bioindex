import flummox.source

from ..lib.query import fetch


class BioIndexDataSource(flummox.source.DataSource):

    def __init__(self, engine, config, indexes, restricted=None):
        """
        Initialize the source.
        """
        self.engine = engine
        self.restricted = restricted
        self.config = config
        self.indexes = indexes

    def query(self, q, table):
        reader = fetch(
            self.engine,
            self.config.s3_bucket,
            self.indexes[table],
            [q.split(',')],
            restricted=self.restricted,
        )

        return reader.records
