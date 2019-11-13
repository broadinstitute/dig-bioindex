import dataclasses

from .s3 import *


@dataclasses.dataclass
class Table:
    """
    A s3 json-lines object that maps to a redis key space and uses the
    columns in locus to map to a position or region.
    """
    path: str
    key: str
    locus: str

    def exists(self, bucket):
        """
        Returns False if this table doesn't exist in s3.
        """
        return s3_test_object(bucket, self.path)
