import dataclasses

from .s3 import *


@dataclasses.dataclass
class Table:
    """
    An s3 json-lines object (bucket+path) that maps to a redis key space
    and uses the columns in locus to map to a position or region in the
    key space.
    """
    bucket: str
    path: str
    key: str
    locus: str

    def exists(self):
        """
        Returns False if this table doesn't exist in s3.
        """
        return s3_test_object(self.bucket, self.path)
