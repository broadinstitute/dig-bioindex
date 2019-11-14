import collections
import csv
import dataclasses
import json
import smart_open

from .s3 import *


csv.register_dialect('csv')
csv.register_dialect('tsv', delimiter='\t')


@dataclasses.dataclass
class Table:
    """
    A s3 json-lines object that maps to a redis key space and uses the
    columns in locus to map to a position or region.
    """
    path: str
    key: str
    locus: str
    dialect: str
    fieldnames: list = None

    def __post_init__(self):
        """
        Ensures the validity of the data passed in.
        """
        assert self.dialect == 'json' or self.dialect in csv.list_dialects()

    def reader(self, file):
        """
        Create a line-reader that parses records into dictionaries.
        """
        if self.dialect == 'json':
            return JSONReader(file)

        return CSVReader(file, self.dialect, self.fieldnames)

    def exists(self, bucket):
        """
        Returns False if this table doesn't exist in s3.
        """
        return s3_test_object(bucket, self.path)


class LineStream:
    """
    A wrapper around a file-like that iterates over lines, but also tracks the
    line offset (seek position within the file) and length.
    """

    def __init__(self, file, comment=None):
        """
        Initialize with a location or file-like that can be opened with smart_open.
        If comment is set, any line beginning with that character is skipped.
        """
        self.file = smart_open.open(file)
        self.comment = comment
        self.offset = self.file.tell()
        self.length = 0

    def __iter__(self):
        """
        The reader is an iterator.
        """
        return self

    def __next__(self):
        """
        Yield the next line, but save it so we have it.
        """
        for line in self.file:
            self.offset += self.length
            self.length = len(line)

            # skip if the line is empty or a comment
            if self.length == 0 or (self.comment and line.startswith(self.comment)):
                continue

            return line

        # got here because the loop is complete
        raise StopIteration

    def tell(self):
        """
        Returns the current offset.
        """
        return self.offset


class JSONReader:
    """
    A csv.reader()-like that reads the next line from a file-like and
    parses it as JSON.
    """

    def __init__(self, file):
        """
        Keep the file like so
        """
        self.file = file
        self.decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)

    def __iter__(self):
        """
        The reader is an iterator.
        """
        return self

    def __next__(self):
        """
        Read the next line and parse it as JSON.
        """
        return self.decoder.decode(next(self.file))


class CSVReader:
    """
    Given a CSV dialect and a file-like object, read each line and
    parse it as a CSV record, returning a dict.
    """

    def __init__(self, file, dialect, fieldnames):
        """
        If the header is not provided, then - like Spark - column names
        of '_0', '_1', '_2', etc. will be generated for each record.
        """
        if fieldnames:
            self.reader = csv.DictReader(file, dialect=dialect, fieldnames=fieldnames)
        else:
            self.reader = csv.reader(file, dialect=dialect)

    def __iter__(self):
        """
        The reader is an iterator.
        """
        return self

    def __next__(self):
        """
        Read the next line from the CSV source and parse it.
        """
        row = next(self.reader)

        # map a row with no header to named columns
        if isinstance(row, list):
            row = collections.OrderedDict((f'_{i}' % i, x) for (i, x) in enumerate(row))

        # map the index to a numeric column name
        return row
