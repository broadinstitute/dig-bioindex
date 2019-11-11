import abc
import csv
import dataclasses
import io
import json
import secrets


@dataclasses.dataclass()
class Schema(abc.ABC):
    """
    A schema defines how records are stored in s3, and is stored with
    the table definition in redis.

    The schema can either be JSONSchema, meaning there's nothing to
    do except parse it, or CSVSchema, which requires additional data
    defining the column order, delimiter, and types.

    Finally, all schemas must have locus columns identified so they can
    be referenced again when loading records.
    """
    locus: str

    @abc.abstractmethod
    def parse(self, line):
        """
        Returns a dictionary of the parsed record line.
        """
        pass


@dataclasses.dataclass()
class JSONSchema(Schema):
    """
    A JSONSchema is just a schema that uses JSON to parse each record.
    """

    def parse(self, line):
        """
        Parse the line as pure JSON.
        """
        return json.loads(line)


@dataclasses.dataclass()
class CSVSchema(Schema):
    """
    A CSVSchema requires ordered column names, delimiter, and a type map.
    """
    columns: list
    sep: str
    types: dict

    def __post_init__(self):
        """
        Create a CSV reader that can be used to parse records.
        """
        self._dialect = secrets.token_urlsafe()

        # create a new dialect for this schema with a unique, unshared name
        csv.register_dialect(self._dialect, delimiter=self.sep)

    def parse(self, line):
        record = io.StringIO(line)
        reader = csv.DictReader(record, fieldnames=self.columns, dialect=self._dialect)

        # parse the only line
        return next(reader)
