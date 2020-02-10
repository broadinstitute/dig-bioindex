import abc

from sqlalchemy import Column, Index, Integer, MetaData, String, Table
from lib.locus import parse_columns


class Schema(abc.ABC):
    """
    Each table has a particular schema associated with it that defines
    how is is indexed. The two supported schemas are by locus and by
    value.
    """

    common_columns = [
        Column('id', Integer, primary_key=True),
        Column('path', String(1024)),
        Column('start_offset', Integer),
        Column('end_offset', Integer),
    ]

    @classmethod
    def from_string(cls, schema):
        """
        Act as a base constructor for either a Locus schema or a Value
        schema derived from the string.
        """
        locus_class, locus_columns = parse_columns(schema)

        if locus_class:
            return LocusSchema(locus_class, locus_columns)
        else:
            return ValueSchema(schema)

    @abc.abstractmethod
    def __str__(self):
        pass

    @abc.abstractmethod
    def table_columns(self):
        pass

    def build_table(self, name, meta):
        """
        Returns the table definition for this schema.
        """
        return Table(name, MetaData(), *self.common_columns, *self.table_columns())

    @abc.abstractmethod
    def build_index(self, engine, table):
        pass

    @abc.abstractmethod
    def index_keys(self, row):
        pass

    @abc.abstractmethod
    def column_values(self, index_key):
        pass


class LocusSchema(Schema):
    """
    A LocusSchema indexes a table by chromosome and position. It can either
    be a single locus (e.g. SNP) or a region (start-end).
    """

    def __init__(self, locus_class, locus_columns):
        """
        Initialize the schema with a locus class constructor and the list
        of columns used in the class constructor.
        """
        self.locus_class = locus_class
        self.locus_columns = locus_columns

    def __str__(self):
        """
        Returns the string stored in the database for table.
        """
        pos = f'{self.locus_columns[0]}:{self.locus_columns[1]}'

        # either a region or a single SNP position
        return f'{pos}-{self.locus_columns[2]}' if self.locus_columns[2] else pos

    def table_columns(self):
        """
        Define the columns for this schema.
        """
        return [
            Column('chromosome', String(4)),
            Column('position', Integer),
        ]

    def build_index(self, engine, table):
        """
        Build the index for the provided table.
        """
        Index('locus_idx', table.c.chromosome, table.c.position).create(engine)

    def locus_of_row(self, row):
        """
        Returns the locus class of a row in s3 for the columns of this schema.
        """
        return self.locus_class(*(row.get(col) for col in self.locus_columns if col))

    def index_keys(self, row):
        """
        LocusSchema objects divide the locus of a given row into multiple
        index keys. For more detail, see at Locus.loci().
        """
        return self.locus_of_row(row).loci()

    def column_values(self, index_key):
        """
        Return a dictionary of values for a given index key. Since a LocusSchema
        has index keys of (chromosome, position), for a given index key it will
        return a dictionary of {'chromosome': chr, 'position': pos}.
        """
        return {'chromosome': index_key[0], 'position': index_key[1]}


class ValueSchema(Schema):
    """
    A ValueSchema is a table indexed by a single columns value. It can be
    a string, integer, or float.
    """

    def __init__(self, column, column_type=String):
        """
        Initialize the schema with a chromosome column name, start position
        column name, and an optional stop position column name.
        """
        self.column = column
        self.column_type = column_type

    def __str__(self):
        """
        Returns the string stored in the database for table.
        """
        return self.column

    def table_columns(self):
        """
        Define the columns for this schema.
        """
        return [
            Column('value', self.column_type),
        ]

    def build_index(self, engine, table):
        """
        Build the index for the provided table.
        """
        Index('value_idx', table.value).create(engine)

    def index_keys(self, row):
        """
        ValueSchema objects only have a single index key per row.
        """
        yield row[self.column]

    def column_values(self, index_key):
        """
        Return a dictionary of values for a given index key. Since a ValueSchema
        has index keys of a single value, it is returned for the value column.
        """
        return {'value': index_key}
