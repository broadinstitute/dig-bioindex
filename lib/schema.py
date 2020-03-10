import abc

from sqlalchemy import Column, Index, Integer, MetaData, String, Table
from lib.locus import parse_columns


class Schema(abc.ABC):
    """
    Each table has a particular schema associated with it that defines
    how is is indexed. The two supported schemas are by locus and by
    value.
    """

    def __init__(self, schema):
        """
        Initialize a new schema object from a schema string. The schema
        string is a comma-separated list of columns that act as a compound
        index. If one of the "columns" is a locus string, then those
        columns are combined together.

        It is an error to have a locus column index in any position other
        than the final position in a compound index!
        """
        self.schema = schema
        self.schema_columns = schema.split(',')
        self.index_columns = []
        self.key_columns = []

        # if this table is indexed by locus, track the class and columns
        self.locus_class = None
        self.locus_columns = None

        # add table columns that will be indexed
        for column in self.schema_columns:
            assert self.locus_class is None, f'Invalid index schema: {self.schema}'

            # is this "column" name a locus?
            self.locus_class, self.locus_columns = parse_columns(column)

            # append either locus or value columns
            if self.locus_class:
                self.index_columns += [
                    Column('chromosome', String(4)),
                    Column('position', Integer),
                ]
            else:
                self.index_columns.append(Column(column, String(200)))
                self.key_columns.append(column)

        # convert to tuples
        self.key_columns = tuple(self.key_columns)
        self.index_columns = tuple(self.index_columns)

    def __str__(self):
        return self.schema

    @property
    def has_locus(self):
        """
        True if this schema has a locus in the index.
        """
        return self.locus_class is not None

    def build_table(self, name, meta):
        """
        Returns the table definition for this schema.
        """
        table_columns = [
            Column('id', Integer, primary_key=True),
            Column('path', String(1024)),
            Column('start_offset', Integer),
            Column('end_offset', Integer),
        ]

        return Table(name, MetaData(), *table_columns, *self.index_columns)

    def build_index(self, engine, table):
        """
        Construct the compound index for this table.
        """
        Index('schema_idx', *self.index_columns).create(engine)

    def locus_of_row(self, row):
        """
        Returns the locus class of a row in s3 for the columns of this schema.
        """
        assert self.locus_class is not None, f'Index schema has not locus: {self.schema}'

        # instantiate the locus for this row
        return self.locus_class(*(row.get(col) for col in self.locus_columns if col))

    def index_keys(self, row):
        """
        A generator of list, where each tuple consists of the value for this
        index. A single row may produce multiple values.
        """
        if self.locus_class:
            for locus in self.locus_of_row(row).loci():
                yield tuple(row[k] for k in self.key_columns) + locus
        else:
            yield self.key_columns

    def column_values(self, index_key):
        """
        Given a tuple yielded by index_keys, convert it into a map of the actual
        column names and values.
        """
        return {c.name: v for c, v in zip(self.index_columns, index_key)}

    @property
    def sql_filters(self):
        """
        Builds the query string from the index columns that can be used in a
        SQL execute statement.
        """
        tests = 'AND'.join(map(lambda k: f'`{k}`=%s ', self.key_columns))

        # if there's a locus index, append it
        if self.has_locus:
            if tests != '':
                tests += 'AND '

            # add the chromosome and position
            tests += '`chromosome`=%s AND `position` BETWEEN %s AND %s '

        return tests

    @property
    def arity(self):
        """
        Returns the number of expected query arguments.
        """
        return len(self.key_columns) + (1 if self.has_locus else 0)
