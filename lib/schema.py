from sqlalchemy import Column, Index, Integer, BigInteger, MetaData, String, Table
from lib.locus import parse_columns


class Schema:
    """
    Each table has a particular schema associated with it that defines
    how is is indexed. The two supported schemas are by locus and by
    value.

    Multiple indexes can be built per index. To do this, indexes are
    separated by the '|' character. Some example schemas:

    "phenotype"
    "varId|dbSNP"
    "chr:pos"
    "chromosome:start-stop"
    "phenotype,chromosome:start-stop"
    "consequence,chromosome,gene|transcript"
    """

    def __init__(self, schema_str):
        """
        Initialize a new schema object from a schema string. The schema
        string is a comma-separated list of columns that act as a compound
        index. If one of the "columns" is a locus string, then those
        columns are combined together.

        It is an error to have a locus column index in any position other
        than the final position in a compound index!
        """
        self.schema_str = schema_str
        self.schema_columns = [s.strip() for s in schema_str.split(',')]
        self.index_columns = []
        self.key_columns = []

        # if this table is indexed by locus, track the class and columns
        self.locus_class = None
        self.locus_columns = None

        # add table columns that will be indexed
        for column in self.schema_columns:
            if self.locus_class is not None:
                raise ValueError(f'Invalid schema (locus must be last): {self.schema_str}')

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

        # ensure a valid schema exists
        if len(self.key_columns) == 0 and self.locus_class is None:
            raise ValueError(f'Invalid schema (no keys or locus specified)')

        # index building helpers
        self.index_keys = _index_keys(self.key_columns)
        self.index_builder = _index_builder(self.index_keys, self.locus_class, self.locus_columns)

    def __str__(self):
        return self.schema_str

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
            Column('start_offset', BigInteger),
            Column('end_offset', BigInteger),
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
        assert self.locus_class is not None, f'Index schema does not have a locus: {self.schema_str}'

        # instantiate the locus for this row
        return self.locus_class(*(row.get(col) for col in self.locus_columns if col))

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


def _index_keys(columns):
    """
    Take all the key columns and build a list of possible index
    tuples that can arise from them.

    For example, given the following schema: "varId|dbSNP,gene"
    these are the possible index key lists:

    [['varId', 'gene'],
     ['dbSNP', 'gene']]
    """
    keys = [k.split('|') for k in columns]

    # recursively go through the keys
    def build_keys(primary, secondary):
        for key in primary:
            if len(secondary) == 0:
                yield [key]
            else:
                for rest in build_keys(secondary[0], secondary[1:]):
                    yield [key, *rest]

    # generate a list of all possible key tuples
    return list(build_keys(keys[0], keys[1:])) if len(keys) > 0 else []


def _index_builder(index_keys, locus_class=None, locus_columns=None):
    """
    Returns a function that - given a row - returns the key generator.

    For example, with the following schema: "varId|dbSNP,gene", the
    index keys are [['varId', 'gene'], ['dbSNP', 'gene']]. So, for
    any row, up to two index records can be generated.

    If this schema also has a locus, then each index record may also
    contain additional indexed records for the loci as well.
    """
    def build_index_key(row):
        indexed_tuples = list(filter(all, [tuple(row.get(k) for k in keys) for keys in index_keys]))
        loci = None

        # if there's a locus in the schema, match it
        if locus_class:
            loci = locus_class(*(row[col] for col in locus_columns if col)).loci()

        # if no index keys, just yield the loci
        if len(index_keys) == 0:
            yield from loci
        else:
            if len(indexed_tuples) == 0:
                raise ValueError(f"Row failed to match schema")

            # if no locus in the schema yield only the indexed keys
            if locus_class is None:
                yield from indexed_tuples
            else:
                for indexed_tuple in indexed_tuples:
                    for locus in loci:
                        yield indexed_tuple + locus

    return build_index_key
