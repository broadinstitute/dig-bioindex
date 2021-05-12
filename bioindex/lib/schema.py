from sqlalchemy import Column, Index, Integer, BigInteger, MetaData, String, Table
from sqlalchemy.exc import OperationalError

from .locus import parse_locus_builder


class Schema:
    """
    Each table has a particular schema associated with it that defines
    how is is indexed. This is a compound, comma-separated list of fields
    that are indexed together. The order of the fields matters and is
    the order of the compound index.

    A locus index (e.g. chromosome:position or chromosome:start-stop) is
    special an MUST appear last. Locus fields are actually multiple fields
    combined together. It must either be a chromosome:position combination
    or chromosome:start-stop combination, where each element is the name
    of the field being endexed (e.g. "chr:pos" and "chr:start-end" are both
    valid).

    Optionally, instead of providing individual columns for a locus, a
    column with a locus ID - which is able to be parsed to get the locus -
    can be used instead. This has a benefits of allowing the locus ID to
    also be used at query time and the locus format to be specific to the
    data being indexed. To do this, the locus index must be a format of
    column=template. The locus ID template format expects the following
    fields to be present:

    * $chr   - the chromosome
    * $pos   - the SNP position if a SNPLocus
    * $start - the start position if a RegionLocus
    * $stop  - the end position if a RegionLocus

    If the last character in the template is a * that indicates the rest
    of the id can be ignored. For example, the following would be a valid
    templates:

      varId=$chr:$pos*
      regionId=region_$chr:$start-$stop

    Finally, multiple indexes can be built as well using different columns.
    To do this, indexes are separated by the '|' character. Some example
    schemas:

    "phenotype"
    "varId|dbSNP"
    "chr:pos"
    "chrom:pos"
    "chromosome:start-stop"
    "varId=$chr:$pos"
    "annotation,region=region_$chr/$start/$stop"
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

            # parse the index "column" for a locus class builder and list of columns
            self.locus_class, self.locus_columns = parse_locus_builder(column)

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

    @property
    def locus_is_template(self):
        """
        True if this locus is a template extracted from a single column.
        """
        return len(self.locus_columns) == 1

    def table_def(self, name, meta):
        """
        Returns the table definition for this schema.
        """
        table_columns = [
            Column('id', Integer, primary_key=True),
            Column('key', Integer, index=True),
            Column('start_offset', BigInteger),
            Column('end_offset', BigInteger),
        ]

        return Table(name, MetaData(), *table_columns, *self.index_columns)

    def create_index(self, engine, table):
        """
        Construct the compound index for this table.
        """
        Index('schema_idx', *self.index_columns).create(engine)

    def drop_index(self, engine, table):
        """
        Removes the index. This can help performance when updating.
        """
        try:
            engine.execute(f'ALTER TABLE `{table.name}` DROP INDEX schema_idx')
        except OperationalError:
            pass

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
        indexed_tuples = (tuple(row.get(k) for k in keys) for keys in index_keys)
        indexed_tuples = [tup for tup in indexed_tuples if all(tup)]

        # if there's a locus in the schema, match it
        if locus_class:
            loci = locus_class(*(row[col] for col in locus_columns if col)).loci()
        else:
            loci = None

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
