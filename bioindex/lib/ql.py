import graphene
import graphql
import logging
import os.path

from .index import Index
from .query import fetch, fetch_all
from .utils import pascal_case_str


class LocusInput(graphene.InputObjectType):
    """
    A generic locus input. Every field is optional, but based on the
    index might fail if not provided.
    """
    gene = graphene.String()
    chromosome = graphene.String()
    position = graphene.Int()
    start = graphene.Int()
    end = graphene.Int()

    def __str__(self):
        """
        Returns the region string for this input.
        """
        if self.gene:
            return self.gene

        # if the gene isn't specified, then chromosome is required
        if not self.chromosome:
            raise ValueError('Missing chromosome in locus input')

        # single base position
        if self.position:
            if self.start or self.end:
                raise ValueError('Cannot specify both position and start/end in locus')
            return f'{self.chromosome}:{self.position}'

        # range position
        if not self.start or not self.end:
            raise ValueError('Either position or start+end must be specified')

        return f'{self.chromosome}:{self.start}-{self.end}'


def load_schema(engine, bucket, schema_file):
    """
    Attempt to load a GraphQL schema file from disk.
    """
    if not os.path.isfile(schema_file):
        return None

    with open(schema_file) as fp:
        source = fp.read()
        schema = graphql.build_schema(source)

        # TODO: add resolvers for each index

        return schema


def build_schema(engine, bucket, subset=None):
    """
    Build the object types for all the indexes. Subset is a list of
    index names that the schema should include. If no subset is
    provided, then all indexes are built and added to the schema.
    """
    fields = {}

    for i in Index.list_indexes(engine):
        if not subset or i.name in subset:
            try:
                output_type, args = build_index_type(engine, bucket, i)
                resolver = ql_resolver(engine, bucket, i)

                # create the field for this table, with arguments and resolver
                fields[f'{i.table.name}'] = graphene.Field(
                    graphene.List(output_type),
                    args=args,
                    resolver=resolver,
                )
            except (ValueError, AssertionError) as ex:
                logging.error('%s; skipping %s...', str(ex), i.name)

    # build the root query object
    return graphene.Schema(query=type('Query', (graphene.ObjectType,), fields))


def build_index_type(engine, bucket, index, n=500):
    """
    Examine records from the index to build GraphQL objects and input
    types for querying the index.
    """
    logging.info('Building object type for %s...', index.name)

    # grab a single object from the index to build the schema object from
    reader = fetch_all(bucket, index.s3_prefix, key_limit=1)

    # read up to N records to get a good approx of all available fields
    records = [r for _, r in zip(range(n), reader.records)]

    # graphql object type (and subtypes) for the index
    obj_type = build_object_type(index.table.name, records)

    # add all the arguments used to query the index
    args = {}
    for col in index.schema.key_columns:
        for field in col.split('|'):
            args[field] = graphene.ID()

    # does this index's schema have a locus argument?
    if index.schema.has_locus:
        if index.schema.locus_is_template:
            name = index.schema.locus_columns[0]
            field_type = graphene.ID
        else:
            name = 'locus'
            field_type = LocusInput

        # add the locus field
        args[name] = field_type()

    return obj_type, args


def build_object_type(name, objs):
    """
    Create the object type of a given set of objects, which is the
    union of all fields across the objects.
    """
    all_columns = set([k for obj in objs for k in obj.keys()])

    # transpose the objects into columns of values
    columns = {}
    for k in all_columns:
        columns[k] = [obj[k] for obj in objs if k in obj]

    # determine the type of each field (ignore nulls)
    fields = {}
    for k, xs in columns.items():
        this_type = ql_type(name, k, xs)

        # create the field for this column
        fields[k] = graphene.Field(this_type, name=pascal_case_str(k))

    # build the final object
    return type(name, (graphene.ObjectType,), fields)


def ql_type(parent_name, field, xs):
    """
    Returns a Graphene field type for all values of xs. It assumes
    every item in xs is the same type or None.
    """
    first = next((x for x in xs if x is not None), None)
    this_type = type(first)

    # ensure the type of all values in the sample set are the same
    assert all(type(x) == this_type for x in xs if x is not None), 'Heterogenous field type'

    # dictionaries are an object type that needs defined
    if this_type == dict:
        return build_object_type(f'{parent_name}{field.capitalize()}', xs)

    # if the base type is a list, wrap the base type
    if this_type == list:
        return graphene.List(ql_type(parent_name, field, [y for ys in xs for y in ys]))

    # scalar type
    if this_type == bool:
        return graphene.Boolean
    elif this_type == int:
        return graphene.Int
    elif this_type == float:
        return graphene.Float
    elif this_type == str:
        return graphene.String

    # unknown, or can't be represented
    raise ValueError(f'Cannot define GraphQL type for {field}')


def ql_resolver(engine, bucket, index):
    """
    Returns a resolver function for a given index.
    """
    async def resolver(parent, info, **kwargs):
        q = []

        # add the key columns to the query
        for key_col in index.schema.key_columns:
            for col in key_col.split('|'):
                if col in kwargs:
                    q.append(kwargs[col])

        # add the locus if present
        if index.schema.has_locus:
            if index.schema.locus_is_template:
                q.append(kwargs[index.schema.locus_columns[0]])
            else:
                q.append(str(kwargs['locus']))

        # execute the query, get the resulting reader
        reader = fetch(engine, bucket, index, q)

        # materialize all the records into a single list
        return list(reader.records)

    return resolver
