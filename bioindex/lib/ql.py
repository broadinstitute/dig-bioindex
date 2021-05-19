import graphql
import graphql.utilities
import logging
import os.path
import re

from .index import Index
from .query import fetch, fetch_all


LocusInput = graphql.GraphQLInputObjectType('Locus', {
    'gene': graphql.GraphQLInputField(graphql.GraphQLString),
    'chromosome': graphql.GraphQLInputField(graphql.GraphQLString),
    'position': graphql.GraphQLInputField(graphql.GraphQLInt),
    'start': graphql.GraphQLInputField(graphql.GraphQLInt),
    'end': graphql.GraphQLInputField(graphql.GraphQLInt),
})


def load_schema(engine, bucket, schema_file):
    """
    Attempt to load a GraphQL schema file from disk.
    """
    if not os.path.isfile(schema_file):
        return None

    # parse the schema
    with open(schema_file) as fp:
        schema = graphql.utilities.build_schema(fp.read())

    # add resolvers for each index
    for i in Index.list_indexes(engine):
        schema.query_type.fields[i.table.name].resolve = ql_resolver(engine, bucket, i)

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
                fields[f'{i.table.name}'] = graphql.GraphQLField(
                    graphql.GraphQLList(output_type),
                    args=args,
                    resolve=resolver,
                )
            except (ValueError, AssertionError) as ex:
                logging.error('%s; skipping %s...', str(ex), i.name)

    # construct the query object for the root object type
    root = graphql.GraphQLObjectType('Query', fields)

    # build the schema
    return graphql.GraphQLSchema(query=root)


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
        optional = '|' in col

        for field in col.split('|'):
            args[field] = graphql.GraphQLID

            if not optional:
                args[field] = graphql.GraphQLNonNull(args[field])

    # does this index's schema have a locus argument?
    if index.schema.has_locus:
        if index.schema.locus_is_template:
            name = index.schema.locus_columns[0]
            field_type = graphql.GraphQLID
        else:
            name = 'locus'
            field_type = graphql.GraphQLString
            #field_type = LocusInput

        # add the locus field; it's required
        args[name] = graphql.GraphQLNonNull(field_type)

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
        if not ql_is_valid_field(k):
            logging.warning('%s is not a valid GraphQL field name; skipping...', k)
        else:
            columns[k] = [obj[k] for obj in objs if k in obj]

    # determine the type of each field (ignore nulls)
    fields = {}
    for field_name, xs in columns.items():
        this_type = ql_type(name, field_name, xs)
        fields[field_name] = graphql.GraphQLField(this_type)

    # build the final object
    return graphql.GraphQLObjectType(name, fields)


def ql_is_valid_field(s):
    """
    Returns False if s is not a valid identifier for a GraphQL field.
    """
    return re.fullmatch(r'[_a-z][_a-z0-9]*', s, re.IGNORECASE) is not None


def ql_type(parent_name, field, xs):
    """
    Returns a Graphene field type for all values of xs. It assumes
    every item in xs is the same type or None.
    """
    all_types = set([type(x) for x in xs if x is not None and x is not float('nan')])

    # handle a special case of floats being parsed as integers
    if all_types == set([int, float]):
        all_types = set([float])

    # there should only be one type per field
    if len(all_types) > 1:
        assert False, f'Heterogenous field type: {parent_name}/{field} ({all_types})'

    # get the only type
    this_type = all_types.pop()

    # dictionaries are an object type that needs defined
    if this_type == dict:
        return build_object_type(f'{parent_name}{field.capitalize()}', xs)

    # if the base type is a list, wrap the base type
    if this_type == list:
        return graphql.GraphQLList(ql_type(parent_name, field, [y for ys in xs for y in ys]))

    # scalar type
    if this_type == bool:
        return graphql.GraphQLBoolean
    elif this_type == int:
        return graphql.GraphQLInt
    elif this_type == float:
        return graphql.GraphQLFloat
    elif this_type == str:
        return graphql.GraphQLString

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
                q.append(kwargs['locus'])
                #q.append(build_region_str(**kwargs['locus']))

        # execute the query, get the resulting reader
        reader = fetch(engine, bucket, index, q)

        # materialize all the records
        return list(reader.records)

    return resolver
