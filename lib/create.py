import logging
import sqlalchemy
import types

import lib.locus
import lib.s3
import lib.schema

from sqlalchemy import Boolean, Column, Integer, String, Table

from lib.utils import cap_case_str


def create_keys(engine):
    """
    Create the __Keys table, which has every S3 object key and MD5.
    """
    table_columns = [
        Column('id', Integer, primary_key=True),
        Column('key', String(1024), index=True, unique=True),
        Column('version', String(32)),
        Column('size', Integer),
        Column('indexed', Boolean, default=False)
    ]

    table = Table('__Keys', sqlalchemy.MetaData(), *table_columns)

    # create the index table (drop any existing table already there)
    logging.info('Creating __Keys table...')
    table.create(engine, checkfirst=True)


def create_index(engine, index, s3_prefix, schema):
    """
    Creates the __Indexes table if it doesn't exist and adds an entry
    for this new index (or overwrites the existing one).
    """
    assert s3_prefix.endswith('/'), "S3 prefix must be a common prefix ending with '/'"

    # definition of the __Indexes table
    table_columns = [
        Column('id', Integer, primary_key=True),
        Column('name', String(200), index=True),
        Column('table', String(200)),
        Column('prefix', String(1024)),
        Column('schema', String(200)),
        Column('built', Boolean, default=False)
    ]

    table = Table('__Indexes', sqlalchemy.MetaData(), *table_columns)

    # create the index table (drop any existing table already there)
    logging.info('Creating __Indexes table...')
    table.create(engine, checkfirst=True)

    # add the new index to the table
    sql = (
        'INSERT INTO `__Indexes` (`name`, `table`, `prefix`, `schema`) '
        'VALUES (%s, %s, %s, %s) '
        'ON DUPLICATE KEY UPDATE '
        '   `table` = VALUES(`table`), '
        '   `prefix` = VALUES(`prefix`), '
        '   `schema` = VALUES(`schema`), '
        '   `built` = 0 '
    )

    engine.execute(sql, index, cap_case_str(index), s3_prefix, str(schema))


def list_indexes(engine, filter_built=True):
    """
    Return an iterator of all the indexes.
    """
    sql = 'SELECT `name`, `table`, `prefix`, `schema`, `built` FROM `__Indexes`'

    # convert all rows to an index definition
    indexes = map(_index_of_row, engine.execute(sql))

    # remove indexes not built?
    if filter_built:
        indexes = filter(lambda i: i.built, indexes)

    return indexes


def lookup_index(engine, index, assert_if_not_built=True):
    """
    Lookup an index in the database, return its table name, s3 prefix,
    schema, etc.
    """
    sql = (
        'SELECT `name`, `table`, `prefix`, `schema`, `built` FROM `__Indexes` '
        'WHERE `name` = %s '
    )

    # lookup the index
    row = engine.execute(sql, index).fetchone()

    if row is None:
        raise KeyError(f'No such index: {index}')

    return _index_of_row(row)


def _index_of_row(row):
    """
    Convert a row to a simple namespace object for an index.
    """
    return types.SimpleNamespace(
        name=row[0],
        table=row[1],
        s3_prefix=row[2],
        schema=lib.schema.Schema(row[3]),
        built=row[4] != 0,
    )
