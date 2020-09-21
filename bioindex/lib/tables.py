import logging
import sqlalchemy
import types

from sqlalchemy import Column, DateTime, Index, Integer, String, Table

from .schema import Schema
from .utils import cap_case_str


def create_index(engine, index, s3_prefix, schema):
    """
    Creates the __Indexes table if it doesn't exist and adds an entry
    for this new index (or overwrites the existing one).
    """
    assert s3_prefix.endswith('/'), "S3 prefix must be a common prefix ending with '/'"

    # create the __Indexes and __Keys tables
    create_index_table(engine)

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


def create_index_table(engine):
    """
    Create the __Indexes table if it doesn't already exist.
    """
    table_columns = [
        Column('id', Integer, primary_key=True),
        Column('name', String(200, collation='ascii_bin'), index=True),
        Column('table', String(200, collation='ascii_bin')),
        Column('prefix', String(1024, collation='ascii_bin')),
        Column('schema', String(200, collation='utf8_bin')),
        Column('built', DateTime, nullable=True),
    ]

    table = Table('__Indexes', sqlalchemy.MetaData(), *table_columns)

    # create the index table (drop any existing table already there)
    logging.info('Creating __Indexes table...')
    table.create(engine, checkfirst=True)


def create_keys_table(engine):
    """
    Create the __Keys table if it doesn't already exist.
    """
    table_columns = [
        Column('id', Integer, primary_key=True),
        Column('index', String(200, collation='ascii_bin'), nullable=False),
        Column('key', String(1024, collation='ascii_bin'), nullable=False),
        Column('version', String(32, collation='ascii_bin'), nullable=False),
        Column('built', DateTime, nullable=True),
    ]

    table = Table('__Keys', sqlalchemy.MetaData(), *table_columns)

    # create the keys table (drop any existing table already there)
    logging.info('Creating __Keys table...')
    table.create(engine, checkfirst=True)

    # create the compound index for the table
    rows = engine.execute('SHOW INDEXES FROM `__Keys`').fetchall()

    # build the index if not present
    if not any(map(lambda r: r[2] == 'key_idx', rows)):
        Index('key_idx', *table_columns[1:3], unique=True).create(engine)


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
        'SELECT `name`, `table`, `prefix`, `schema`, `built` '
        'FROM `__Indexes` '
        'WHERE `name` = %s '
    )

    # lookup the index
    row = engine.execute(sql, index).fetchone()

    if row is None:
        raise KeyError(f'No such index: {index}')

    return _index_of_row(row)


def lookup_keys(engine, index):
    """
    Look up all the keys and versions for a given index. Returns
    a dictionary of key -> {id, version}. The version will be None
    if the key hasn't been completely indexed.
    """
    sql = 'SELECT `id`, `key`, `version`, `built` FROM `__Keys` WHERE `index` = %s '
    rows = engine.execute(sql, index).fetchall()

    return {key: {'id': id, 'version': built and ver} for id, key, ver, built in rows}


def insert_key(engine, index, key, version):
    """
    Adds a key to the __Keys table for an index. If the key already
    exists and the versions match, just return the ID for it. If the
    versions don't match, delete the existing record and create a
    new one with a new ID.
    """
    sql = 'SELECT `id`, `version` FROM `__Keys` WHERE `index` = %s and `key` = %s'
    row = engine.execute(sql, index, key).fetchone()

    if row is not None:
        if row[1] == version:
            return row[0]

        # delete the existing key entry
        engine.execute('DELETE FROM `__Keys` WHERE `id` = %s', row[0])

    # add a new entry
    sql = 'INSERT INTO `__Keys` (`index`, `key`, `version`) VALUES (%s, %s, %s)'
    row = engine.execute(sql, index, key, version)

    return row.lastrowid


def delete_key(engine, index, key):
    """
    Removes all records from the index and the key from the __Keys
    table for a paritcular index/key pair.
    """
    sql = 'DELETE FROM `__Keys` WHERE `index` = %s and `key` = %s'
    engine.execute(sql, index, key)


def delete_keys(engine, index):
    """
    Removes all records from the __Keys table for a paritcular index.
    """
    engine.execute('DELETE FROM `__Keys` WHERE `index` = %s', index)


def _index_of_row(row):
    """
    Convert a row to a simple namespace object for an index.
    """
    return types.SimpleNamespace(
        name=row[0],
        table=row[1],
        s3_prefix=row[2],
        schema=Schema(row[3]),
        built=row[4],
    )
