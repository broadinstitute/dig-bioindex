import logging
import sqlalchemy
import sys

from sqlalchemy import Column, DateTime, Index, Integer, String, Table

from .aws import connect_to_db


# tables
__Indexes = None
__Keys = None


def migrate(config):
    """
    Ensure all tables exist for the BioIndex to function. Returns
    the SQLAlchemy Engine object connected to the database.
    """
    rds_config = config.rds_config
    name = rds_config['name']

    # indicate what's being connected to and try
    logging.info('Connecting to %s/%s...', name, config.bio_schema)

    try:
        engine = connect_to_db(**rds_config, schema=config.bio_schema)

        # create all tables
        create_indexes_table(engine)
        create_keys_table(engine)

        return engine

    except sqlalchemy.exc.OperationalError as ex:
        logging.error(ex.orig)
        sys.exit(-1)


def create_indexes_table(engine):
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

    # define the __Indexes table
    __Indexes = Table('__Indexes', sqlalchemy.MetaData(), *table_columns)

    # create the index table (drop any existing table already there)
    logging.info('Migrating __Indexes...')
    __Indexes.create(engine, checkfirst=True)


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

    # define the __Keys table
    __Keys = Table('__Keys', sqlalchemy.MetaData(), *table_columns)

    # create the keys table (drop any existing table already there)
    logging.info('Migrating __Keys...')
    __Keys.create(engine, checkfirst=True)

    # create the compound index for the table
    rows = engine.execute('SHOW INDEXES FROM `__Keys`').fetchall()

    # build the index if not present
    if not any(map(lambda r: r[2] == 'key_idx', rows)):
        Index('key_idx', table_columns[1:3], unique=True).create(engine)
