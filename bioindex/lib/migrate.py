import logging
import sqlalchemy
import sys

from sqlalchemy import Column, DateTime, Index, Integer, String, Table, FetchedValue, Boolean, text
from sqlalchemy.orm import Session

from .gcp import connect_to_db

# tables
__Indexes = None
__Keys = None


def migrate(config):
    """
    Ensure all tables exist for the BioIndex to function. Returns
    the SQLAlchemy Engine object connected to the database.
    """
    cloudsql_config = config.cloudsql_config
    name = cloudsql_config['name']

    # indicate what's being connected to and try
    logging.info('Connecting to %s/%s...', name, config.bio_schema)

    try:
        engine = connect_to_db(**cloudsql_config, schema=config.bio_schema)

        # create all tables
        create_indexes_table(engine)
        index_migration_1(engine)
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
        Column('compressed', Boolean, nullable=False, server_default=text('0'), default=False),
    ]

    # define the __Indexes table
    __Indexes = Table('__Indexes', sqlalchemy.MetaData(), *table_columns)

    # create the index table (drop any existing table already there)
    logging.info('Migrating __Indexes...')
    __Indexes.create(engine, checkfirst=True)


def index_migration_1(engine):
    with Session(engine) as session:  # Using Session with engine
        with session.begin():  # Beginning the transaction explicitly
            result = session.execute(text('SHOW INDEXES FROM `__Indexes`'))  # Using session for executing
            indexes = result.fetchall()

            # drop name_UNIQUE index if present
            maybe_name_idx = [r[2] == 'name_UNIQUE' for r in indexes]
            if any(maybe_name_idx):
                session.execute(text('ALTER TABLE __Indexes DROP INDEX `name_UNIQUE`'))

            # create name_arity index if not present
            maybe_name_arity_idx = [r[2] != 'name_arity_idx' for r in indexes]
            if all(maybe_name_arity_idx):
                session.execute(text(
                    'CREATE UNIQUE INDEX `name_arity_idx` ON __Indexes '
                    '(name, (LENGTH(`schema`) - LENGTH(REPLACE(`schema`, ",", "")) + 1))'
                ))


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
    with engine.connect() as conn:
        rows = conn.execute(text('SHOW INDEXES FROM `__Keys`')).fetchall()

    # build the index if not present
    if not any(map(lambda r: r[2] == 'key_idx', rows)):
        Index('key_idx', __Keys.c.index, __Keys.c.key, unique=True).create(engine)
