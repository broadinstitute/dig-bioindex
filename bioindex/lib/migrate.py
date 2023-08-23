import logging
import sqlalchemy
import sys

from .aws import connect_to_sqlite


# tables
__Indexes = None
__Keys = None


def migrate(config):
    """
    Ensure all tables exist for the BioIndex to function. Returns
    the SQLAlchemy Engine object connected to the database.
    """

    try:
        engine = connect_to_sqlite()

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
    # create the index table (drop any existing table already there)
    logging.info('Migrating __Indexes...')
    sql = sqlalchemy.text('CREATE TABLE IF NOT EXISTS __Indexes ('
                          '`id` INTEGER PRIMARY KEY, '
                          '`name` VARCHAR(200) NOT NULL, '
                          '`table` VARCHAR(200) NOT NULL, '
                          '`prefix` VARCHAR(1024) NOT NULL, '
                          '`schema` VARCHAR(200) NOT NULL,'
                          '`built` TIMESTAMP, '
                          'compressed TINYINT(1) NOT NULL DEFAULT 0'
                          ')')
    with engine.connect() as conn:
        conn.execute(sql)


def index_migration_1(engine):
    with engine.connect() as conn:
        # drop name_UNIQUE index if present
        conn.execute(sqlalchemy.text('DROP INDEX IF EXISTS `name_UNIQUE`'))

        # create name_arity index if not present
        conn.execute(sqlalchemy.text(
            'CREATE UNIQUE INDEX IF NOT EXISTS `name_arity_idx` ON __Indexes '
            '(name, (LENGTH(`schema`) - LENGTH(REPLACE(`schema`, ",", "")) + 1))'
        ))
        conn.commit()


def create_keys_table(engine):
    """
    Create the __Keys table if it doesn't already exist.
    """
    # create the keys table (drop any existing table already there)
    logging.info('Migrating __Keys...')
    sql = sqlalchemy.text('CREATE TABLE IF NOT EXISTS __Keys ('
                          '`id` INTEGER PRIMARY KEY, '
                          '`index` VARCHAR(200) NOT NULL, '
                          '`key` VARCHAR(1024) NOT NULL, '
                          '`version` VARCHAR(32) NOT NULL,'
                          '`built` TIMESTAMP '
                          ')')
    with engine.connect() as conn:
        conn.execute(sql)

    # build the index if not present
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(
            "CREATE UNIQUE INDEX IF NOT EXISTS `key_idx` ON __Keys (`index`, `key`)"
        ))
