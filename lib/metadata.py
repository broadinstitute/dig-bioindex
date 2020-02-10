import logging

import lib.schema

from sqlalchemy.dialects.mysql import insert


def update(engine, table_name, schema):
    lib.schema.MetaData.__table__.create(engine, checkfirst=True)

    # insert/update this table's metadata scheme
    ins = insert(lib.schema.MetaData.__table__).values(table=table_name, schema=schema)
    upd = ins.on_duplicate_key_update(schema=schema)

    # perform metadata update
    logging.info('Inserting metadata schema for %s...', table_name)
    engine.execute(upd)


def load_all(engine):
    """
    Loads the current metadata state: tables and their indexing schemas.
    """
    metadata = {}

    for record in engine.execute(lib.schema.MetaData.__table__.select()):
        metadata[record.table] = record.schema

    return metadata


def load(engine, table_name):
    """
    Loads the schema for a particular table.
    """
    select = lib.schema.MetaData.__table__.select()

    return engine.execute(select.where(table=table_name)) \
        .fetchone() \
        .schema
