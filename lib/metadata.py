import logging

from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class MetaData(Base):
    """
    Each index table has a row in the metadata table, which tells the
    query system how it can be called. The metadata table should just
    be loaded and cached when the REST server starts.

    The schema determines how the files in S3 are sorted and indexed
    for the table. It is one of 3 schemas:

      * [field]
      * [chromosome field]:[position field]
      * [chromosome field]:[start position field]-[end position field]

    The schema is parsed and is used when determining what type of
    query to build for a given table and - if a locus query - filtering
    the records loaded from S3 to ensure they are within the given
    region before returning them. Example schemas:

      * varId
      * phenotype
      * chr:pos
      * chromosome:start-stop
    """
    __tablename__ = '_metadata'

    id = Column('id', Integer, primary_key=True)
    table = Column('table', String(200), index=True, unique=True)
    schema = Column('schema', String(200))


def update(engine, table_name, schema):
    MetaData.__table__.create(engine, checkfirst=True)

    # insert/update this table's metadata scheme
    ins = insert(MetaData.__table__).values(table=table_name, schema=schema)
    upd = ins.on_duplicate_key_update(schema=schema)

    # perform metadata update
    logging.info('Inserting metadata schema for %s...', table_name)
    engine.execute(upd)


def load_all(engine):
    """
    Loads the current metadata state: tables and their indexing schemas.
    """
    metadata = {}

    for record in engine.execute(MetaData.__table__.select()):
        metadata[record.table] = record.schema

    return metadata


def load(engine, table_name):
    """
    Loads the schema for a particular table.
    """
    select = MetaData.__table__.select()

    return engine.query(MetaData).filter(MetaData.table == table_name).fetchone() \
        .schema
