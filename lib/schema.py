from sqlalchemy.ext.declarative import declarative_base
from lib.mixins import *


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
      * chromosome:position
      * chromosome:start-stop
    """
    __tablename__ = 'metadata'

    id = Column('id', Integer, primary_key=True)
    table = Column('table', String(200), index=True, unique=True)
    schema = Column('schema', String(200))


class Genes(Base, S3LocationMixin, LocusMixin):
    __tablename__ = 'genes'


class GlobalEnrichment(Base, S3LocationMixin, StringIndexMixin):
    __tablename__= 'global_enrichment'


class PhenotypeAssociations(Base, S3LocationMixin, StringIndexMixin):
    __tablename__ = 'phenotype_associations'


class AnnotatedRegions(Base, S3LocationMixin, LocusMixin):
    __tablename__ = 'annotated_regions'


class Variants(Base, S3LocationMixin, StringIndexMixin):
    __tablename__ = 'variants'
