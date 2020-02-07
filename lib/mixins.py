from sqlalchemy import *
from sqlalchemy.ext.declarative import declared_attr


class S3LocationMixin(object):
    id = Column('id', Integer, primary_key=True)
    path = Column('path', String(400))
    offset = Column('offset', Integer)
    length = Column('length', Integer)


class LocusMixin(object):
    chromosome = Column('chromosome', String(4))
    position = Column('position', Integer)


class StringIndexMixin(object):
    field = Column('field', String(200), index=True)


class IntegerIndexMixin(object):
    field = Column('field', Integer, index=True)


class FloatIndexMixin(object):
    field = Column('field', Float, index=True)
