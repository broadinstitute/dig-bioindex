from sqlalchemy import *


class S3LocationMixin(object):
    id = Column('id', Integer, primary_key=True)
    path = Column('path', String(400))
    start_offset = Column('start_offset', Integer)
    end_offset = Column('end_offset', Integer)


class LocusMixin(object):
    chromosome = Column('chromosome', String(4))
    position = Column('position', Integer)


class StringIndexMixin(object):
    field = Column('value', String(200), index=True)


class IntegerIndexMixin(object):
    field = Column('value', Integer, index=True)


class FloatIndexMixin(object):
    field = Column('value', Float, index=True)
