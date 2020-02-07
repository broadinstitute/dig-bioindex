from sqlalchemy.ext.declarative import declarative_base
from lib.mixins import *


Base = declarative_base()


class Genes(Base, S3LocationMixin, LocusMixin):
    __tablename__ = 'genes'
