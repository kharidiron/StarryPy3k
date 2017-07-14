"""
StarryPy Storage Managment Plugin

A plugin to handle storage for StarryPy. This was developed to replace the
very temperamental shelve system that was originally implemented.

Original authors: kharidiron
"""

from contextlib import contextmanager

import sqlalchemy as sqla
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from plugin_manager import BasePlugin


DeclarativeBase = declarative_base()
Session = sessionmaker()


@contextmanager
def db_session(_sessionmaker):
    session = _sessionmaker()

    try:
        yield session
    except:
        session.rollback()
        raise
    finally:
        session.close()


def cache_query(result, collection=False):
    record = result

    if not isinstance(result, DeclarativeBase):
        return record

    if collection:
        record = []
        for r in result:
            record.append(SessionCacher(r))
    else:
        record = SessionCacher(result)

    return record


def pack(obj, delim=","):
    """Turn a csv/tsv object into a string."""

    return delim.join(obj)


def unpack(obj, obj_type=set, delim=","):
    if not obj:
        return obj_type()
    else:
        return obj_type(obj.split(delim))


class SessionCacher(object):
    def __init__(self, record):
        self.__dict__["record"] = record
        self.__dict__["sessionmaker"] = Session

    def __getattr__(self, name):
        with db_session(self.sessionmaker) as session:
            if sessionmaker.object_session(self.record) != session:
                session.add(self.record)
            session.refresh(self.record)
            val = getattr(self.record, name)
        return val

    def __setattr__(self, name, value):
        with db_session(self.sessionmaker) as session:
            if sessionmaker.object_session(self.record) != session:
                session.add(self.record)
            session.refresh(self.record)
            setattr(self.record, name, value)
            session.merge(self.record)
            session.commit()

    def __str__(self):
        with db_session(self.sessionmaker) as session:
            if sessionmaker.object_session(self.record) != session:
                session.add(self.record)
            session.refresh(self.record)
            return str(self.record)


class SessionAccessMixin:
    def __init__(self):
        super().__init__()
        self.session = Session


class StorageManger(BasePlugin):
    name = "storage_manager"

    def __init__(self):
        self.default_config = {"storage_db": "config/storage.db"}
        super().__init__()

        self.engine = sqla.create_engine("sqlite:///{}".format(
            self.plugin_config.storage_db))

        DeclarativeBase.metadata.create_all(self.engine, checkfirst=True)
        Session.configure(bind=self.engine)
