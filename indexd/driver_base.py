from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()


class SQLAlchemyDriverBase(object):
    """
    SQLAlchemy implementation of index driver.
    """

    def __init__(self, conn, **config):
        """
        Initialize the SQLAlchemy database driver.
        """
        engine = create_engine(conn, **config)
        if not database_exists(engine.url):
            create_database(engine.url)
        self.engine = engine
