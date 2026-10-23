from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class SQLAlchemyDriverBase(object):
    """
    SQLAlchemy implementation of index driver.
    """

    def __init__(self, conn, **config):
        """
        Initialize the SQLAlchemy asynchronous database driver.
        """
        self.engine = create_async_engine(conn, **config)
