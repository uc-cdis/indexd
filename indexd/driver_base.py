from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base
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
        # 1. Create a synchronous connection string for sqlalchemy-utils
        # (e.g., changing "postgresql+asyncpg://" to "postgresql://")
        sync_conn = conn.replace("+asyncpg", "").replace("+aiosqlite", "")

        # 2. Check and create the database synchronously
        if not database_exists(sync_conn):
            create_database(sync_conn)

        # 3. Dynamically create the correct engine type based on the connection string
        # This allows you to migrate drivers to async one at a time.
        if "+asyncpg" in conn or "+aiosqlite" in conn:
            self.engine = create_async_engine(conn, **config)
        else:
            self.engine = create_engine(conn, **config)
