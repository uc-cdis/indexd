from cdislogging import get_logger
from indexd.driver_base import SQLAlchemyDriverBase
from sqlalchemy import String, Column, BigInteger, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker


Base = declarative_base()
CURRENT_SCHEMA_VERSION = 2


class IndexRecord(Base):
    """
    Base index record representation.
    """

    __tablename__ = "index_record"

    did = Column(String, primary_key=True)

    rev = Column(String)
    form = Column(String)
    size = Column(BigInteger)

    urls = relationship(
        "IndexRecordUrl", backref="index_record", cascade="all, delete-orphan"
    )

    hashes = relationship(
        "IndexRecordHash", backref="index_record", cascade="all, delete-orphan"
    )


class IndexRecordUrl(Base):
    """
    Base index record url representation.
    """

    __tablename__ = "index_record_url"

    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    url = Column(String, primary_key=True)


class IndexRecordHash(Base):
    """
    Base index record hash representation.
    """

    __tablename__ = "index_record_hash"

    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    hash_type = Column(String, primary_key=True)
    hash_value = Column(String)


class SQLAlchemyIndexTestDriver(SQLAlchemyDriverBase):
    """
    SQLAlchemy implementation of index driver.
    """

    def __init__(self, conn, logger=None, **config):
        super().__init__(conn, **config)
        self.logger = logger or get_logger("SQLAlchemyIndexTestDriver")

        Base.metadata.bind = self.engine

        self.Session = sessionmaker(bind=self.engine)
