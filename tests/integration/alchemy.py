import sqlalchemy as sa
from sqlalchemy import orm

from indexd.driver_base import SQLAlchemyDriverBase

Base = orm.declarative_base()
CURRENT_SCHEMA_VERSION = 2


class IndexRecord(Base):
    """
    Base index record representation.
    """

    __tablename__ = "index_record"

    did = sa.Column(sa.String, primary_key=True)

    rev = sa.Column(sa.String)
    form = sa.Column(sa.String)
    size = sa.Column(sa.BigInteger)

    urls = orm.relationship(
        "IndexRecordUrl",
        back_populates="index_record",
        cascade="all, delete-orphan",
    )

    hashes = orm.relationship(
        "IndexRecordHash",
        back_populates="index_record",
        cascade="all, delete-orphan",
    )


class IndexRecordUrl(Base):
    """
    Base index record url representation.
    """

    __tablename__ = "index_record_url"

    did = sa.Column(sa.String, sa.ForeignKey("index_record.did"), primary_key=True)
    url = sa.Column(sa.String, primary_key=True)


class IndexRecordHash(Base):
    """
    Base index record hash representation.
    """

    __tablename__ = "index_record_hash"

    did = sa.Column(sa.String, sa.ForeignKey("index_record.did"), primary_key=True)
    hash_type = sa.Column(sa.String, primary_key=True)
    hash_value = sa.Column(sa.String)


class SQLAlchemyIndexTestDriver(SQLAlchemyDriverBase):
    """
    SQLAlchemy implementation of index driver.
    """

    def __init__(self, conn, logger=None, **config):
        super().__init__(conn, **config)

        Base.metadata.bind = self.engine
        Base.metadata.create_all(bind=self.engine)

        self.Session = orm.sessionmaker(bind=self.engine)
