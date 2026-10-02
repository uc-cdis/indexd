import uuid

from cdislogging import get_logger
from contextlib import asynccontextmanager

from sqlalchemy import and_, func, text
from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm import declarative_base
from sqlalchemy import select, delete

from indexd.alias.driver import AliasDriverABC

from indexd.alias.errors import NoRecordFound, MultipleRecordsFound, RevisionMismatch
from indexd.utils import migrate_database

Base = declarative_base()


class AliasSchemaVersion(Base):
    """
    This migration logic is DEPRECATED. It is still supported for backwards compatibility,
    but any new migration should be added using Alembic.

    Table to track current database's schema version
    """

    __tablename__ = "alias_schema_version"
    version = Column(Integer, primary_key=True)


class AliasRecord(Base):
    """
    Base alias record representation.
    """

    __tablename__ = "alias_record"

    name = Column(String, primary_key=True)
    rev = Column(String)
    size = Column(BigInteger)

    hashes = relationship(
        "AliasRecordHash",
        backref="alias_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    release = Column(String)
    metastring = Column(String)

    host_authorities = relationship(
        "AliasRecordHostAuthority",
        backref="alias_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    keeper_authority = Column(String)


class AliasRecordHash(Base):
    """
    Base alias record hash representation.
    """

    __tablename__ = "alias_record_hash"

    name = Column(String, ForeignKey("alias_record.name"), primary_key=True)
    hash_type = Column(String, primary_key=True)
    hash_value = Column(String)


class AliasRecordHostAuthority(Base):
    """
    Base alias record host authority representation.
    """

    __tablename__ = "alias_record_host_authority"

    name = Column(String, ForeignKey("alias_record.name"), primary_key=True)
    host = Column(String, primary_key=True)


class SQLAlchemyAliasDriver(AliasDriverABC):
    """
    SQLAlchemy implementation of alias driver.
    """

    def __init__(self, conn, logger=None, **config):
        """
        Initialize the SQLAlchemy database driver.
        """
        super().__init__(conn, **config)
        self.logger = logger or get_logger("SQLAlchemyAliasDriver")
        Base.metadata.bind = self.engine

        self.Session = async_sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def migrate_alias_database(self):
        """
        This migration logic is DEPRECATED. It is still supported for backwards compatibility,
        but any new migration should be added using Alembic.

        migrate alias database to match CURRENT_SCHEMA_VERSION
        """
        # NOTE: If migrate_database performs sync operations, it needs async refactoring too.
        await migrate_database(
            driver=self,
            migrate_functions=SCHEMA_MIGRATION_FUNCTIONS,
            current_schema_version=CURRENT_SCHEMA_VERSION,
            model=AliasSchemaVersion,
        )

    @property
    @asynccontextmanager
    async def session(self):
        """
        Provide a transactional scope around a series of operations.
        """
        async with self.Session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def aliases(self, limit=100, start=None, size=None, hashes=None, page=None):
        """
        Returns list of records stored by the backend.
        """
        async with self.session as session:
            query = select(AliasRecord)

            if start is not None:
                query = query.filter(AliasRecord.name > start)

            if size is not None:
                query = query.filter(AliasRecord.size == size)

            if hashes is not None:
                for h, v in hashes.items():
                    subq = select(AliasRecordHash.name).filter(
                        and_(
                            AliasRecordHash.hash_type == h,
                            AliasRecordHash.hash_value == v,
                        )
                    )
                    query = query.filter(AliasRecord.name.in_(subq))

            query = query.order_by(AliasRecord.name)
            query = query.limit(limit)

            result = await session.execute(query)
            return [i.name for i in result.scalars().all()]

    async def upsert(
        self,
        name,
        rev=None,
        size=None,
        hashes=None,
        release=None,
        metastring=None,
        host_authorities=None,
        keeper_authority=None,
    ):
        """
        Updates or inserts a new record.
        """

        hashes = hashes or {}
        host_authorities = host_authorities or []

        async with self.session as session:
            query = select(AliasRecord).filter(AliasRecord.name == name)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound as err:
                record = AliasRecord()
            except MultipleResultsFound as err:
                raise MultipleRecordsFound("multiple records found")

            record.name = name

            if rev is not None and record.rev and rev != record.rev:
                raise RevisionMismatch("revision mismatch")

            if size is not None:
                record.size = size

            if hashes is not None:
                record.hashes = [
                    AliasRecordHash(name=record.name, hash_type=h, hash_value=v)
                    for h, v in hashes.items()
                ]

            if release is not None:
                record.release = release

            if metastring is not None:
                record.metastring = metastring

            if host_authorities is not None:
                record.host_authorities = [
                    AliasRecordHostAuthority(name=name, host=host)
                    for host in host_authorities
                ]

            if keeper_authority is not None:
                record.keeper_authority = keeper_authority

            record.rev = str(uuid.uuid4())[:8]

            session.add(record)
            await session.commit()

            return record.name, record.rev

    async def get(self, name):
        """
        Gets a record given the record name.
        """
        async with self.session as session:
            query = select(AliasRecord).filter(AliasRecord.name == name)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound as err:
                raise NoRecordFound("no record found")
            except MultipleResultsFound as err:
                raise MultipleRecordsFound("multiple records found")

            rev = record.rev
            size = record.size
            hashes = {h.hash_type: h.hash_value for h in record.hashes}
            release = record.release
            metastring = record.metastring
            host_authorities = [h.host for h in record.host_authorities]
            keeper_authority = record.keeper_authority

        ret = {
            "name": name,
            "rev": rev,
            "size": size,
            "hashes": hashes,
            "release": release,
            "metadata": metastring,
            "host_authorities": host_authorities,
            "keeper_authority": keeper_authority,
        }

        return ret

    async def delete(self, name, rev=None):
        """
        Removes a record.
        """
        async with self.session as session:
            query = select(AliasRecord).filter(AliasRecord.name == name)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound as err:
                raise NoRecordFound("no record found")
            except MultipleResultsFound as err:
                raise MultipleRecordsFound("multiple records found")

            if rev is not None and rev != record.rev:
                raise RevisionMismatch("revision mismatch")

            await session.delete(record)

    async def has_record(self, record):
        """
        Async replacement for the synchronous __contains__ magic method.
        """
        async with self.session as session:
            query = select(AliasRecord).filter(AliasRecord.name == record)
            result = await session.execute(select(query.exists()))
            return result.scalar()

    async def __aiter__(self):
        """
        Async replacement for the synchronous __iter__ magic method.
        """
        async with self.session as session:
            result = await session.stream_scalars(select(AliasRecord))
            async for i in result:
                yield i.name

    async def len(self):
        """
        Async replacement for the synchronous __len__ magic method.
        """
        async with self.session as session:
            result = await session.execute(
                select(func.count()).select_from(AliasRecord)
            )
            return result.scalar()


async def migrate_1(session, **kwargs):
    await session.execute(
        text(
            "ALTER TABLE {} ALTER COLUMN size TYPE bigint;".format(
                AliasRecord.__tablename__
            )
        )
    )


# ordered schema migration functions that the index should correspond to
# CURRENT_SCHEMA_VERSION - 1 when it's written
SCHEMA_MIGRATION_FUNCTIONS = [migrate_1]
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
