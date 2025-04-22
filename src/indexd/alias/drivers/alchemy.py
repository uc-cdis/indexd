import logging
import uuid
from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy import exc, orm

from indexd.alias.driver import AliasDriverABC
from indexd.alias.errors import (
    MultipleRecordsFoundError,
    NoRecordFoundError,
    RevisionMismatchError,
)
from indexd.index.errors import UnhealthyCheckError
from indexd.utils import init_schema_version, is_empty_database, migrate_database

logger = logging.getLogger(__name__)
Base = orm.declarative_base()


class AliasSchemaVersion(Base):
    """
    Table to track current database's schema version
    """

    __tablename__ = "alias_schema_version"
    version: orm.Mapped[int] = sa.Column(sa.Integer, primary_key=True)


class AliasRecord(Base):
    """
    Base alias record representation.
    """

    __tablename__ = "alias_record"

    name: orm.Mapped[str] = sa.Column(sa.String, primary_key=True)
    rev: orm.Mapped[str] = sa.Column(sa.String)
    size: orm.Mapped[int] = sa.Column(sa.BigInteger)

    hashes: orm.Mapped[list["AliasRecordHash"]] = orm.relationship(
        "AliasRecordHash",
        back_populates="alias_record",
        cascade="all, delete-orphan",
    )

    release: orm.Mapped[str] = sa.Column(sa.String)
    metastring: orm.Mapped[str] = sa.Column(sa.String)

    host_authorities: orm.Mapped[list["AliasRecordHostAuthority"]] = orm.relationship(
        "AliasRecordHostAuthority",
        back_populates="alias_record",
        cascade="all, delete-orphan",
    )

    keeper_authority: orm.Mapped[str] = sa.Column(sa.String)


class AliasRecordHash(Base):
    """
    Base alias record hash representation.
    """

    __tablename__ = "alias_record_hash"

    name: orm.Mapped[str] = sa.Column(
        sa.String, sa.ForeignKey("alias_record.name"), primary_key=True
    )
    hash_type: orm.Mapped[str] = sa.Column(sa.String, primary_key=True)
    hash_value: orm.Mapped[str] = sa.Column(sa.String)

    alias_record: orm.Mapped[AliasRecord] = orm.relationship(back_populates="hashes")


class AliasRecordHostAuthority(Base):
    """
    Base alias record host authority representation.
    """

    __tablename__ = "alias_record_host_authority"

    name: orm.Mapped[str] = sa.Column(
        sa.String, sa.ForeignKey("alias_record.name"), primary_key=True
    )
    host: orm.Mapped[str] = sa.Column(sa.String, primary_key=True)

    alias_record: orm.Mapped[AliasRecord] = orm.relationship(
        back_populates="host_authorities"
    )


class SQLAlchemyAliasDriver(AliasDriverABC):
    """
    SQLAlchemy implementation of alias driver.
    """

    def __init__(self, conn, auto_migrate=True, **config):
        """
        Initialize the SQLAlchemy database driver.
        """
        super().__init__(conn, **config)
        Base.metadata.bind = self.engine
        self.Session = orm.sessionmaker(bind=self.engine)

        is_empty_db = is_empty_database(driver=self)
        Base.metadata.create_all(bind=self.engine)
        if is_empty_db:
            init_schema_version(
                driver=self,
                model=AliasSchemaVersion,
                current_version=CURRENT_SCHEMA_VERSION,
            )

        if auto_migrate:
            self.migrate_alias_database()

    def migrate_alias_database(self):
        """
        migrate alias database to match CURRENT_SCHEMA_VERSION
        """
        migrate_database(
            driver=self,
            migrate_functions=SCHEMA_MIGRATION_FUNCTIONS,
            current_schema_version=CURRENT_SCHEMA_VERSION,
            model=AliasSchemaVersion,
        )

    def health_check(self):
        """
        Does a health check of the backend.
        """
        with self.session as session:
            try:
                session.execute(sa.text("SELECT 1"))
            except Exception:
                raise UnhealthyCheckError()

            return True

    @property
    @contextmanager
    def session(self):
        """
        Provide a transactional scope around a series of operations.
        """
        session = self.Session()

        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def aliases(self, limit=100, start=None, size=None, hashes=None):
        """
        Returns list of records stored by the backend.
        """
        with self.session as session:
            query = session.query(AliasRecord)

            if start is not None:
                query = query.filter(AliasRecord.name > start)

            if size is not None:
                query = query.filter(AliasRecord.size == size)

            if hashes is not None:
                for h, v in hashes.items():
                    subq = session.query(AliasRecordHash.name).filter(
                        sa.and_(
                            AliasRecordHash.hash_type == h,
                            AliasRecordHash.hash_value == v,
                        )
                    )
                    query = query.filter(AliasRecord.name.in_(subq.subquery()))

            query = query.order_by(AliasRecord.name)
            query = query.limit(limit)

            return [i.name for i in query]

    def upsert(
        self,
        name,
        rev=None,
        size=None,
        hashes={},
        release=None,
        metastring=None,
        host_authorities=[],
        keeper_authority=None,
    ):
        """
        Updates or inserts a new record.
        """

        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == name)

            try:
                record = query.one()
            except exc.NoResultFound:
                record = AliasRecord()
            except exc.MultipleResultsFound:
                raise MultipleRecordsFoundError("multiple records found")

            record.name = name

            if rev is not None and record.rev and rev != record.rev:
                raise RevisionMismatchError("revision mismatch")

            if size is not None:
                record.size = size

            if hashes is not None:
                record.hashes = [
                    AliasRecordHash(
                        name=record,
                        hash_type=h,
                        hash_value=v,
                    )
                    for h, v in hashes.items()
                ]

            if release is not None:
                record.release = release

            if metastring is not None:
                record.metastring = metastring

            if host_authorities is not None:
                record.host_authorities = [
                    AliasRecordHostAuthority(
                        name=name,
                        host=host,
                    )
                    for host in host_authorities
                ]

            if keeper_authority is not None:
                record.keeper_authority = keeper_authority

            record.rev = str(uuid.uuid4())[:8]

            session.add(record)

            return record.name, record.rev

    def get(self, name):
        """
        Gets a record given the record name.
        """
        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == name)

            try:
                record = query.one()
            except exc.NoResultFound:
                raise NoRecordFoundError("no record found")
            except exc.MultipleResultsFound:
                raise MultipleRecordsFoundError("multiple records found")

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

    def delete(self, name, rev=None):
        """
        Removes a record.
        """
        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == name)

            try:
                record = query.one()
            except exc.NoResultFound:
                raise NoRecordFoundError("no record found")
            except exc.MultipleResultsFound:
                raise MultipleRecordsFoundError("multiple records found")

            if rev is not None and rev != record.rev:
                raise RevisionMismatchError("revision mismatch")

            session.delete(record)

    def __contains__(self, record):
        """
        Returns True if record is stored by backend.
        Returns False otherwise.
        """
        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == record)

            return query.exists()

    def __iter__(self):
        """
        Iterator over unique records stored by backend.
        """
        with self.session as session:
            for i in session.query(AliasRecord):
                yield i.name

    def __len__(self):
        """
        Number of unique records stored by backend.
        """
        with self.session as session:
            return session.query(AliasRecord).count()


def migrate_1(session, **kwargs):
    session.execute(
        sa.text(
            f"ALTER TABLE {AliasRecord.__tablename__} ALTER COLUMN size TYPE bigint"
        )
    )


# ordered schema migration functions that the index should correspond to
# CURRENT_SCHEMA_VERSION - 1 when it's written
SCHEMA_MIGRATION_FUNCTIONS = [migrate_1]
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
