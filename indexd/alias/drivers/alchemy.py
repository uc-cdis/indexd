import json
import uuid
from contextlib import contextmanager

from cdislogging import get_logger
from sqlalchemy import BigInteger, Column, ForeignKey, Integer, String, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from indexd import alias
from indexd.alias.driver import AliasDriverABC
from indexd.alias.errors import (
    MultipleRecordsFound,
    NoRecordFound,
    RevisionMismatch,
)
from indexd.utils import (
    init_schema_version,
    is_empty_database,
    migrate_database,
)

Base = declarative_base()


class AliasSchemaVersion(Base):
    '''
    Table to track current database's schema version
    '''
    __tablename__ = 'alias_schema_version'
    version = Column(Integer, primary_key=True)

class AliasRecord(Base):
    '''
    Base alias record representation.
    '''
    __tablename__ = 'alias_record'

    name = Column(String, primary_key=True)
    rev = Column(String)
    size = Column(BigInteger)

    hashes = relationship('AliasRecordHash',
        backref='alias_record',
        cascade='all, delete-orphan',
    )

    release = Column(String)
    metastring = Column(String)

    host_authorities = relationship('AliasRecordHostAuthority',
        backref='alias_record',
        cascade='all, delete-orphan',
    )

    keeper_authority = Column(String)

class AliasRecordHash(Base):
    '''
    Base alias record hash representation.
    '''
    __tablename__ = 'alias_record_hash'

    name = Column(String, ForeignKey('alias_record.name'), primary_key=True)
    hash_type = Column(String, primary_key=True)
    hash_value = Column(String)

class AliasRecordHostAuthority(Base):
    '''
    Base alias record host authority representation.
    '''
    __tablename__ = 'alias_record_host_authority'

    name = Column(String, ForeignKey('alias_record.name'), primary_key=True)
    host = Column(String, primary_key=True)

class SQLAlchemyAliasDriver(AliasDriverABC):
    '''
    SQLAlchemy implementation of alias driver.
    '''

    def __init__(self, conn, logger=None, auto_migrate=True, **config):
        '''
        Initialize the SQLAlchemy database driver.
        '''
        super(SQLAlchemyAliasDriver, self).__init__(conn, **config)
        self.logger = logger or get_logger('SQLAlchemyAliasDriver')
        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)

        is_empty_db = is_empty_database(driver=self)
        Base.metadata.create_all()
        if is_empty_db:
            init_schema_version(driver=self, model=AliasSchemaVersion, version=CURRENT_SCHEMA_VERSION)

        if auto_migrate:
            self.migrate_alias_database()

    def migrate_alias_database(self):
        '''
        migrate alias database to match CURRENT_SCHEMA_VERSION
        '''
        migrate_database(
            driver=self, migrate_functions=SCHEMA_MIGRATION_FUNCTIONS,
            current_schema_version=CURRENT_SCHEMA_VERSION,
            model=AliasSchemaVersion)

    @property
    @contextmanager
    def session(self):
        '''
        Provide a transactional scope around a series of operations.
        '''
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
        '''
        Returns list of records stored by the backend.
        '''
        with self.session as session:
            query = session.query(AliasRecord)

            if start is not None:
                query = query.filter(AliasRecord.name > start)

            if size is not None:
                query = query.filter(AliasRecord.size == size)

            if hashes is not None:
                for h,v in hashes.items():
                    subq = (
                        session.query(AliasRecordHash.name)
                        .filter(and_(
                        AliasRecordHash.hash_type == h,
                        AliasRecordHash.hash_value == v))
                    )
                    query = query.filter(AliasRecord.name.in_(subq.subquery()))

            query = query.order_by(AliasRecord.name)
            query = query.limit(limit)

            return [i.name for i in query]

    def upsert(self, name, rev=None, size=None, hashes={}, release=None,
                metastring=None, host_authorities=[], keeper_authority=None):
        '''
        Updates or inserts a new record.
        '''

        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == name)

            try: record = query.one()
            except NoResultFound as err:
                record = AliasRecord()
            except MultipleResultsFound as err:
                raise MultipleRecordsFound('multiple records found')

            record.name = name

            if rev is not None and record.rev and rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            if size is not None:
                record.size = size

            if hashes is not None:
                record.hashes = [AliasRecordHash(
                    name=record,
                    hash_type=h,
                    hash_value=v,
                ) for h,v in hashes.items()]

            if release is not None:
                record.release = release

            if metastring is not None:
                record.metastring = metastring

            if host_authorities is not None:
                record.host_authorities = [AliasRecordHostAuthority(
                    name=name,
                    host=host,
                ) for host in host_authorities]

            if keeper_authority is not None:
                record.keeper_authority = keeper_authority

            record.rev = str(uuid.uuid4())[:8]

            session.add(record)

            return record.name, record.rev

    def get(self, name):
        '''
        Gets a record given the record name.
        '''
        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == name)

            try: record = query.one()
            except NoResultFound as err:
                raise NoRecordFound('no record found')
            except MultipleResultsFound as err:
                raise MultipleRecordsFound('multiple records found')

            rev = record.rev

            size = record.size
            hashes = {h.hash_type: h.hash_value for h in record.hashes}
            release = record.release
            metastring = record.metastring
            host_authorities = [h.host for h in record.host_authorities]
            keeper_authority = record.keeper_authority

        ret = {
            'name': name,
            'rev': rev,
            'size': size,
            'hashes': hashes,
            'release': release,
            'metadata': metastring,
            'host_authorities': host_authorities,
            'keeper_authority': keeper_authority,
        }

        return ret

    def delete(self, name, rev=None):
        '''
        Removes a record.
        '''
        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == name)

            try: record = query.one()
            except NoResultFound as err:
                raise NoRecordFound('no record found')
            except MultipleResultsFound as err:
                raise MultipleRecordsFound('multiple records found')

            if rev is not None and rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            session.delete(record)

    def __contains__(self, record):
        '''
        Returns True if record is stored by backend.
        Returns False otherwise.
        '''
        with self.session as session:
            query = session.query(AliasRecord)
            query = query.filter(AliasRecord.name == record)

            return query.exists()

    def __iter__(self):
        '''
        Iterator over unique records stored by backend.
        '''
        with self.session as session:
            for i in session.query(AliasRecord):
                yield i.name

    def __len__(self):
        '''
        Number of unique records stored by backend.
        '''
        with self.session as session:
            return session.query(AliasRecord).count()


def migrate_1(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ALTER COLUMN size TYPE bigint"
        .format(AliasRecord.__tablename__))


# ordered schema migration functions that the index should correspond to
# CURRENT_SCHEMA_VERSION - 1 when it's written
SCHEMA_MIGRATION_FUNCTIONS = [migrate_1]
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
