import uuid
import datetime

from cdispyutils.log import get_logger
from contextlib import contextmanager
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import and_
from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import BigInteger
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from indexd.index.driver import IndexDriverABC
from indexd.index.errors import NoRecordFound
from indexd.index.errors import MultipleRecordsFound
from indexd.index.errors import RevisionMismatch
from indexd.index.errors import UnhealthyCheck
from indexd.index.errors import AddExistedColumn
from indexd.errors import UserError
from indexd.utils import migrate_database

from sqlalchemy.exc import ProgrammingError

CURRENT_SCHEMA_VERSION = 3
Base = declarative_base()


class BaseVersion(Base):
    '''
    Base index record version representation.
    '''
    __tablename__ = 'base_version'

    baseid = Column(String, primary_key=True)
    dids = relationship(
        'IndexRecord',
        backref='base_version',
        cascade='all, delete-orphan')


class IndexSchemaVersion(Base):
    '''
    Table to track current database's schema version
    '''
    __tablename__ = 'index_schema_version'
    version = Column(Integer, primary_key=True)


class IndexRecord(Base):
    '''
    Base index record representation.
    '''
    __tablename__ = 'index_record'

    did = Column(String, primary_key=True)

    baseid = Column(String, ForeignKey('base_version.baseid'))
    rev = Column(String)
    form = Column(String)
    size = Column(BigInteger)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow)
    file_name = Column(String, index=True)

    urls = relationship(
        'IndexRecordUrl',
        backref='index_record',
        cascade='all, delete-orphan',
    )

    hashes = relationship(
        'IndexRecordHash',
        backref='index_record',
        cascade='all, delete-orphan',
    )


class IndexRecordUrl(Base):
    '''
    Base index record url representation.
    '''
    __tablename__ = 'index_record_url'

    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    url = Column(String, primary_key=True)


class IndexRecordHash(Base):
    '''
    Base index record hash representation.
    '''
    __tablename__ = 'index_record_hash'

    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    hash_type = Column(String, primary_key=True)
    hash_value = Column(String)


class SQLAlchemyIndexDriver(IndexDriverABC):
    '''
    SQLAlchemy implementation of index driver.
    '''

    def __init__(self, conn, logger=None, auto_migrate=True, **config):
        '''
        Initialize the SQLAlchemy database driver.
        '''
        self.engine = create_engine(conn, **config)
        self.logger = logger or get_logger('SQLAlchemyIndexDriver')

        Base.metadata.bind = self.engine
        Base.metadata.create_all()

        self.Session = sessionmaker(bind=self.engine)

        if auto_migrate:
            self.migrate_index_database()

    def migrate_index_database(self):
        '''
        migrate alias database to match CURRENT_SCHEMA_VERSION
        '''
        migrate_database(
            driver=self, migrate_functions=SCHEMA_MIGRATION_FUNCTIONS,
            current_schema_version=CURRENT_SCHEMA_VERSION,
            model=IndexSchemaVersion)

    @property
    @contextmanager
    def session(self):
        '''
        Provide a transactional scope around a series of operations.
        '''
        session = self.Session()

        yield session

        try:
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def ids(
            self, limit=100, start=None,
            size=None, urls=None, hashes=None, file_name=None):
        '''
        Returns list of records stored by the backend.
        '''
        with self.session as session:
            query = session.query(IndexRecord)

            if start is not None:
                query = query.filter(IndexRecord.did > start)

            if size is not None:
                query = query.filter(IndexRecord.size == size)

            if file_name is not None:
                query = query.filter(IndexRecord.file_name == file_name)

            if urls is not None and urls:
                query = query.join(IndexRecord.urls)
                for u in urls:
                    query = query.filter(IndexRecordUrl.url == u)

            if hashes is not None and hashes:
                for h, v in hashes.items():
                    sub = session.query(IndexRecordHash.did)
                    sub = sub.filter(and_(
                        IndexRecordHash.hash_type == h,
                        IndexRecordHash.hash_value == v,
                    ))
                    query = query.filter(IndexRecord.did.in_(sub.subquery()))

            query = query.order_by(IndexRecord.did)
            query = query.limit(limit)

            return [i.did for i in query]

    def hashes_to_urls(self, size, hashes, start=0, limit=100):
        '''
        Returns a list of urls matching supplied size and hashes.
        '''
        with self.session as session:
            query = session.query(IndexRecordUrl)

            query = query.join(IndexRecordUrl.index_record)
            query = query.filter(IndexRecord.size == size)

            for h, v in hashes.items():
                # Select subset that matches given hash.
                sub = session.query(IndexRecordHash.did)
                sub = sub.filter(and_(
                    IndexRecordHash.hash_type == h,
                    IndexRecordHash.hash_value == v,
                ))

                # Filter anything that does not match.
                query = query.filter(IndexRecordUrl.did.in_(sub.subquery()))

            # Remove duplicates.
            query = query.distinct()

            # Return only specified window.
            query = query.offset(start)
            query = query.limit(limit)

            return [r.url for r in query]

    def add(self, form, size=None, urls=None, hashes=None, file_name=None):
        '''
        Creates a new record given urls and hashes.
        '''

        if urls is None:
            urls = []
        if hashes is None:
            hashes = {}
        with self.session as session:
            record = IndexRecord()

            base_version = BaseVersion()
            baseid = str(uuid.uuid4())
            base_version.baseid = baseid

            record.baseid = baseid
            record.file_name = file_name

            did = str(uuid.uuid4())
            record.did, record.rev = did, str(uuid.uuid4())[:8]

            record.form, record.size = form, size

            record.urls = [IndexRecordUrl(
                did=record,
                url=url,
            ) for url in urls]

            record.hashes = [IndexRecordHash(
                did=record,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]

            try:
                session.add(base_version)
                session.add(record)
                session.commit()

            except IntegrityError:
                raise UserError('{did} already exists'.format(did=did), 400)

            return record.did, record.rev, record.baseid

    def get(self, did):
        '''
        Gets a record given the record id.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            baseid = record.baseid
            rev = record.rev

            form = record.form
            size = record.size
            file_name = record.file_name

            urls = [u.url for u in record.urls]
            hashes = {h.hash_type: h.hash_value for h in record.hashes}

            created_date = record.created_date
            updated_date = record.updated_date

            ret = {
                'did': did,
                'baseid': baseid,
                'rev': rev,
                'size': size,
                'file_name': file_name,
                'urls': urls,
                'hashes': hashes,
                'form': form,
                'created_date': created_date,
                "updated_date": updated_date,
            }

        return ret

    def update(self, did, rev, urls=None, file_name=None):
        '''
        Updates an existing record with new values.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            if rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            if urls is not None:
                record.urls = [IndexRecordUrl(
                    did=record,
                    url=url
                ) for url in urls]
            if file_name is not None:
                record.file_name = file_name

            record.rev = str(uuid.uuid4())[:8]

            session.add(record)

            return record.did, record.baseid, record.rev

    def delete(self, did, rev):
        '''
        Removes record if stored by backend.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            if rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            session.delete(record)

    def add_version(
            self, did, form, size=None,
            file_name=None, urls=None, hashes=None):
        '''
        Add a record version given did
        '''
        with self.session as session:
            query = session.query(IndexRecord).filter_by(did=did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            baseid = record.baseid
            record = IndexRecord()
            did = str(uuid.uuid4())

            record.did = did
            record.baseid = baseid
            record.rev = str(uuid.uuid4())[:8]
            record.form = form
            record.size = size
            record.file_name = file_name

            record.urls = [IndexRecordUrl(
                did=record,
                url=url,
            ) for url in urls]

            record.hashes = [IndexRecordHash(
                did=record,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]

            try:
                session.add(record)
                session.commit()
            except IntegrityError:
                raise UserError('{did} already exists'.format(did=did), 400)

            return record.did, record.baseid, record.rev

    def get_all_versions(self, did):
        '''
        Get all record versions given did
        '''
        ret = dict()
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            query = session.query(IndexRecord)
            records = query.filter(IndexRecord.baseid == record.baseid).all()

            for idx, record in enumerate(records):
                rev = record.rev
                did = record.did
                form = record.form

                size = record.size
                file_name =record.file_name
                urls = [u.url for u in record.urls]
                hashes = {h.hash_type: h.hash_value for h in record.hashes}

                created_date = record.created_date
                updated_date = record.updated_date

                ret[idx] = {
                    'did': did,
                    'rev': rev,
                    'size': size,
                    'file_name': file_name,
                    'urls': urls,
                    'hashes': hashes,
                    'form': form,
                    'created_date': created_date,
                    'updated_date': updated_date,
                }

        return ret

    def get_latest_version(self, did):
        '''
        Get the lattest record version given did
        '''
        ret = {}
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            query = session.query(IndexRecord)
            records = query.filter(IndexRecord.baseid == record.baseid) \
                .order_by(IndexRecord.updated_date).all()

            if(not records):
                raise NoRecordFound('no record found')

            record = records[-1]

            rev = record.rev
            did = record.did

            form = record.form
            size = record.size
            file_name = record.file_name

            urls = [u.url for u in record.urls]
            hashes = {h.hash_type: h.hash_value for h in record.hashes}

            created_date = record.created_date
            updated_date = record.updated_date

            ret = {
                'did': did,
                'rev': rev,
                'size': size,
                'file_name': file_name,
                'urls': urls,
                'hashes': hashes,
                'form': form,
                'created_date': created_date,
                'updated_date': updated_date,
            }

        return ret

    def health_check(self):
        '''
        Does a health check of the backend.
        '''
        with self.session as session:
            try:
                query = session.execute('SELECT 1')
            except Exception as e:
                raise UnhealthyCheck()

            return True

    def __contains__(self, record):
        '''
        Returns True if record is stored by backend.
        Returns False otherwise.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == record)

            return query.exists()

    def __iter__(self):
        '''
        Iterator over unique records stored by backend.
        '''
        with self.session as session:
            for i in session.query(IndexRecord):
                yield i.did

    def totalbytes(self):
        '''
        Total number of bytes of data represented in the index.
        '''
        with self.session as session:
            result = session.execute(select([func.sum(IndexRecord.size)])).scalar()
            if result is None:
                return 0
            return long(result)

    def len(self):
        '''
        Number of unique records stored by backend.
        '''
        with self.session as session:

            return session.execute(select([func.count()]).select_from(IndexRecord)).scalar()


def migrate_1(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ALTER COLUMN size TYPE bigint;"
        .format(IndexRecord.__tablename__))

def migrate_2(session, **kwargs):
    '''
    Migrate db from version 1 -> 2
    In the case of the brand new db, we don't need to do migration since all the tables are the newest versions. We side-stepping this issue by catching exceptions 
    '''
    try:
        session.execute(
            "ALTER TABLE {} \
                ADD COLUMN baseid VARCHAR DEFAULT NULL, \
                ADD COLUMN created_date TIMESTAMP DEFAULT NOW(), \
                ADD COLUMN updated_date TIMESTAMP DEFAULT NOW()".format(IndexRecord.__tablename__))
    except ProgrammingError:
        session.rollback()
    session.commit()

    try:
        session.execute(
            "CREATE TABLE {} (baseid VARCHAR NOT NULL, PRIMARY KEY(baseid))"
            .format(BaseVersion.__tablename__))
    except ProgrammingError:
        session.rollback()
    session.commit()

    count = session.execute(
        "SELECT COUNT(*) FROM {};"
        .format(IndexRecord.__tablename__)).fetchone()[0]

    # create tmp_index_record table for fast retrival
    try:
        session.execute(
            "CREATE TABLE tmp_index_record AS SELECT did, ROW_NUMBER() OVER (ORDER BY did) AS RowNumber \
            FROM {}".format(IndexRecord.__tablename__))
    except ProgrammingError:
        session.rollback()

    for loop in range(count):
        baseid = str(uuid.uuid4())
        session.execute(
            "UPDATE index_record SET baseid = '{}'\
            WHERE did =  (SELECT did FROM tmp_index_record WHERE RowNumber = {});".format(baseid, loop+1))
        session.execute(
            "INSERT INTO {}(baseid) VALUES('{}');".format(BaseVersion.__tablename__,baseid))

    session.execute(
        "ALTER TABLE {} \
        ADD CONSTRAINT baseid_FK FOREIGN KEY (baseid) references base_version(baseid);"
        .format(IndexRecord.__tablename__))

    # drop tmp table
    session.execute(
        "DROP TABLE IF EXISTS tmp_index_record;"
        )


def migrate_3(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ADD COLUMN file_name VARCHAR;"
        .format(IndexRecord.__tablename__))

    session.execute(
        "CREATE INDEX {tb}__file_name_idx ON {tb} ( file_name )"
        .format(tb=IndexRecord.__tablename__))

# ordered schema migration functions that the index should correspond to
# CURRENT_SCHEMA_VERSION - 1 when it's written
SCHEMA_MIGRATION_FUNCTIONS = [migrate_1, migrate_2, migrate_3]
