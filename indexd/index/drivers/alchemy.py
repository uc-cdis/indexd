import json
import uuid

from contextlib import contextmanager

from sqlalchemy import and_
from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.ext.declarative import declarative_base

from indexd import index

from indexd.index.driver import IndexDriverABC

from indexd.index.errors import NoRecordFound
from indexd.index.errors import MultipleRecordsFound
from indexd.index.errors import RevisionMismatch


Base = declarative_base()


class IndexRecord(Base):
    '''
    Base index record representation.
    '''
    __tablename__ = 'index_record'

    did = Column(String, primary_key=True)
    rev = Column(String)
    form = Column(String)
    size = Column(Integer)

    urls = relationship('IndexRecordUrl',
                        backref='index_record',
                        cascade='all, delete-orphan',
                        )

    hashes = relationship('IndexRecordHash',
                          backref='index_record',
                          cascade='all, delete-orphan',
                          )

    apis = relationship(
        'IndexRecordAPI', backref='index_record_url', cascade='all, delete-orphan',)


class IndexRecordUrl(Base):
    '''
    Base index record url representation.
    '''
    __tablename__ = 'index_record_url'

    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    url = Column(String, primary_key=True)


class IndexRecordAPI(Base):
    '''
    Base index record api.
    '''

    __tablename__ = 'index_record_api'

    did = Column(String, ForeignKey('index_record_url.did'), primary_key=True)
    url = Column(String, ForeignKey('index_record_url.url'), primary_key=True)
    api = Column(String)


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

    def __init__(self, conn, **config):
        '''
        Initialize the SQLAlchemy database driver.
        '''
        self.engine = create_engine(conn, **config)

        Base.metadata.bind = self.engine
        Base.metadata.create_all()

        self.Session = sessionmaker(bind=self.engine)

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

    def ids(self, limit=100, start=None, size=None, urls=None, hashes=None):
        '''
        Returns list of records stored by the backend.
        '''
        # TODO add dids to filter on
        with self.session as session:
            query = session.query(IndexRecord)

            if start is not None:
                query = query.filter(IndexRecord.did > start)

            if size is not None:
                query = query.filter(IndexRecord.size == size)

            if urls is not None and urls:
                query = query.join(IndexRecord.urls)
                for u in urls:
                    query = query.filter(IndexRecordUrl.url == u)

            if hashes is not None and hashes:
                for h, v in hashes.items():
                    sub = session.query(IndexRecord)
                    sub = sub.join(IndexRecord.hashes)
                    sub = sub.filter(and_(
                        IndexRecordHash.hash_type == h,
                        IndexRecordHash.hash_value == v,
                    ))
                    query = query.intersect(sub)

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
                sub = session.query(IndexRecordUrl)
                sub = sub.join(IndexRecord.hashes)
                sub = sub.filter(and_(
                    IndexRecordHash.hash_type == h,
                    IndexRecordHash.hash_value == v,
                ))

                # Filter anything that does not match.
                query = query.intersect(sub)

            # Remove duplicates.
            query = query.distinct()

            # Return only specified window.
            query = query.offset(start)
            query = query.limit(limit)

            return [r.url for r in query]

    def add(self, form, size=None, urls={}, hashes={}):
        '''
        Creates a new record given urls and hashes.
        '''
        if form not in index.FORMS:
            raise ValueError('form must be one of: %s' % index.FORMS)

        if size is not None and size < 0:
            raise ValueError('size must be non-negative')

        with self.session as session:
            record = IndexRecord()

            record.did = str(uuid.uuid4())
            record.rev = str(uuid.uuid4())[:8]

            record.form = form
            record.size = size

            record.urls = [IndexRecordUrl(
                did=record,
                url=url,
            ) for url in urls]

            record.apis = [IndexRecordAPI(
                did=record,
                url=url,
                api=urls[url],
            ) for url in urls]

            record.hashes = [IndexRecordHash(
                did=record,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]

            session.add(record)

            return record.did, record.rev

    def get(self, did):
        '''
        Gets a record given the record id.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound as err:
                raise NoRecordFound('no record found')
            except MultipleResultsFound as err:
                raise MultipleRecordsFound('multiple records found')

            rev = record.rev

            form = record.form
            size = record.size

            urls = [u.url for u in record.urls]
            hashes = {h.hash_type: h.hash_value for h in record.hashes}

        ret = {
            'did': did,
            'rev': rev,
            'size': size,
            'urls': urls,
            'hashes': hashes,
            'form': form,
        }

        return ret

    def update(self, did, rev, size=None, urls=None, hashes=None):
        '''
        Updates an existing record with new values.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound as err:
                raise NoRecordFound('no record found')
            except MultipleResultsFound as err:
                raise MultipleRecordsFound('multiple records found')

            if rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            if size is not None:
                record.size = size

            if urls is not None:
                record.urls = [IndexRecordUrl(
                    did=record,
                    url=url
                ) for url in urls]

            if hashes is not None:
                record.hashes = [IndexRecordHash(
                    did=record,
                    hash_type=h,
                    hash_value=v,
                ) for h, v in hashes.items()]

            record.rev = str(uuid.uuid4())[:8]

            session.add(record)

            return record.did, record.rev

    def delete(self, did, rev):
        '''
        Removes record if stored by backend.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound as err:
                raise NoRecordFound('no record found')
            except MultipleResultsFound as err:
                raise MultipleRecordsFound('multiple records found')

            if rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            session.delete(record)

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

    def __len__(self):
        '''
        Number of unique records stored by backend.
        '''
        with self.session as session:
            return session.query(IndexRecord).count()
