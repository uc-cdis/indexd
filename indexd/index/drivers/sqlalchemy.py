import json
import uuid

from contextlib import contextmanager

import sqlalchemy

from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

from indexd.index import driver
from indexd.index import errors

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

class SQLAlchemyIndexDriver(driver.IndexDriverABC):
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
        
        try: session.commit()
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
            query = query.filter(IndexRecord.did > start)
            query = query.limit(limit)
            
            if size is not None:
                query = query.filter(IndexRecord.size == size)
            
            if urls is not None:
                # TODO add filters for urls
                pass
            
            if hashes is not None:
                # TODO add filters for hashes
                pass
            
            return [i.did for i in query]

    def add(self, form, size=0, urls=[], hashes={}):
        '''
        Creates a new record given urls and hashes.
        '''
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
            
            record.hashes = [IndexRecordHash(
                did=record,
                hash_type=h,
                hash_value=v,
            ) for h,v in hashes.items()]
            
            session.add(record)
            
            return record.did, record.rev

    def get(self, did):
        '''
        Gets a record given the record id.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)
            
            try: record = query.one()
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
            'type': form,
        }
        
        return ret

    def update(self, did, rev, size=None, urls=None, hashes=None):
        '''
        Updates an existing record with new values.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)
            
            try: record = query.one()
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
                ) for h,v in hashes.items()]
            
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
            
            try: record = query.one()
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
