import json
import uuid

from contextlib import contextmanager

import sqlalchemy

from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

from indexd.index import driver
from indexd.index import errors

from indexd.index.errors import NoRecordError
from indexd.index.errors import MultipleRecordsError


Base = declarative_base()

class IndexRecord(Base):
    '''
    Base index record representation.
    '''
    __tablename__ = 'index_records'

    did = Column(String, primary_key=True)
    rev = Column(String)
    form = Column(String)
    size = Column(Integer)

class IndexRecordUrl(Base):
    '''
    Base index record url representation.
    '''
    __tablename__ = 'index_record_urls'

    did = Column(String, primary_key=True)
    url = Column(String, primary_key=True)

class IndexRecordHash(Base):
    '''
    Base index record hash representation.
    '''
    __tablename__ = 'index_record_hashes'

    did = Column(String, primary_key=True)
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

    def ids(self, limit=100, start=None, size=None, hashes={}):
        '''
        Returns list of records stored by the backend.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did > start)
            query = query.limit(limit)
            return [i.did for i in query]

    def __getitem__(self, record):
        '''
        Returns record if stored by backend.
        Raises KeyError otherwise.
        '''
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == record)
            
            try: result = query.one()
            except NoResultFound as err:
                raise NoRecordError('no record found')
            except MultipleResultsFound as err:
                raise MultipleRecordsError('multiple records found')
            
            rev = result.rev
            form = result.form
            size = result.size
            
            urls = session.query(IndexRecordUrl)
            urls = urls.filter(IndexRecordUrl.did == record)
            urls = [i.url for i in urls]
            
            hashes = session.query(IndexRecordHash)
            hashes = hashes.filter(IndexRecordHash.did == record)
            hashes = {i.hash_type: i.hash_value for i in hashes}
        
        ret = {
            'id': record,
            'rev': rev,
            'size': size,
            'urls': urls,
            'hashes': hashes,
            'type': form,
        }
        
        return ret

    def __setitem__(self, record, data):
        '''
        Replaces record if stored by backend.
        Raises KeyError otherwise.
        '''
        with self.session as session:
            try: result = self[record]
            except NoRecordError as err:
                result = IndexRecord()
            
            result.did = record
            result.rev = data.get('rev')
            result.form = data.get('form')
            result.size = data.get('size')
            result.urls = data.get('urls')
            result.hashes = data.get('hashes')
            
            session.add(result)

    def __delitem__(self, record):
        '''
        Removes record if stored by backend.
        Raises KeyError otherwise.
        '''
        with self.session as session:
            result = self[record]
            session.delete(result)

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
