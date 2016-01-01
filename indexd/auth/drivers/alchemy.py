import json
import uuid
import hashlib

from contextlib import contextmanager

from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base

from indexd import auth

from indexd.auth.driver import AuthDriverABC

from indexd.auth.errors import AuthError


Base = declarative_base()

class AuthRecord(Base):
    '''
    Base auth record representation.
    '''
    __tablename__ = 'auth_record'

    username = Column(String, primary_key=True)
    password = Column(String)

class SQLAlchemyAuthDriver(AuthDriverABC):
    '''
    SQLAlchemy implementation of auth driver.
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

    def auth(self, username, password):
        '''
        Returns a dict of user information.
        Raises AutheError otherwise.
        '''
        password = hashlib.sha256(password).hexdigest()
        with self.session as session:
            query = session.query(AuthRecord)

            # Select on username / password.
            query = query.filter(AuthRecord.username == username)
            query = query.filter(AuthRecord.password == password)

            try: query.one()
            except NoResultFound as err:
                raise AuthError('username / password mismatch')

        context = {
            'username': username,
            # TODO include other user information
        }

        return context
