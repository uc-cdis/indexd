import hashlib
from contextlib import contextmanager

from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from indexd.auth.driver import AuthDriverABC
from indexd.auth.errors import AuthError
from indexd.index.errors import UnhealthyCheck

Base = declarative_base()

class AuthRecord(Base):
    """
    Base auth record representation.
    """
    __tablename__ = 'auth_record'

    username = Column(String, primary_key=True)
    password = Column(String)


class SQLAlchemyAuthDriver(AuthDriverABC):
    """
    SQLAlchemy implementation of auth driver.
    """

    def __init__(self, conn, **config):
        """
        Initialize the SQLAlchemy database driver.
        """
        super(SQLAlchemyAuthDriver, self).__init__(conn, **config)
        Base.metadata.bind = self.engine
        Base.metadata.create_all()
        self.Session = sessionmaker(bind=self.engine)

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

    @staticmethod
    def digest(password):
        """
        Digests a string.
        """
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    def add(self, username, password):
        password = self.digest(password)
        with self.session as session:
            if (session.query(AuthRecord)
                .filter(AuthRecord.username == username)
                .first()):
                raise AuthError('User {} already exists'.format(username))

            new_record = AuthRecord(
                username=username, password=password)
            session.add(new_record)

    def delete(self, username):
        with self.session as session:
            user = session.query(AuthRecord).filter(
                AuthRecord.username == username).first()
            if not user:
                raise AuthError("User {} doesn't exist".format(username))
            session.delete(user)

    def health_check(self):
        """
        Does a health check of the backend.
        """
        with self.session as session:
            try:
                session.execute('SELECT 1')
            except Exception:
                raise UnhealthyCheck()

            return True

    def auth(self, username, password):
        """
        Returns a dict of user information.
        Raises AuthError otherwise.
        """
        password = self.digest(password)
        with self.session as session:
            query = session.query(AuthRecord)

            # Select on username / password.
            query = query.filter(AuthRecord.username == username)
            query = query.filter(AuthRecord.password == password)

            try:
                query.one()
            except NoResultFound:
                raise AuthError('username / password mismatch')

        context = {
            'username': username,
            # TODO include other user information
        }

        return context
