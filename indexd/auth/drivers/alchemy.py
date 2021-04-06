import hashlib

from contextlib import contextmanager

from authutils.token import get_jwt_token
from gen3authz.client.arborist.client import ArboristClient
from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base

from indexd.auth.driver import AuthDriverABC

from indexd.auth.errors import AuthError


Base = declarative_base()


class AuthRecord(Base):
    """
    Base auth record representation.
    """

    __tablename__ = "auth_record"

    username = Column(String, primary_key=True)
    password = Column(String)


class SQLAlchemyAuthDriver(AuthDriverABC):
    """
    SQLAlchemy implementation of auth driver.
    """

    def __init__(self, conn, arborist=None, **config):
        """
        Initialize the SQLAlchemy database driver.
        """
        super().__init__(conn, **config)
        Base.metadata.bind = self.engine
        Base.metadata.create_all()
        self.Session = sessionmaker(bind=self.engine)
        if arborist is not None:
            arborist = ArboristClient(arborist_base_url=arborist)
        self.arborist = arborist

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
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def add(self, username, password):
        password = self.digest(password)
        with self.session as session:
            if (
                session.query(AuthRecord)
                .filter(AuthRecord.username == username)
                .first()
            ):
                raise AuthError("User {} already exists".format(username))

            new_record = AuthRecord(username=username, password=password)
            session.add(new_record)

    def delete(self, username):
        with self.session as session:
            user = (
                session.query(AuthRecord)
                .filter(AuthRecord.username == username)
                .first()
            )
            if not user:
                raise AuthError("User {} doesn't exist".format(username))
            session.delete(user)

    def auth(self, username, password):
        """
        Returns a dict of user information.
        Raises AutheError otherwise.
        """
        password = self.digest(password)
        with self.session as session:
            query = session.query(AuthRecord)
            if not query.first():
                raise AuthError("No username / password configured in indexd")

            # Select on username / password.
            query = query.filter(AuthRecord.username == username)
            query = query.filter(AuthRecord.password == password)

            try:
                query.one()
            except NoResultFound as err:
                raise AuthError("username / password mismatch")

        context = {
            "username": username,
            # TODO include other user information
        }

        return context

    def authz(self, method, resource):
        if not self.arborist:
            raise AuthError(
                "Arborist is not configured; cannot perform authorization check"
            )
        if not resource:
            # if the `authz` is empty, admins should still be able to perform
            # operations on the record. For now, admin = access to `/programs`.
            # TODO: Figure out how to handle Gen3 operational admins in a better way
            resource = ["/programs"]

        try:
            if not self.arborist.auth_request(
                get_jwt_token(), "indexd", method, resource
            ):
                raise AuthError("Permission denied.")
        except Exception as e:
            raise AuthError(e)
