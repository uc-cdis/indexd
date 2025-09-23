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
from indexd.auth.drivers import request_auth_cache

from indexd.auth.errors import AuthError, AuthzError

from cdislogging import get_logger

logger = get_logger(__name__)


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
        except Exception:
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

        try:
            # A successful call from arborist returns a bool, else returns ArboristError
            try:
                authorized = self.cached_auth_request(get_jwt_token(), "indexd", method, resource)
            except Exception as e:
                logger.error(
                    f"Request to Arborist failed; now checking admin access. Details:\n{e}"
                )
                authorized = False

            if not authorized:
                # admins can perform all operations
                is_admin = self.arborist.auth_request(
                    get_jwt_token(), "indexd", method, ["/services/indexd/admin"]
                )
                if not is_admin and not resource:
                    # if `authz` is empty (no `resource`), admin == access to
                    # `/programs` (deprecated - for backwards compatibility).
                    is_admin = self.arborist.auth_request(
                        get_jwt_token(), "indexd", method, ["/programs"]
                    )
                    if is_admin:
                        logger.warning(
                            "The indexd admin '/programs' logic is deprecated. Please update your policy to '/services/indexd/admin'"
                        )
                if not is_admin:
                    raise AuthError("Permission denied.")
        except Exception as err:
            logger.error(err)
            raise AuthzError(err)

    def resources(self):
        """
        Returns a list of resources for the given user.
        """
        if not self.arborist:
            raise AuthError(
                "Arborist is not configured; cannot perform authorization check"
            )
        token = get_jwt_token()
        try:
            authz_resources = self.caching_auth_mapping(token)
        except Exception as err:
            raise AuthError(
                "Failed to get resources from Arborist. Please check your Arborist configuration."
            )
        return authz_resources

    # cache the result of the auth request
    @request_auth_cache()
    def caching_auth_mapping(self, token):
        """
        Returns a list of resources the user has access to.
        Uses Arborist if available.
        If a token is provided, it will use that token to get the auth mapping.
        If no token is provided, it will use the default auth mapping.
        """
        if token:
            resources = self.arborist.auth_mapping(
                jwt=token
            )
        else:
            resources = self.arborist.auth_mapping()
        return resources

    @request_auth_cache()
    def cached_auth_request(self, token, service, method, resource):
        """
        Makes an authenticated request to Arborist and caches the result.
        This method is used to check if the user has access to a specific resource
        with a specific method.
        """
        return self.arborist.auth_request(
            token, service, method, resource
        )


