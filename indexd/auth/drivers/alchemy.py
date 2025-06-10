import hashlib
import sys

from contextlib import contextmanager

from authutils.token import get_jwt_token
from gen3authz.client.arborist.client import ArboristClient
from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base

from indexd.auth.driver import AuthDriverABC

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
        print("DEBUG auth called with username: {}, password: {}".format(username, password), file=sys.stderr)
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
                print("DEBUG auth OK", file=sys.stderr)
            except NoResultFound as err:
                print("DEBUG auth AuthError username / password mismatch", file=sys.stderr)
                raise AuthError("username / password mismatch")

        context = {
            "username": username,
            # TODO include other user information
        }

        return context

    def authz(self, method, resource):
        print("DEBUG authz called with method: {}, resource: {}".format(method, resource), file=sys.stderr)
        if not self.arborist:
            raise AuthError(
                "Arborist is not configured; cannot perform authorization check"
            )

        try:
            # A successful call from arborist returns a bool, else returns ArboristError
            try:
                token = get_jwt_token()
                if not token:
                    raise AuthzError("No JWT token found for authorization check")
                authorized = self.arborist.auth_request(
                    token, "indexd", method, resource
                )
            except Exception as e:
                logger.error(
                    f"Request to Arborist failed; now checking admin access. Details:\n{e}"
                )
                print(f"DEBUG Request to Arborist failed; now checking admin access. Details:\n{e}", file=sys.stderr)
                authorized = False

            print(f"DEBUG authorized {('indexd', method, resource)} {authorized}", file=sys.stderr)
            if not authorized:
                token = get_jwt_token()
                if not token:
                    raise AuthError("No JWT token found for authorization check")
                # admins can perform all operations
                is_admin = self.arborist.auth_request(
                    token, "indexd", method, ["/services/indexd/admin"]
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
                    raise AuthError("Permission denied. (not is_admin)")
        except AuthError as err:
            logger.error(err)
            raise err
        except AuthzError as err:
            logger.error(err)
            raise err
        except Exception as err:
            logger.error(err)
            raise AuthzError(err)

    def resources(self):
        """
        Returns a list of resources for the given user.
        """
        print("DEBUG resources called", file=sys.stderr)
        if not self.arborist:
            raise AuthError(
                "Arborist is not configured; cannot perform authorization check"
            )
        print("DEBUG resources calling get_jwt_token", file=sys.stderr)
        token = get_jwt_token()
        print(f"DEBUG resources calling auth_mapping {token}", file=sys.stderr)
        try:
            _ = self.arborist.auth_mapping(
                jwt=token
            )
            print(("DEBUG resources called auth_mapping got", _), file=sys.stderr)
            return _
        except Exception as err:
            print(f"DEBUG request failed with {err}", file=sys.stderr)
            raise AuthError(
                "Failed to get resources from Arborist. Please check your Arborist configuration."
            )

