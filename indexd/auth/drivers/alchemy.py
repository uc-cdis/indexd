import hashlib
import asyncio
from contextlib import asynccontextmanager

from authutils.token import get_jwt_token
from gen3authz.client.arborist.client import ArboristClient
from sqlalchemy import String, Column, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import declarative_base

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

        self.Session = async_sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False
        )

        if arborist is not None:
            arborist = ArboristClient(arborist_base_url=arborist)
        self.arborist = arborist

    @property
    @asynccontextmanager
    async def session(self):
        """
        Provide an asynchronous transactional scope around a series of operations.
        """
        async with self.Session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    def digest(password):
        """
        Digests a string.
        """
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    async def add(self, username, password):
        password = self.digest(password)
        async with self.session as session:
            query = select(AuthRecord).filter(AuthRecord.username == username)
            result = await session.execute(query)

            if result.scalar_one_or_none():
                raise AuthError("User {} already exists".format(username))

            new_record = AuthRecord(username=username, password=password)
            session.add(new_record)

    async def delete(self, username):
        async with self.session as session:
            query = select(AuthRecord).filter(AuthRecord.username == username)
            result = await session.execute(query)
            user = result.scalar_one_or_none()

            if not user:
                raise AuthError("User {} doesn't exist".format(username))

            await session.delete(user)

    async def auth(self, username, password):
        """
        Returns a dict of user information.
        Raises AuthError otherwise.
        """
        password = self.digest(password)
        async with self.session as session:
            # Check if any users are configured
            query = select(AuthRecord).limit(1)
            res = await session.execute(query)
            if not res.first():
                raise AuthError("No username / password configured in indexd")

            # Select on username / password.
            query = select(AuthRecord).filter(
                AuthRecord.username == username, AuthRecord.password == password
            )
            result = await session.execute(query)

            try:
                result.scalar_one()
            except NoResultFound:
                raise AuthError("username / password mismatch")

        context = {
            "username": username,
            # TODO include other user information
        }

        return context

    async def authz(self, method, resource):
        if not self.arborist:
            raise AuthError(
                "Arborist is not configured; cannot perform authorization check"
            )

        try:
            # A successful call from arborist returns a bool, else returns ArboristError
            try:
                # Arborist uses synchronous requests, so we wrap it in a thread
                authorized = await asyncio.to_thread(
                    self.arborist.auth_request,
                    get_jwt_token(),
                    "indexd",
                    method,
                    resource,
                )
            except Exception as e:
                logger.error(
                    f"Request to Arborist failed; now checking admin access. Details:\n{e}"
                )
                authorized = False

            if not authorized:
                # admins can perform all operations
                is_admin = await asyncio.to_thread(
                    self.arborist.auth_request,
                    get_jwt_token(),
                    "indexd",
                    method,
                    ["/services/indexd/admin"],
                )
                if not is_admin and not resource:
                    # if `authz` is empty (no `resource`), admin == access to
                    # `/programs` (deprecated - for backwards compatibility).
                    is_admin = await asyncio.to_thread(
                        self.arborist.auth_request,
                        get_jwt_token(),
                        "indexd",
                        method,
                        ["/programs"],
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
