import abc
from ..driver_base import SQLAlchemyDriverBase


class AuthDriverABC(SQLAlchemyDriverBase, metaclass=abc.ABCMeta):
    """
    Auth Driver Abstract Base Class

    Driver interface for authorization.
    """

    def __init__(self, conn, **config):
        super().__init__(conn, **config)

    @abc.abstractmethod
    def auth(self, username, password):
        """
        Returns a dict of user information.
        Raises AuthError otherwise.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def authz(self, method, resource):
        """
        RBAC Authorization.
        Raises AuthError if the permission is denied.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, username, password):
        """
        Create an user.
        Raises AuthError if user already exists.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def delete(self, username):
        """
        Delete an user
        Raises AuthError if user doesn't exist.
        """
        raise NotImplementedError("TODO")
