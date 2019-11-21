import abc
from ..driver_base import SQLAlchemyDriverBase


class AliasDriverABC(SQLAlchemyDriverBase, metaclass=abc.ABCMeta):
    """
    Alias Driver Abstract Base Class

    Driver interface for interacting with alias backends.
    """

    def __init__(self, conn, **config):
        super().__init__(conn, **config)

    @abc.abstractmethod
    def aliases(self, limit=100, start="", size=None, urls=None, hashes=None):
        """
        Returns a list of aliases.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def upsert(
        self,
        name,
        rev=None,
        size=None,
        hashes=None,
        release=None,
        metastring=None,
        host_authorities=None,
        keeper_authority=None,
    ):
        """
        Update or insert alias record.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def get(self, did):
        """
        Gets a record given the record id.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def delete(self, did, rev):
        """
        Deletes record.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def __contains__(self, did):
        """
        Returns True if record is stored by backend.
        Returns False otherwise.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def __iter__(self):
        """
        Returns an iterator over unique records stored by backend.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def __len__(self):
        """
        Returns the number of unique records stored by backend.
        """
        raise NotImplementedError("TODO")
