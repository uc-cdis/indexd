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
    async def aliases(self, limit=100, start="", size=None, urls=None, hashes=None):
        """
        Returns a list of aliases.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    async def upsert(
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
    async def get(self, did):
        """
        Gets a record given the record id.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    async def delete(self, did, rev):
        """
        Deletes record.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    async def has_record(self, did):
        """
        Async replacement for __contains__.
        Returns True if record is stored by backend.
        Returns False otherwise.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    async def __aiter__(self):
        """
        Returns an async iterator over unique records stored by backend.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    async def len(self):
        """
        Async replacement for __len__.
        Returns the number of unique records stored by backend.
        """
        raise NotImplementedError("TODO")
