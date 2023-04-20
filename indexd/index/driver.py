import abc
from ..driver_base import SQLAlchemyDriverBase


class IndexDriverABC(SQLAlchemyDriverBase, metaclass=abc.ABCMeta):
    """
    Index Driver Abstract Base Class

    Driver interface for interacting with index backends.
    """

    def __init__(self, conn, **config):
        super().__init__(conn, **config)

    @abc.abstractmethod
    def ids(
        self,
        limit=100,
        start=None,
        size=None,
        urls=None,
        acl=None,
        authz=None,
        hashes=None,
        file_name=None,
        version=None,
        uploader=None,
        metadata=None,
        ids=None,
        urls_metadata=None,
        negate_params=None,
    ):
        """
        Returns a list of records stored by the backend.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def get_urls(self, size=None, hashes=None, ids=None, start=0, limit=100):
        """
        Returns a list of urls matching supplied size and hashes.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def add(
        self,
        form,
        did=None,
        size=None,
        file_name=None,
        metadata=None,
        urls_metadata=None,
        version=None,
        urls=None,
        acl=None,
        authz=None,
        hashes=None,
        baseid=None,
        uploader=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
    ):
        """
        Creates record for given data.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def get(self, did):
        """
        Gets a record given the record id.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def update(self, did, rev, changing_fields):
        """
        Updates record with new values.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def delete(self, did, rev):
        """
        Deletes record.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def add_version(
        self,
        current_did,
        form,
        new_did=None,
        size=None,
        file_name=None,
        metadata=None,
        urls_metadata=None,
        version=None,
        urls=None,
        acl=None,
        authz=None,
        hashes=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
    ):
        """
        Add a record version given did
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def get_all_versions(self, did):
        """
        Get all record versions given did
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def get_latest_version(self, did, has_version=None):
        """
        Get the lattest record version given did
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def health_check(self):
        """
        Performs a health check.
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
    def totalbytes(self):
        """
        Returns the total bytes of the data represented in the index.
        """
        raise NotImplementedError("TODO")

    @abc.abstractmethod
    def len(self):
        """
        Returns the number of unique records stored by backend.
        """
        raise NotImplementedError("TODO")
