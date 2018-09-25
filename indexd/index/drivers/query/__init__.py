from abc import ABCMeta, abstractmethod


class URLsQueryDriver(object):
    """Relatively abstract class for URLs querying, useful when support for other drivers is added"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def query_urls(self, exclude=None, include=None, versioned=None, offset=0, limit=1000):
        """ The exclude and include patterns are used to match per record. That is a record wth 3 urls will
            be returned/excluded if any one of the URLs match the include/exclude patterns
        Args:
            exclude (str): url pattern to exclude
            include (str): url pattern to include
            versioned (bool): query only versioned records or not
            offset (int):
            limit (int):
        Returns:
            list: result list
        """
        pass

    @abstractmethod
    def query_metadata_by_key(self, key, value, url=None, versioned=None, offset=0, limit=1000):
        """ Queries urls_metadata based on provided key and value
        Args:
            key (str): urls_metadata key
            value (str): urls_metadata key value
            url (str): URL pattern to match
            versioned (bool): if True/False return only versioned/unversioned entries else return all
            offset (int): query offset
            limit (int): Maximum rows to return
        Returns:
            list: result list
        """
        pass
