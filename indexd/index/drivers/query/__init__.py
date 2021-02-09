from abc import ABCMeta, abstractmethod


class URLsQueryDriver(object, metaclass=ABCMeta):
    """Relatively abstract class for URLs querying, useful when support for other drivers is added"""

    @abstractmethod
    def query_urls(
        self,
        exclude=None,
        include=None,
        versioned=None,
        offset=0,
        limit=1000,
        fields="did,urls",
        **kwargs
    ):
        """The exclude and include patterns are used to match per record. That is a record wth 3 urls will
            be returned/excluded if any one of the URLs match the include/exclude patterns
        Args:
            exclude (str): url pattern to exclude
            include (str): url pattern to include
            versioned (str): query only versioned records or not
            offset (int):
            limit (int):
            fields (str): comma separated list of fields to return, if not specified return all fields
            kwargs (dict): unexpected query parameters
        Returns:
            list: result list
        """
        raise NotImplementedError("TODO")

    @abstractmethod
    def query_metadata_by_key(
        self,
        key,
        value,
        url=None,
        versioned=None,
        offset=0,
        limit=1000,
        fields="dir,urls,rev",
        **kwargs
    ):
        """Queries urls_metadata based on provided key and value
        Args:
            key (str): urls_metadata key
            value (str): urls_metadata key value
            url (str): URL pattern to match
            versioned (str): if True/False return only versioned/unversioned entries else return all
            offset (int): query offset
            limit (int): Maximum rows to return
            fields (str): comma separated list of fields to return, if not specified return all fields
            kwargs (dict): unexpected query parameters
        Returns:
            list: result list
        """
        raise NotImplementedError("TODO")
