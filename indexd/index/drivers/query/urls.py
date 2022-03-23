from sqlalchemy import and_, func

from indexd.errors import UserError
from indexd.index.drivers.alchemy import (
    IndexRecord,
    IndexRecordUrlMetadataJsonb,
)
from indexd.index.drivers.query import URLsQueryDriver

driver_query_map = {
    "sqlite": dict(array_agg=func.group_concat, string_agg=func.group_concat),
    "postgresql": dict(array_agg=func.array_agg, string_agg=func.string_agg)
}


class AlchemyURLsQueryDriver(URLsQueryDriver):
    """SQLAlchemy based impl"""

    def __init__(self, alchemy_driver):
        """ Queries index records based on URL
        Args:
            alchemy_driver (indexd.index.drivers.alchemy.SQLAlchemyIndexDriver):
        """
        self.driver = alchemy_driver

    def query_urls(self, exclude=None, include=None, versioned=None, exclude_deleted=False, offset=0, limit=1000,
                   fields="did,urls", **kwargs):
        """
        Get a list of document fields matching the search parameters
        Args:
            include (str): If defined, at least one URL in a document must contain this string to be included in search.
            exclude (str): If defined, no URL in a document may contain this string to be included in search.
            versioned (bool): If None (default), search documents regardless of whether they are versioned or not. If
                True, filter only for versioned documents. If False, filter only for un-versioned documents.
            exclude_deleted (bool): If True, exclude deleted documents from search. If False, include deleted
                and not deleted documents in search.
            offset (int): Defines a position offset, the first n results to skip
            limit (int): Defines the number of results to return
            fields (str): Comma separated list (defaults to did,urls) of fields to return
        Returns:
            list (dict): matching documents with specified return fields
        """
        if kwargs:
            raise UserError("Unexpected query parameter(s) {}".format(kwargs.keys()))

        with self.driver.session as session:
            # special database specific functions dependent of the selected dialect
            q_func = driver_query_map.get(session.bind.dialect.name)

            query = session.query(
                IndexRecordUrlMetadataJsonb.did,
                q_func["string_agg"](IndexRecordUrlMetadataJsonb.url, ","),
            )

            # handle filters for versioned and/or exclude_deleted flags
            query = self._filter_indexrecord(query, versioned, exclude_deleted)

            query = query.group_by(IndexRecordUrlMetadataJsonb.did)

            # add url filters
            if include and exclude:
                query = query.having(and_(~q_func["string_agg"](IndexRecordUrlMetadataJsonb.url, ",").contains(exclude),
                                          q_func["string_agg"](IndexRecordUrlMetadataJsonb.url, ",").contains(include)))
            elif include:
                query = query.having(q_func["string_agg"](IndexRecordUrlMetadataJsonb.url, ",").contains(include))
            elif exclude:
                query = query.having(~q_func["string_agg"](IndexRecordUrlMetadataJsonb.url, ",").contains(exclude))
            # [("did", "urls")]
            record_list = query.order_by(IndexRecordUrlMetadataJsonb.did.asc()).offset(offset).limit(limit).all()
        return self._format_response(fields, record_list)

    def query_metadata_by_key(self, key, value, url=None, versioned=None, exclude_deleted=False, offset=0,
                              limit=1000, fields="did,urls,rev", **kwargs):
        """
        Get a list of document fields matching the search parameters
        Args:
            key (str): metadata key
            value (str): metadata value for key
            url (str): full url or pattern for limit to
            versioned (bool): If None (default), search documents regardless of whether they are versioned or not. If
                True, filter only for versioned documents. If False, filter only for un-versioned documents.
            exclude_deleted (bool): If True, exclude deleted documents from search. If False, include deleted
                and not deleted documents in search.
            offset (int): Defines a position offset, the first n results to skip
            limit (int): Defines the number of results to return
            fields (str): Comma separated list (defaults to did,urls) of fields to return
        Returns:
            list (dict): matching documents with specified return fields
        """
        if kwargs:
            raise UserError("Unexpected query parameter(s) {}".format(kwargs.keys()))

        with self.driver.session as session:
            query = session.query(IndexRecordUrlMetadataJsonb.did,
                                  IndexRecordUrlMetadataJsonb.url,
                                  IndexRecord.rev)
            if key == "type":
                query = query.filter(
                    IndexRecord.did == IndexRecordUrlMetadataJsonb.did,
                    IndexRecordUrlMetadataJsonb.type == value)
            elif key == "state":
                query = query.filter(
                    IndexRecord.did == IndexRecordUrlMetadataJsonb.did,
                    IndexRecordUrlMetadataJsonb.state == value)
            else:
                query = query.filter(
                    IndexRecord.did == IndexRecordUrlMetadataJsonb.did,
                    IndexRecordUrlMetadataJsonb.urls_metadata[key].astext == value)

            # handle filters for versioned and/or exclude_deleted flags
            query = self._filter_indexrecord(query, versioned, exclude_deleted)

            # add url filter
            if url:
                query = query.filter(IndexRecordUrlMetadataJsonb.url.like("%{}%".format(url)))

            # [('did', 'url', 'rev')]
            record_list = query.order_by(IndexRecordUrlMetadataJsonb.did.asc()).offset(offset).limit(limit).all()
        return self._format_response(fields, record_list)

    @staticmethod
    def _filter_indexrecord(query, versioned, exclude_deleted):
        """ Handles outer join to IndexRecord for versioned and exclude_deleted filters if filter flags exist """
        if versioned is not None or exclude_deleted:
            query = query.outerjoin(IndexRecord)

            # handle not deleted filter
            if exclude_deleted:
                query = query.filter(
                    (func.lower(IndexRecord.index_metadata["deleted"].astext) == "true").isnot(True)
                )

            # handle version filter if not None
            if versioned is True:  # retrieve only those with a version number
                query = query.filter(IndexRecord.version.isnot(None))
            elif versioned is False:  # retrieve only those without a version number
                query = query.filter(~IndexRecord.version.isnot(None))

        return query

    @staticmethod
    def _format_response(requested_fields, record_list):
        """ loops through the query result and removes undesired columns and converts result of urls string_agg to list
        Args:
            requested_fields (str): comma separated list of fields to return, if not specified return all fields
            record_list (list(tuple]): must be of the form [(did, urls, rev)], rev is not required for urls query
        Returns:
            list[dict]: list of response dicts
        """
        result = []
        provided_fields_dict = {k: 1 for k in requested_fields.split(",")}
        for record in record_list:
            resp_dict = {}
            if provided_fields_dict.get("did"):
                resp_dict["did"] = record[0]
            if provided_fields_dict.get("urls"):
                resp_dict["urls"] = record[1].split(",") if record[1] else []

            # check if record is returned in tuple
            if provided_fields_dict.get("rev") and len(record) == 3:
                resp_dict["rev"] = record[2]
            result.append(resp_dict)
        return result
