from sqlalchemy import func, and_

from indexd.index.drivers.alchemy import IndexRecord, IndexRecordUrl, IndexRecordUrlMetadata
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

    def query_urls(self, exclude=None, include=None, only_versioned=True, offset=0, limit=1000):

        with self.driver.session as sxn:
            # special database specific functions dependent of the selected dialect
            q_func = driver_query_map.get(sxn.bind.dialect.name)

            query = sxn.query(IndexRecordUrl.did, q_func['string_agg'](IndexRecordUrl.url, ",").label("urls"))\
                .outerjoin(IndexRecord)

            # filter by version
            if only_versioned is True:
                query = query.filter(IndexRecord.version.isnot(None))
            elif only_versioned is False:
                query = query.filter(~IndexRecord.version.isnot(None))

            query = query.group_by(IndexRecordUrl.did)

            # add url filters
            if include and exclude:
                query = query.having(and_(~q_func['string_agg'](IndexRecordUrl.url, ",").contains(exclude),
                                     q_func['string_agg'](IndexRecordUrl.url, ",").contains(include)))
            elif include:
                query = query.having(q_func['string_agg'](IndexRecordUrl.url, ",").contains(include))
            elif exclude:
                query = query.having(~q_func['string_agg'](IndexRecordUrl.url, ",").contains(exclude))

            # [('did', 'urls')]
            record_list = query.order_by(IndexRecordUrl.did.asc()).offset(offset).limit(limit).all()
        return record_list

    def query_metadata_by_key(self, key, value, url=None, only_versioned=True, offset=0, limit=1000):

        with self.driver.session as sxn:
            query = sxn.query(IndexRecordUrlMetadata.did,
                              IndexRecordUrlMetadata.url,
                              IndexRecord.rev)\
                .filter(IndexRecord.did == IndexRecordUrlMetadata.did) \
                .filter(IndexRecordUrlMetadata.key == key) \
                .filter(IndexRecordUrlMetadata.value == value)

            # filter by version
            if only_versioned is True:
                query = query.filter(IndexRecord.version.isnot(None))
            elif only_versioned is False:
                query = query.filter(~IndexRecord.version.isnot(None))

            # add url filter
            if url:
                query = query.filter(IndexRecordUrlMetadata.url.like("%{}%".format(url)))

            # [('did', 'url', 'rev')]
            record_list = query.order_by(IndexRecordUrlMetadata.did.asc()).offset(offset).limit(limit).all()
        return record_list
