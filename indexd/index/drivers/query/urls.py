from indexd.index.drivers.alchemy import IndexRecord, IndexRecordUrl, IndexRecordUrlMetadata
from indexd.index.drivers.query import URLsQueryDriver


class AlchemyURLsQueryDriver(URLsQueryDriver):

    def __init__(self, alchemy_driver):
        """
        Args:
            alchemy_driver (indexd.index.drivers.alchemy.SQLAlchemyIndexDriver):
        """
        self.driver = alchemy_driver

    def query_urls(self, exclude=None, include=None, only_versioned=True, offset=0, limit=1000):

        with self.driver.session as sxn:
            sql = "SELECT u.did, array_agg(u.url) as urls FROM index_record_url u, index_record r " \
                  "WHERE u.did = r.did "

        include = include or ""

        if only_versioned:
            sql = "{} AND r.version IS NOT NULL".format(sql)
        sql = "{} GROUP BY u.did HAVING string_agg(u.url, ',') LIKE '%{}%'".format(sql, include)
        if exclude:
            sql = "{} AND string_agg(u.url, ',') NOT LIKE '%{}%'".format(sql, exclude)
        sql = "{} offset {} limit {}".format(sql, offset, limit)

        with self.driver.session as sxn:
            records = sxn.execute(sql)
        # [('did', 'urls')]
        return records

    def query_metadata_by_key(self, key, value, url=None, only_versioned=True, offset=0, limit=1000):

        with self.driver.session as sxn:
            query = sxn.query(IndexRecordUrlMetadata.did,
                              IndexRecordUrlMetadata.url,
                              IndexRecord.rev)\
                .filter(IndexRecord.did == IndexRecordUrlMetadata.did) \
                .filter(IndexRecordUrlMetadata.key == key) \
                .filter(IndexRecordUrlMetadata.value == value)
            if only_versioned:
                query = query.filter(IndexRecord.version.isnot(None))
            if url:
                query = query.filter(IndexRecordUrlMetadata.url.like("%{}%".format(url)))

            # [('did', 'url', 'rev')]
            records = query.order_by(IndexRecordUrlMetadata.did.asc()).offset(offset).limit(limit).all()
        return records
