from indexd.index.drivers.alchemy import IndexRecord, IndexRecordUrlMetadata
from indexd.index.drivers.query import URLsQueryDriver


class AlchemyURLsQueryDriver(URLsQueryDriver):

    def __init__(self, alchemy_driver):
        """
        Args:
            alchemy_driver (indexd.index.drivers.alchemy.SQLAlchemyIndexDriver):
        """
        self.driver = alchemy_driver

    def get_metadata_by_key(self, url, key, value, only_versioned=True, offset=0, limit=1000):

        with self.driver.session as sxn:
            query = sxn.query(IndexRecordUrlMetadata.did,
                              IndexRecordUrlMetadata.url,
                              IndexRecord.rev)\
                .filter(IndexRecord.did == IndexRecordUrlMetadata.did) \
                .filter(IndexRecordUrlMetadata.url.like("%{}%".format(url))) \
                .filter(IndexRecordUrlMetadata.key == key) \
                .filter(IndexRecordUrlMetadata.value == value)

            if only_versioned:
                query.filter(IndexRecord.version.isnot(None))
            # [('did', 'url', 'rev')]
            records = query.order_by(IndexRecordUrlMetadata.did.asc()).limit(limit).offset(offset).all()
        return records
