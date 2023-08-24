import datetime

from sqlalchemy import Column, String, ForeignKey, BigInteger, DateTime, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

from indexd.index.driver import IndexDriverABC

Base = declarative_base()


class IndexRecord(Base):
    """
    Base index record representation.
    """

    __tablename__ = "index_record"

    did = Column(String, primary_key=True)

    baseid = Column(String, ForeignKey("base_version.baseid"), index=True)
    rev = Column(String)
    form = Column(String)
    size = Column(BigInteger, index=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow)
    file_name = Column(String, index=True)
    version = Column(String, index=True)
    uploader = Column(String, index=True)
    description = Column(String)
    content_created_date = Column(DateTime)
    content_updated_date = Column(DateTime)
    hashes = Column(JSONB)
    acl = Column(ARRAY(String))
    authz = Column(ARRAY(String))
    urls = Column(ARRAY(String))
    metadata = Column(JSONB)
    url_metadata = Column(JSONB)
    alias = Column(ARRAY(String))


class SingleTableSQLAlchemyIndexDriver(IndexDriverABC):
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
        pass

    def get_urls(self, size=None, hashes=None, ids=None, start=0, limit=100):
        pass

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
        pass

    def get(self, did):
        pass

    def update(self, did, rev, changing_fields):
        pass

    def delete(self, did, rev):
        pass

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
        pass

    def get_all_versions(self, did):
        pass

    def get_latest_version(self, did, has_version=None):
        pass

    def health_check(self):
        pass

    def __contains__(self, did):
        pass

    def __iter__(self):
        pass

    def totalbytes(self):
        pass

    def len(self):
        pass
