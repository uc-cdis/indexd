import datetime
import uuid

from cdislogging import get_logger
from sqlalchemy import Column, String, ForeignKey, BigInteger, DateTime, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from indexd.index.driver import IndexDriverABC
from indexd.index.drivers.alchemy import IndexSchemaVersion
from indexd.utils import migrate_database

Base = declarative_base()


class Record(Base):
    """
    Base index record representation.
    """

    __tablename__ = "record"

    guid = Column(String, primary_key=True)

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
    record_metadata = Column(JSONB)
    url_metadata = Column(JSONB)
    alias = Column(ARRAY(String))


class SingleTableSQLAlchemyIndexDriver(IndexDriverABC):
    def __init__(self, conn, logger=None, index_config=None, **config):
        super().__init__(conn, **config)
        self.logger = logger or get_logger("SQLAlchemyIndexDriver")
        self.config = index_config or {}
        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)

    def migrate_index_database(self):
        """
        This migration logic is DEPRECATED. It is still supported for backwards compatibility,
        but any new migration should be added using Alembic.

        migrate index database to match CURRENT_SCHEMA_VERSION
        """
        migrate_database(
            driver=self,
            migrate_functions=SCHEMA_MIGRATION_FUNCTIONS,
            current_schema_version=CURRENT_SCHEMA_VERSION,
            model=IndexSchemaVersion,
        )

    @property
    @contextmanager
    def session(self):
        """
        Provide a transactional scope around a series of operations.
        """
        session = self.Session()

        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

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
        with self.session as session:
            query = session.query(Record)

            if start is not None:
                query = query.filter(Record.guid > start)

            if size is not None:
                query = query.filter(Record.size == size)

            if file_name is not None:
                query = query.filter(Record.file_name == file_name)

            if version is not None:
                query = query.filter(Record.version == version)

            if uploader is not None:
                query = query.filter(Record.uploader == uploader)

            if urls:
                for u in urls:
                    query = query.filter(Record.urls.contains(u)).all()

            if acl:
                for u in acl:
                    query = query.filter(Record.acl.contains(u).all())
            elif acl == []:
                query = query.filter(Record.acl == None)

            if authz:
                for u in authz:
                    query = query.filter(Record.authz.contains(u)).all()
            elif authz == []:
                query = query.filter(Record.authz == None)

            if hashes:
                for h, v in hashes.items():
                    query = query.filter(Record.hashes.contains({h: v}))

            if metadata:
                for k, v in metadata.items():
                    query = query.filter(Record.metadata.contains({k: v}))

            if urls_metadata:
                for url_key, url_dict in urls_metadata.items():
                    query = query.filter(Record.urls_metadata.contains(url_key))
                    for k, v in url_dict.items():
                        query = query.filter(Record.urls_metadata.any({k: v}))

            if negate_params:
                query = self._negate_filter(session, query, **negate_params)

            if page is not None:
                query = query.order_by(Record.updated_date)
            else:
                query = query.order_by(Record.guid)

            if ids:
                DEFAULT_PREFIX = self.config.get("DEFAULT_PREFIX")
                found_ids = []
                new_ids = []

                if not DEFAULT_PREFIX:
                    self.logger.info("NO DEFAULT_PREFIX")
                else:
                    subquery = query.filter(Record.guid.in_(ids))
                    found_ids = [i.guid for i in subquery]

                    for i in ids:
                        if i not in found_ids:
                            if not i.startswith(DEFAULT_PREFIX):
                                new_ids.append(DEFAULT_PREFIX + i)
                            else:
                                stripped = i.split(DEFAULT_PREFIX, 1)[1]
                                new_ids.append(stripped)

                query = query.filter(Record.guid.in_(found_ids + new_ids))
            else:
                query = query.limit(limit)

            if page is not None:
                query = query.offset(limit * page)

            return [i.to_document_dict() for i in query]

    def get_urls(self, size=None, hashes=None, ids=None, start=0, limit=100):
        """
        Returns a list of urls matching supplied size/hashes/guids.
        """
        if size is None and hashes is None and ids is None:
            raise UserError("Please provide size/hashes/ids to filter")

        with self.session as session:
            query = session.query(Record)

            if size:
                query = query.filter(Record.size == size)
            if hashes:
                for h, v in hashes.items():
                    query = query.filter(Record.hashes.contains({h: v}))
            if ids:
                query = query.filter(Record.guid.in_(ids))
            # Remove duplicates.
            query = query.distinct()

            # Return only specified window.
            query = query.offset(start)
            query = query.limit(limit)

            return [
                {"url": r.urls, "metadata": {m.key: m.value for m in r.url_metadata}}
                for r in query
            ]

    def add(
        self,
        form,
        guid=None,
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
        Creates a new record given size, urls, acl, authz, hashes, metadata,
        urls_metadata file name and version
        if guid is provided, update the new record with the guid otherwise create it
        """

        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        with self.session as session:
            record = Record()

            if not baseid:
                baseid = str(uuid.uuid4())

            record.baseid = baseid
            record.file_name = file_name
            record.version = version

            if guid:
                record.guid = guid
            else:
                new_guid = str(uuid.uuid4())
                if self.config.get("PREPEND_PREFIX"):
                    new_guid = self.config["DEFAULT_PREFIX"] + new_guid
                record.guid = new_guid

            record.rev = str(uuid.uuid4())[:8]

            record.form, record.size = form, size

            record.uploader = uploader

            record.urls = urls

            record.acl = acl

            record.authz = authz

            record.hashes = hashes

            record.metadata = metadata

            record.description = description

            if content_created_date is not None:
                record.content_created_date = datetime.datetime.fromisoformat(
                    content_created_date
                )
                # Users cannot set content_updated_date without a content_created_date
                record.content_updated_date = (
                    datetime.datetime.fromisoformat(content_updated_date)
                    if content_updated_date is not None
                    else record.content_created_date  # Set updated to created if no updated is provided
                )

            try:
                checked_urls_metadata = check_urls_metadata(urls_metadata, record)
                record.url_metadata = checked_urls_metadata

                if self.config.get("Add_PREFIX_ALIAS"):
                    self.add_prefix_alias(record, session)
                session.add(record)
                session.commit()
            except IntegrityError:
                raise MultipleRecordsFound(
                    'guid "{guid}" already exists'.format(guid=record.guid)
                )

            return record.guid, record.rev, record.baseid

    def get(self, guid):
        """
        Gets a record given the record id or baseid.
        If the given id is a baseid, it will return the latest version
        """
        with self.session as session:
            query = session.query(Record)
            query = query.filter(
                or_(Record.guid == guid, Record.baseid == guid)
            ).order_by(Record.created_date.desc())

            record = query.first()
            if record is None:
                try:
                    record = self.get_bundle(bundle_id=guid, expand=expand)
                    return record
                except NoRecordFound:
                    raise NoRecordFound("no record found")

            return record.to_document_dict()

    def get_with_nonstrict_prefix(self, guid, expand=True):
        """
        Attempt to retrieve a record both with and without a prefix.
        Proxies 'get' with provided id.
        If not found but prefix matches default, attempt with prefix stripped.
        If not found and id has no prefix, attempt with default prefix prepended.
        """
        try:
            record = self.get(guid, expand=expand)
        except NoRecordFound as e:
            DEFAULT_PREFIX = self.config.get("DEFAULT_PREFIX")
            if not DEFAULT_PREFIX:
                raise e

            if not guid.startswith(DEFAULT_PREFIX):
                record = self.get(DEFAULT_PREFIX + guid, expand=expand)
            else:
                stripped = guid.split(DEFAULT_PREFIX, 1)[1]
                record = self.get(stripped, expand=expand)

        return record

    def update(self, guid, rev, changing_fields):
        """
        Updates an existing record with new values.
        """
        authz_err_msg = "Auth error when attempting to update a record. User must have '{}' access on '{}' for service 'indexd'."

        composite_fields = [
            "urls",
            "acl",
            "authz",
            "metadata",
            "urls_metadata",
            "content_created_date",
            "content_updated_date",
        ]

        with self.session as session:
            query = session.query(Record).filter(Record.guid == guid)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if rev != record.rev:
                raise RevisionMismatch("Revision mismatch")

            # Some operations are dependant on other operations. For example
            # urls has to be updated before urls_metadata because of schema
            # constraints.
            if "urls" in changing_fields:
                session.delete(record.urls)

                record.urls = Record(guid=record.guid, urls=changing_fields["urls"])

            if "acl" in changing_fields:
                session.delete(record.acl)

                record.acl = Record(guid=record.guid, acl=changing_fields["acl"])

            all_authz = list(set(record.authz))
            if "authz" in changing_fields:
                new_authz = list(set(changing_fields["authz"]))
                all_authz += new_authz

                session.delete(record.authz)

                record.authz = Record(guid=record.guid, authz=new_authz)

            # authorization check: `update` access on old AND new resources
            try:
                auth.authorize("update", all_authz)
            except AuthError:
                self.logger.error(authz_err_msg.format("update", all_authz))
                raise

            if "metadata" in changing_fields:
                session.delete(record.metadata)

                record.metadata = changing_fields["metadata"].items()

            if "urls_metadata" in changing_fields:
                session.delete(record.url_metadata)

                checked_urls_metadata = check_urls_metadata(
                    changing_fields["urls_metadata"], record
                )
                record.url_metadata = checked_urls_metadata

            if changing_fields.get("content_created_date") is not None:
                record.content_created_date = datetime.datetime.fromisoformat(
                    changing_fields["content_created_date"]
                )
            if changing_fields.get("content_updated_date") is not None:
                if record.content_created_date is None:
                    raise UserError(
                        "Cannot set content_updated_date on record that does not have a content_created_date"
                    )
                if record.content_created_date > datetime.datetime.fromisoformat(
                    changing_fields["content_updated_date"]
                ):
                    raise UserError(
                        "Cannot set content_updated_date before the content_created_date"
                    )

                record.content_updated_date = datetime.datetime.fromisoformat(
                    changing_fields["content_updated_date"]
                )

            for key, value in changing_fields.items():
                if key not in composite_fields:
                    # No special logic needed for other updates.
                    # ie file_name, version, etc
                    setattr(record, key, value)

                    record.rev = str(uuid.uuid4())[:8]

            record.updated_date = datetime.datetime.utcnow()

            session.add(record)

            return record.guid, record.baseid, record.rev

    def delete(self, guid, rev):
        """
        Removes record if stored by backend.
        """
        with self.session as session:
            query = session.query(Record)
            query = query.filter(Record.guid == guid)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if rev != record.rev:
                raise RevisionMismatch("revision mismatch")

            auth.authorize("delete", [u.resource for u in record.authz])

            session.delete(record)

    def add_version(
        self,
        current_guid,
        form,
        new_guid=None,
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
        Add a record version given guid
        """
        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        with self.session as session:
            query = session.query(Record).filter_by(guid=current_guid)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            auth.authorize("update", [u.resource for u in record.authz] + authz)

            baseid = record.baseid
            record = IndexRecord()
            guid = new_guid
            if not guid:
                guid = str(uuid.uuid4())
                if self.config.get("PREPEND_PREFIX"):
                    guid = self.config["DEFAULT_PREFIX"] + guid

            record.guid = guid
            record.baseid = baseid
            record.rev = str(uuid.uuid4())[:8]
            record.form = form
            record.size = size
            record.file_name = file_name
            record.version = version
            record.description = description
            record.content_created_date = content_created_date
            record.content_updated_date = content_updated_date
            record.urls = urls
            record.acl = acl
            record.authz = authz
            record.hashes = hashes
            record.metadata = metadata
            record.url_metadata = check_urls_metadata(urls_metadata, record)

            try:
                session.add(record)
                session.commit()
            except IntegrityError:
                raise MultipleRecordsFound("{guid} already exists".format(guid=guid))

            return record.guid, record.baseid, record.rev

    def add_blank_version(
        self, current_guid, new_guid=None, file_name=None, uploader=None, authz=None
    ):
        pass

    def get_all_versions(self, guid):
        pass

    def get_latest_version(self, guid, has_version=None):
        pass

    def health_check(self):
        pass

    def __contains__(self, guid):
        pass

    def __iter__(self):
        pass

    def totalbytes(self):
        pass

    def len(self):
        pass


def check_urls_metadata(urls_metadata, record):
    """
    create url metadata record in database
    """
    urls = {u.url for u in record.urls}
    for url, url_metadata in urls_metadata.items():
        if url not in urls:
            raise UserError("url {} in urls_metadata does not exist".format(url))
    return url_metadata


SCHEMA_MIGRATION_FUNCTIONS = []
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
