import datetime
import uuid

from cdislogging import get_logger
from sqlalchemy import (
    Column,
    String,
    ForeignKey,
    BigInteger,
    DateTime,
    ARRAY,
    func,
    or_,
    text,
    not_,
    and_,
    cast,
    TEXT,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from contextlib import contextmanager

from indexd import auth
from indexd.errors import UserError, AuthError
from indexd.index.driver import IndexDriverABC
from indexd.index.drivers.alchemy import IndexSchemaVersion, DrsBundleRecord
from indexd.index.errors import (
    MultipleRecordsFound,
    NoRecordFound,
    RevisionMismatch,
    UnhealthyCheck,
)
from indexd.utils import migrate_database

Base = declarative_base()


class Record(Base):
    """
    Base index record representation.
    """

    __tablename__ = "record"

    guid = Column(String, primary_key=True)

    baseid = Column(String, index=True)
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

    def to_document_dict(self):
        """
        Get the full index document
        """
        # TODO: some of these fields may not need to be a variable and could directly go to the return object -Binam
        urls = self.urls
        acl = self.acl or []
        authz = self.authz or []
        hashes = self.hashes
        record_metadata = self.record_metadata
        url_metadata = self.url_metadata
        created_date = self.created_date.isoformat()
        updated_date = self.updated_date.isoformat()
        content_created_date = (
            self.content_created_date.isoformat()
            if self.content_created_date is not None
            else None
        )
        content_updated_date = (
            self.content_updated_date.isoformat()
            if self.content_updated_date is not None
            else None
        )

        return {
            "did": self.guid,
            "baseid": self.baseid,
            "rev": self.rev,
            "size": self.size,
            "file_name": self.file_name,
            "version": self.version,
            "uploader": self.uploader,
            "urls": urls,
            "urls_metadata": url_metadata,
            "acl": acl,
            "authz": authz,
            "hashes": hashes,
            "metadata": record_metadata,
            "form": self.form,
            "created_date": created_date,
            "updated_date": updated_date,
            "description": self.description,
            "content_created_date": content_created_date,
            "content_updated_date": content_updated_date,
        }


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
        page=None,
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
                    query = query.filter(Record.urls.any(u))

            if acl:
                for u in acl:
                    query = query.filter(Record.acl.any(u))
            elif acl == []:
                query = query.filter(Record.acl == None)

            if authz:
                for u in authz:
                    query = query.filter(Record.authz.any(u))
            elif authz == []:
                query = query.filter(Record.authz == None)

            if hashes:
                for h, v in hashes.items():
                    query = query.filter(Record.hashes == {h: v})

            if metadata:
                for k, v in metadata.items():
                    query = query.filter(Record.record_metadata[k].astext == v)

            if urls_metadata:
                for url_key, url_dict in urls_metadata.items():
                    for k, v in url_dict.items():
                        query = query.filter(
                            func.jsonb_path_match(
                                Record.url_metadata, '$.*.{} == "{}"'.format(k, v)
                            )
                        )

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

    @staticmethod
    def _negate_filter(
        session,
        query,
        urls=None,
        acl=None,
        authz=None,
        file_name=None,
        version=None,
        metadata=None,
        urls_metadata=None,
    ):
        """
        param_values passed in here will be negated

        for string (version, file_name), filter with value != <value>
        for list (urls, acl), filter with doc that don't HAS <value>
        for dict (metadata, urls_metadata). In each (key,value) pair:
        - if value is None or empty: then filter with key doesn't exist
        - if value is provided, then filter with value != <value> OR key doesn't exist

        Args:
            session: db session
            query: sqlalchemy query
            urls (list): doc.urls don't have any <url> in the urls list
            acl (list): doc.acl don't have any <acl> in the acl list
            authz (list): doc.authz don't have any <resource> in the authz list
            file_name (str): doc.file_name != <file_name>
            version (str): doc.version != <version>
            metadata (dict): see above for dict
            urls_metadata (dict): see above for dict

        Returns:
            Database query
        """
        if file_name is not None:
            query = query.filter(Record.file_name != file_name)

        if version is not None:
            query = query.filter(Record.version != version)

        if urls is not None and urls:
            for u in urls:
                query = query.filter(not_(Record.urls.any(u)))

        if acl is not None and acl:
            for u in acl:
                query = query.filter(
                    Record.acl.isnot(None),
                    func.array_length(Record.acl, 1) > 0,
                    not_(Record.acl.any(u)),
                )

        if authz is not None and authz:
            for u in authz:
                query = query.filter(
                    Record.authz.isnot(None),
                    func.array_length(Record.authz, 1) > 0,
                    not_(Record.authz.any(u)),
                )

        if metadata is not None and metadata:
            for k, v in metadata.items():
                if not v:
                    query = query.filter(~text(f"record_metadata ? :key")).params(key=k)
                else:
                    query = query.filter(Record.record_metadata[k].astext != v)

        if urls_metadata is not None and urls_metadata:
            for url_key, url_dict in urls_metadata.items():
                if not url_dict:
                    query = query.filter(
                        ~text(
                            f"EXISTS (SELECT 1 FROM UNNEST(urls) AS element WHERE element LIKE '%{url_key}%')"
                        )
                    )
                    query = query.filter(
                        ~text(
                            f"EXISTS (SELECT 1 FROM jsonb_object_keys(url_metadata) AS key WHERE key LIKE '%{url_key}%')"
                        )
                    )
                else:
                    for k, v in url_dict.items():
                        if not v:
                            query = session.query(Record).filter(
                                text(
                                    f"EXISTS (SELECT 1 FROM jsonb_each_text(url_metadata) AS x WHERE x.value LIKE '%{k}%')"
                                )
                            )
                        else:
                            query = query.filter(
                                text(
                                    "url_metadata IS NOT NULL AND url_metadata != '{}'"
                                ),
                                ~func.jsonb_path_match(
                                    Record.url_metadata, '$.*.{} == "{}"'.format(k, v)
                                ),
                            )

        return query

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
            return_urls = []
            for r in query:
                for url, values in r.url_metadata.items():
                    return_urls.append(
                        {
                            "url": url,
                            "metadata": values,
                        }
                    )

            return return_urls

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
        url_metadata file name and version
        if guid is provided, update the new record with the guid otherwise create it
        """

        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        url_metadata = urls_metadata or {}

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

            record.urls = list(set(urls))

            record.acl = list(set(acl))

            record.authz = list(set(authz))

            record.hashes = hashes

            record.record_metadata = metadata

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
                checked_url_metadata = check_url_metadata(url_metadata, record)
                record.url_metadata = checked_url_metadata

                if self.config.get("Add_PREFIX_ALIAS"):
                    self.add_prefix_alias(record, session)
                session.add(record)
                session.commit()
            except IntegrityError:
                raise MultipleRecordsFound(
                    'guid "{guid}" already exists'.format(guid=record.guid)
                )

            return record.guid, record.rev, record.baseid

    def add_blank_record(self, uploader, file_name=None, authz=None):
        """
        Create a new blank record with only uploader and optionally
        file_name and authz fields filled
        """
        # if an authz is provided, ensure that user can actually create for that resource
        authorized = False
        authz_err_msg = "Auth error when attempting to update a blank record. User must have '{}' access on '{}' for service 'indexd'."
        if authz:
            try:
                auth.authorize("create", authz)
                authorized = True
            except AuthError as err:
                self.logger.error(
                    authz_err_msg.format("create", authz)
                    + " Falling back to 'file_upload' on '/data_file'."
                )

        if not authorized:
            # either no 'authz' was provided, or user doesn't have
            # the right CRUD access. Fall back on 'file_upload' logic
            try:
                auth.authorize("file_upload", ["/data_file"])
            except AuthError as err:
                self.logger.error(authz_err_msg.format("file_upload", "/data_file"))
                raise

        with self.session as session:
            record = Record()

            did = str(uuid.uuid4())
            baseid = str(uuid.uuid4())
            if self.config.get("PREPEND_PREFIX"):
                did = self.config["DEFAULT_PREFIX"] + did

            record.guid = did
            record.baseid = baseid

            record.rev = str(uuid.uuid4())[:8]
            record.baseid = baseid
            record.uploader = uploader
            record.file_name = file_name

            record.authz = authz

            session.add(record)
            session.commit()

            return record.guid, record.rev, record.baseid

    def update_blank_record(self, did, rev, size, hashes, urls, authz=None):
        """
        Update a blank record with size, hashes, urls, authz and raise
        exception if the record is non-empty or the revision is not matched
        """
        hashes = hashes or {}
        urls = urls or []

        with self.session as session:
            query = session.query(Record).filter(Record.guid == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if record.size or record.hashes:
                raise UserError("update api is not supported for non-empty record!")

            if rev != record.rev:
                raise RevisionMismatch("revision mismatch")

            record.size = size

            record.hashes = hashes

            record.urls = list(set(urls))

            authorized = False
            authz_err_msg = "Auth error when attempting to update a blank record. User must have '{}' access on '{}' for service 'indexd'."

            if authz:
                # if an authz is provided, ensure that user can actually
                # create/update for that resource (old authz and new authz)
                old_authz = [u for u in record.authz] if record.authz else []
                all_authz = old_authz + authz
                try:
                    auth.authorize("update", all_authz)
                    authorized = True
                except AuthError as err:
                    self.logger.error(
                        authz_err_msg.format("update", all_authz)
                        + " Falling back to 'file_uplaod' on '/data_file'."
                    )

                record.authz = set(authz)

            if not authorized:
                # either no 'authz' was provided, or user doesn't have
                # the right CRUD access. Fall back on 'file_upload' logic
                try:
                    auth.authorize("file_upload", ["/data_file"])
                except AuthError as err:
                    self.logger.error(authz_err_msg.format("file_upload", "/data_file"))
                    raise

            record.rev = str(uuid.uuid4())[:8]

            record.updated_data = datetime.datetime.utcnow()

            session.add(record)
            session.commit()

            return record.guid, record.rev, record.baseid

    def add_prefix_alias(self, record, session):
        """
        Create a index alias with the alias as {prefix:did}
        """
        prefix = self.config["DEFAULT_PREFIX"]
        session.add(Record().alias.append(prefix + record.guid))

    def get_by_alias(self, alias):
        """
        Gets a record given a record alias
        """
        with self.session as session:
            try:
                record = session.query(Record).filter(Record.alias.any(alias)).one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")
            return record.to_document_dict

    def get_aliases_for_did(self, did):
        """
        Gets the aliases for a did
        """
        with self.session as session:
            self.logger.info(f"Trying to get all aliases for did {did}...")

            index_record = get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            query = session.query(Record).filter(Record.guid == did)
            return [i.alias for i in query]

    def append_aliases_for_did(self, aliases, did):
        """
        Append one or more aliases to aliases already associated with one DID / GUID.
        """
        with self.session as session:
            self.logger.info(
                f"Trying to append new aliases {aliases} to aliases for did {did}..."
            )

            index_record = get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            # authorization
            try:
                resources = [u.resource for u in index_record.authz]
                auth.authorize("update", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while appending aliases to did {did}: User not authorized to update one or more of these resources: {resources}"
                )
                raise err

            # add new aliases
            query = session.query(Record).filter(Record.guid == did)
            record = query.one()

            try:
                record.alias = record.alias + aliases
                session.commit()
            except IntegrityError as err:
                # One or more aliases in request were non-unique
                self.logger.warning(
                    f"One or more aliases in request already associated with this or another GUID: {aliases}",
                    exc_info=True,
                )
                raise MultipleRecordsFound(
                    f"One or more aliases in request already associated with this or another GUID: {aliases}"
                )

    def replace_aliases_for_did(self, aliases, did):
        """
        Replace all aliases for one DID / GUID with new aliases.
        """
        with self.session as session:
            self.logger.info(
                f"Trying to replace aliases for did {did} with new aliases {aliases}..."
            )

            index_record = get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            # authorization
            try:
                resources = [u.resource for u in index_record.authz]
                auth.authorize("update", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while replacing aliases for did {did}: User not authorized to update one or more of these resources: {resources}"
                )
                raise err

            try:
                query = session.query(Record).filter(Record.guid == did)
                record = query.one()
                # delete this GUID's aliases and add new aliases
                record.alias = aliases
                session.commit()
                self.logger.info(
                    f"Replaced aliases for did {did} with new aliases {aliases}"
                )
            except IntegrityError:
                # One or more aliases in request were non-unique
                self.logger.warning(
                    f"One or more aliases in request already associated with another GUID: {aliases}"
                )
                raise MultipleRecordsFound(
                    f"One or more aliases in request already associated with another GUID: {aliases}"
                )

    def delete_all_aliases_for_did(self, did):
        """
        Delete all of this DID / GUID's aliases.
        """
        with self.session as session:
            self.logger.info(f"Trying to delete all aliases for did {did}...")

            index_record = get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            # authorization
            try:
                resources = [u.resource for u in index_record.authz]
                auth.authorize("delete", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while deleting all aliases for did {did}: User not authorized to delete one or more of these resources: {resources}"
                )
                raise err

            query = session.query(Record).filter(Record.guid == did)
            record = query.one()
            # delete this GUID's aliases and add new aliases
            record.alias = []
            session.commit()

            self.logger.info(f"Deleted all aliases for did {did}.")

    def delete_one_alias_for_did(self, alias, did):
        """
        Delete one of this DID / GUID's aliases.
        """
        with self.session as session:
            self.logger.info(f"Trying to delete alias {alias} for did {did}...")

            index_record = get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            # authorization
            try:
                resources = [u.resource for u in index_record.authz]
                auth.authorize("delete", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error deleting alias {alias} for did {did}: User not authorized to delete one or more of these resources: {resources}"
                )
                raise err

            query = session.query(Record).filter(Record.guid == did)
            record = query.one()
            # delete just this alias
            if alias in record.alias:
                record.alias.remove(alias)
                session.commit()
            else:
                self.logger.warning(f"No alias {alias} found for did {did}")
                raise NoRecordFound(alias)

            self.logger.info(f"Deleted alias {alias} for did {did}.")

    def get(self, guid, expand=True):
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

    def update(self, did, rev, changing_fields):
        """
        Updates an existing record with new values.
        """
        authz_err_msg = "Auth error when attempting to update a record. User must have '{}' access on '{}' for service 'indexd'."

        composite_fields = [
            "urls",
            "acl",
            "authz",
            "record_metadata",
            "url_metadata",
            "content_created_date",
            "content_updated_date",
        ]

        with self.session as session:
            query = session.query(Record).filter(Record.guid == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("no Record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if rev != record.rev:
                raise RevisionMismatch("Revision mismatch")

            # Some operations are dependant on other operations. For example
            # urls has to be updated before url_metadata because of schema
            # constraints.
            if "urls" in changing_fields:
                record.urls = list(set(changing_fields["urls"]))

            if "acl" in changing_fields:
                record.acl = list(set(changing_fields["acl"]))

            all_authz = list(set(record.authz)) if record.authz else []
            if "authz" in changing_fields:
                new_authz = list(set(changing_fields["authz"]))
                all_authz += new_authz
                record.authz = new_authz

            # authorization check: `update` access on old AND new resources
            try:
                auth.authorize("update", all_authz)
            except AuthError:
                self.logger.error(authz_err_msg.format("update", all_authz))
                raise

            if "metadata" in changing_fields:
                record.record_metadata = changing_fields["metadata"]

            if "urls_metadata" in changing_fields:
                checked_url_metadata = check_url_metadata(
                    changing_fields["urls_metadata"], record
                )
                record.url_metadata = checked_url_metadata

            if changing_fields.get("content_created_date") is not None:
                record.content_created_date = datetime.datetime.fromisoformat(
                    changing_fields["content_created_date"]
                )
            if changing_fields.get("content_updated_date") is not None:
                if record.content_created_date is None:
                    raise UserError(
                        "Cannot set content_updated_date on Record that does not have a content_created_date"
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

            auth.authorize("update", [u for u in record.authz] + authz)

            baseid = record.baseid
            record = Record()
            guid = new_did
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
            record.record_metadata = metadata
            record.url_metadata = check_url_metadata(urls_metadata, record)

            try:
                session.add(record)
                session.commit()
            except IntegrityError:
                raise MultipleRecordsFound("{guid} already exists".format(guid=guid))

            return record.guid, record.baseid, record.rev

    def add_blank_version(
        self, current_guid, new_did=None, file_name=None, uploader=None, authz=None
    ):
        """
        Add a blank record version given did.
        If authz is not specified, acl/authz fields carry over from previous version.
        """
        # if an authz is provided, ensure that user can actually create for that resource
        authz_err_msg = "Auth error when attempting to update a record. User must have '{}' access on '{}' for service 'indexd'."
        if authz:
            try:
                auth.authorize("create", authz)
            except AuthError as err:
                self.logger.error(authz_err_msg.format("create", authz))
                raise

        with self.session as session:
            query = session.query(Record).filter_by(guid=current_guid)

            try:
                old_record = query.one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            old_authz = old_record.authz
            try:
                auth.authorize("update", old_authz)
            except AuthError as err:
                self.logger.error(authz_err_msg.format("update", old_authz))
                raise

            # handle the edgecase where new_did matches the original doc's guid to
            # prevent sqlalchemy FlushError
            if new_did == old_record.guid:
                raise MultipleRecordsFound("{guid} already exists".format(guid=new_did))

            new_record = Record()
            guid = new_did
            if not guid:
                guid = str(uuid.uuid4())
                if self.config.get("PEPREND_PREFIX"):
                    guid = self.config["DEFAULT_PREFIX"] + guid

            new_record.guid = guid
            new_record.baseid = old_record.baseid
            new_record.rev = str(uuid.uuid4())
            new_record.file_name = old_record.file_name
            new_record.uploader = old_record.uploader

            new_record.acl = []
            if not authz:
                authz = old_authz
                old_acl = old_record.acl
                new_record.acl = old_acl
            new_record.authz = authz

            try:
                session.add(new_record)
                session.commit()
            except IntegrityError:
                raise MultipleRecordsFound("{guid} already exists".format(guid=guid))

            return new_record.guid, new_record.baseid, new_record.rev

    def get_all_versions(self, guid):
        """
        Get all record versions (in order of creation) given DID
        """
        ret = dict()
        with self.session as session:
            query = session.query(Record)
            query = query.filter(Record.guid == guid)

            try:
                record = query.one()
                baseid = record.baseid
            except NoResultFound:
                record = session.query(Record).filter_by(baseid=guid).first()
                if not record:
                    raise NoRecordFound("no record found")
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            # Find all versions of this record
            query = session.query(Record)
            records = (
                query.filter(Record.baseid == baseid)
                .order_by(Record.created_date.asc())
                .all()
            )

            for idx, record in enumerate(records):
                ret[idx] = record.to_document_dict()

        return ret

    def update_all_versions(self, guid, acl=None, authz=None):
        """
        Update all record versions with new acl and authz
        """
        with self.session as session:
            query = session.query(Record)
            query = query.filter(Record.guid == guid)

            try:
                record = query.one()
                baseid = record.baseid
            except NoResultFound:
                record = session.query(Record).filter_by(baseid=guid).first()
                if not record:
                    raise NoRecordFound("no record found")
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            # Find all versions of this record
            query = session.query(Record)
            records = (
                query.filter(Record.baseid == baseid)
                .order_by(Record.created_date.asc())
                .all()
            )

            # User requires update permissions for all versions of the record
            all_resources = []
            all_resources.append([rec.authz] for rec in records)
            auth.authorize("update", list(all_resources))

            ret = []
            # Update fields for all versions
            for record in records:
                record.acl = set(acl) if acl else None
                record.authz = set(authz) if authz else None

                record.rev = str(uuid.uuid4())[:8]
                ret.append(
                    {"did": record.guid, "baseid": record.baseid, "rev": record.rev}
                )
            session.commit()
            return ret

    def get_latest_version(self, guid, has_version=None):
        """
        Get the lattest record version given did
        """
        with self.session as session:
            query = session.query(Record)
            query = query.filter(Record.guid == guid)

            try:
                record = query.one()
                baseid = record.baseid
            except NoResultFound:
                baseid = guid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = session.query(Record)
            query = query.filter(Record.baseid == baseid).order_by(
                Record.created_date.desc()
            )

            if has_version:
                query = query.filter(Record.version.isnot(None))
            record = query.first()
            if not record:
                raise NoRecordFound("no record found")

            return record.to_document_dict()

    def health_check(self):
        """
        Does a health check of the backend.
        """
        with self.session as session:
            try:
                query = session.execute("SELECT 1")  # pylint: disable=unused-variable
            except Exception:
                raise UnhealthyCheck()

            return True

    def __contains__(self, record):
        """
        Returns True if record is stored by backend.
        Returns False otherwise.
        """
        with self.session as session:
            query = session.query(Record)
            query = query.filter(Record.guid == record)

            return query.exists()

    def __iter__(self):
        """
        Iterator over unique records stored by backend.
        """
        with self.session as session:
            for i in session.query(Record):
                yield i.did

    def totalbytes(self):
        """
        Total number of bytes of data represented in the index.
        """
        with self.session as session:
            result = session.execute(select([func.sum(Record.size)])).scalar()
            if result is None:
                return 0
            return int(result)

    def len(self):
        """
        Number of unique records stored by backend.
        """
        with self.session as session:
            return session.execute(select([func.count()]).select_from(Record)).scalar()

    def add_bundle(
        self,
        bundle_id=None,
        name=None,
        checksum=None,
        size=None,
        bundle_data=None,
        description=None,
        version=None,
        aliases=None,
    ):
        """
        Add a bundle record
        """
        with self.session as session:
            record = DrsBundleRecord()
            if not bundle_id:
                bundle_id = str(uuid.uuid4())
                if self.config.get("PREPEND_PREFIX"):
                    bundle_id = self.config["DEFAULT_PREFIX"] + bundle_id
            if not name:
                name = bundle_id

            record.bundle_id = bundle_id

            record.name = name

            record.checksum = checksum

            record.size = size

            record.bundle_data = bundle_data

            record.description = description

            record.version = version

            record.aliases = aliases

            try:
                session.add(record)
                session.commit()
            except IntegrityError:
                raise MultipleRecordsFound(
                    'bundle id "{bundle_id}" already exists'.format(
                        bundle_id=record.bundle_id
                    )
                )

            return record.bundle_id, record.name, record.bundle_data

    def get_bundle_list(self, start=None, limit=100, page=None):
        """
        Returns list of all bundles
        """
        with self.session as session:
            query = session.query(DrsBundleRecord)
            query = query.limit(limit)

            if start is not None:
                query = query.filter(DrsBundleRecord.bundle_id > start)

            if page is not None:
                query = query.offset(limit * page)

            return [i.to_document_dict() for i in query]

    def get_bundle(self, bundle_id, expand=False):
        """
        Gets a bundle record given the bundle_id.
        """
        with self.session as session:
            query = session.query(DrsBundleRecord)

            query = query.filter(or_(DrsBundleRecord.bundle_id == bundle_id)).order_by(
                DrsBundleRecord.created_time.desc()
            )

            record = query.first()
            if record is None:
                raise NoRecordFound("No bundle found")

            doc = record.to_document_dict(expand)

            return doc

    def get_bundle_and_object_list(
        self,
        limit=100,
        page=None,
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
        Gets bundles and objects and orders them by created time.
        """
        limit = int((limit / 2) + 1)
        bundle = self.get_bundle_list(start=start, limit=limit, page=page)
        objects = self.ids(
            limit=limit,
            page=page,
            start=start,
            size=size,
            urls=urls,
            acl=acl,
            authz=authz,
            hashes=hashes,
            file_name=file_name,
            version=version,
            uploader=uploader,
            metadata=metadata,
            ids=ids,
            urls_metadata=urls_metadata,
            negate_params=negate_params,
        )

        ret = []
        i = 0
        j = 0

        while i + j < len(bundle) + len(objects):
            if i != len(bundle) and (
                j == len(objects)
                or bundle[i]["created_time"] > objects[j]["created_date"]
            ):
                ret.append(bundle[i])
                i += 1
            else:
                ret.append(objects[j])
                j += 1
        return ret

    def delete_bundle(self, bundle_id):
        with self.session as session:
            query = session.query(DrsBundleRecord)
            query = query.filter(DrsBundleRecord.bundle_id == bundle_id)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("No bundle found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("Multiple bundles found")

            session.delete(record)


def check_url_metadata(url_metadata, record):
    """
    create url metadata record in database
    """
    urls = {u for u in record.urls}
    for url, metadata in url_metadata.items():
        if url not in urls:
            raise UserError("url {} in url_metadata does not exist".format(url))
    return url_metadata


def get_record_if_exists(did, session):
    """
    Searches for a record with this did and returns it.
    If no record found, returns None.
    """
    return session.query(Record).filter(Record.guid == did).first()


SCHEMA_MIGRATION_FUNCTIONS = []
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
