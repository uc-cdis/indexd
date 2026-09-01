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
    delete,
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, JSONPATH
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from contextlib import asynccontextmanager

from indexd import auth
from indexd.errors import UserError
from indexd.auth.errors import AuthError
from indexd.index.driver import IndexDriverABC
from indexd.index.drivers.alchemy import (
    IndexSchemaVersion,
    DrsBundleRecord,
    StatsRecord,
    get_stats,
    update_stats,
)
from indexd.index.errors import (
    MultipleRecordsFound,
    NoRecordFound,
    RevisionMismatch,
    UnhealthyCheck,
)

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
    file_name = Column(String)
    version = Column(String)
    uploader = Column(String)
    description = Column(String)
    content_created_date = Column(DateTime)
    content_updated_date = Column(DateTime)
    hashes = Column(JSONB, index=True)
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
        acl = self.acl or []
        authz = self.authz or []
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
        urls_metadata = generate_url_metadata(self.url_metadata, self.urls)

        return {
            "did": self.guid,
            "baseid": self.baseid,
            "rev": self.rev,
            "size": self.size,
            "file_name": self.file_name,
            "version": self.version,
            "uploader": self.uploader,
            "urls": self.urls,
            "urls_metadata": urls_metadata,
            "acl": acl,
            "authz": authz,
            "hashes": self.hashes,
            "metadata": self.record_metadata,
            "form": self.form,
            "created_date": self.created_date.isoformat(),
            "updated_date": self.updated_date.isoformat(),
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

        self.Session = async_sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False
        )

    @property
    @asynccontextmanager
    async def session(self):
        """
        Provide a transactional scope around a series of operations.
        """
        async with self.Session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def ids(
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
        """
        Returns list of records stored by the backend.
        """
        async with self.session as session:
            query = select(Record)

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
                    matches = ""
                    for k, v in url_dict.items():
                        matches += '@.{} == "{}" && '.format(k, v)
                    if matches:
                        matches = matches.rstrip("&& ")
                        match_string = "$.* ? ({})".format(matches)
                        query = query.filter(
                            func.jsonb_path_exists(
                                Record.url_metadata, cast(match_string, JSONPATH)
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
                    result = await session.execute(subquery)
                    found_ids = [i.guid for i in result.scalars().all()]

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

            result = await session.execute(query)
            return [i.to_document_dict() for i in result.scalars().all()]

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
                    query = query.filter(text(f"NOT (record_metadata ? :key)")).params(
                        key=k
                    )
                else:
                    query = query.filter(Record.record_metadata[k].astext != v)

        if urls_metadata is not None and urls_metadata:
            for url_key, url_dict in urls_metadata.items():
                if not url_dict:
                    query = query.filter(
                        text(
                            f"NOT EXISTS (SELECT 1 FROM UNNEST(urls) AS element WHERE element LIKE '%{url_key}%')"
                        )
                    )
                    query = query.filter(
                        text(
                            f"NOT EXISTS (SELECT 1 FROM jsonb_object_keys(url_metadata) AS key WHERE key LIKE '%{url_key}%')"
                        )
                    )
                else:
                    for k, v in url_dict.items():
                        if not v:
                            query = query.filter(
                                text(
                                    f"EXISTS (SELECT 1 FROM jsonb_each_text(url_metadata) AS x WHERE x.value LIKE '%{k}%')"
                                )
                            )
                        else:
                            query = query.filter(
                                text(
                                    "url_metadata IS NOT NULL AND url_metadata != '{}'"
                                ),
                                not_(
                                    func.jsonb_path_match(
                                        Record.url_metadata,
                                        cast('$.*.{} == "{}"'.format(k, v), JSONPATH),
                                    )
                                ),
                            )

        return query

    async def get_urls(self, size=None, hashes=None, ids=None, start=0, limit=100):
        if size is None and hashes is None and ids is None:
            raise UserError("Please provide size/hashes/ids to filter")

        async with self.session as session:
            query = select(Record)

            if size:
                query = query.filter(Record.size == size)
            if hashes:
                for h, v in hashes.items():
                    query = query.filter(Record.hashes.contains({h: v}))
            if ids:
                query = query.filter(Record.guid.in_(ids))

            query = query.distinct()
            query = query.offset(start)
            query = query.limit(limit)

            result = await session.execute(query)
            return_urls = []

            for r in result.scalars().all():
                if r.url_metadata:
                    for url, values in r.url_metadata.items():
                        return_urls.append(
                            {
                                "url": url,
                                "metadata": values,
                            }
                        )

            return return_urls

    def _validate_and_set_content_dates(
        self, record, content_created_date, content_updated_date
    ):
        if content_created_date is not None:
            record.content_created_date = datetime.datetime.fromisoformat(
                content_created_date
            )
            record.content_updated_date = (
                datetime.datetime.fromisoformat(content_updated_date)
                if content_updated_date is not None
                else record.content_created_date
            )

    async def add(
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
        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        url_metadata = urls_metadata or {}

        async with self.session as session:
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

            self._validate_and_set_content_dates(
                record=record,
                content_created_date=content_created_date,
                content_updated_date=content_updated_date,
            )
            try:
                record.url_metadata = url_metadata
                if self.config.get("ADD_PREFIX_ALIAS"):
                    prefix = self.config["DEFAULT_PREFIX"]
                    record.alias = list(set([prefix + record.guid]))
                session.add(record)
                await update_stats(session, 1, size)
                await session.commit()
            except IntegrityError:
                raise MultipleRecordsFound(
                    'guid "{guid}" already exists'.format(guid=record.guid)
                )

            return record.guid, record.rev, record.baseid

    async def add_blank_record(self, uploader, file_name=None, authz=None):
        async with self.Session() as session:
            record = Record()

            did = str(uuid.uuid4())
            baseid = str(uuid.uuid4())
            if self.config.get("PREPEND_PREFIX"):
                did = self.config["DEFAULT_PREFIX"] + did

            record.guid = did
            record.baseid = baseid
            record.rev = str(uuid.uuid4())[:8]
            record.uploader = uploader
            record.file_name = file_name
            record.authz = authz

            session.add(record)
            await update_stats(session, 1, 0)
            await session.commit()

            return record.guid, record.rev, record.baseid

    async def update_blank_record(self, auth, did, rev, size, hashes, urls, authz=None):
        hashes = hashes or {}
        urls = urls or []

        async with self.session as session:
            query = select(Record).filter(Record.guid == did)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
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
                old_authz = record.authz if record.authz else []
                all_authz = old_authz + authz
                try:
                    authorized = await auth.authorize("update", all_authz, False)
                except AuthError as err:
                    self.logger.error(
                        authz_err_msg.format("update", all_authz)
                        + " Falling back to 'file_upload' on '/data_file'."
                    )
                record.authz = set(authz)

            if not authorized:
                try:
                    await auth.authorize("file_upload", ["/data_file"])
                except AuthError as err:
                    self.logger.error(authz_err_msg.format("file_upload", "/data_file"))
                    raise

            record.rev = str(uuid.uuid4())[:8]
            record.updated_date = datetime.datetime.utcnow()

            session.add(record)
            await update_stats(session, 0, size)
            await session.commit()

            return record.guid, record.rev, record.baseid

    async def get_by_alias(self, alias):
        async with self.session as session:
            query = select(Record).filter(Record.alias.any(alias))
            result = await session.execute(query)
            try:
                record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")
            return record.to_document_dict()

    async def get_aliases_for_did(self, did):
        async with self.session as session:
            self.logger.info(f"Trying to get all aliases for did {did}...")

            index_record = await get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            query = select(Record).filter(Record.guid == did)
            result = await session.execute(query)
            records = result.scalars().all()
            return [i.alias for i in records]

    async def append_aliases_for_did(self, auth, aliases, did):

        async with self.Session() as session:
            self.logger.info(
                f"Trying to append new aliases {aliases} to aliases for did {did}..."
            )

            index_record = await get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            resources = index_record.authz
            await auth.authorize("update", resources, throw=True)

            query = select(Record).filter(Record.guid == did)
            result = await session.execute(query)
            record = result.scalar_one()

            try:
                if record.alias:
                    record.alias = record.alias + aliases
                else:
                    record.alias = aliases
                await session.commit()
            except IntegrityError as err:
                self.logger.warning(
                    f"One or more aliases in request already associated with this or another GUID: {aliases}",
                    exc_info=True,
                )
                raise MultipleRecordsFound(
                    f"One or more aliases in request already associated with this or another GUID: {aliases}"
                )

    async def replace_aliases_for_did(self, auth, aliases, did):

        async with self.session as session:
            self.logger.info(
                f"Trying to replace aliases for did {did} with new aliases {aliases}..."
            )

            index_record = await get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            try:
                resources = index_record.authz
                await auth.authorize("update", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while replacing aliases for did {did}: User not authorized to update one or more of these resources: {resources}"
                )
                raise err

            try:
                query = select(Record).filter(Record.guid == did)
                result = await session.execute(query)
                record = result.scalar_one()

                record.alias = aliases
                await session.commit()
                self.logger.info(
                    f"Replaced aliases for did {did} with new aliases {aliases}"
                )
            except IntegrityError:
                self.logger.warning(
                    f"One or more aliases in request already associated with another GUID: {aliases}"
                )
                raise MultipleRecordsFound(
                    f"One or more aliases in request already associated with another GUID: {aliases}"
                )

    async def delete_all_aliases_for_did(self, auth, did):

        async with self.session as session:
            self.logger.info(f"Trying to delete all aliases for did {did}...")

            index_record = await get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            try:
                resources = index_record.authz
                await auth.authorize("delete", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while deleting all aliases for did {did}: User not authorized to delete one or more of these resources: {resources}"
                )
                raise err

            query = select(Record).filter(Record.guid == did)
            result = await session.execute(query)
            record = result.scalar_one()

            record.alias = []
            await session.commit()

            self.logger.info(f"Deleted all aliases for did {did}.")

    async def delete_one_alias_for_did(self, auth, alias, did):

        async with self.session as session:
            self.logger.info(f"Trying to delete alias {alias} for did {did}...")

            index_record = await get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            try:
                resources = index_record.authz
                await auth.authorize("delete", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error deleting alias {alias} for did {did}: User not authorized to delete one or more of these resources: {resources}"
                )
                raise err

            query = select(Record).filter(Record.guid == did)
            result = await session.execute(query)
            record = result.scalar_one()

            if record.alias and alias in record.alias:
                record.alias = [a for a in record.alias if a != alias]
                await session.commit()
            else:
                self.logger.warning(f"No alias {alias} found for did {did}")
                raise NoRecordFound(alias)

            self.logger.info(f"Deleted alias {alias} for did {did}.")

    async def get(self, guid, expand=True):
        async with self.session as session:
            query = (
                select(Record)
                .filter(or_(Record.guid == guid, Record.baseid == guid))
                .order_by(Record.created_date.desc())
            )

            result = await session.execute(query)
            record = result.scalars().first()

            if record is None:
                try:
                    record = await self.get_bundle(bundle_id=guid, expand=expand)
                    return record
                except NoRecordFound:
                    raise NoRecordFound("no record found")

            return record.to_document_dict()

    async def get_bulk(self, guid_list, expand=True):
        async with self.session as session:
            query = select(Record).filter(Record.guid.in_(guid_list))
            result = await session.execute(query)
            return [q.to_document_dict() for q in result.scalars().all()]

    async def get_with_nonstrict_prefix(self, guid, expand=True):
        try:
            record = await self.get(guid, expand=expand)
        except NoRecordFound as e:
            DEFAULT_PREFIX = self.config.get("DEFAULT_PREFIX")
            if not DEFAULT_PREFIX:
                raise e

            if not guid.startswith(DEFAULT_PREFIX):
                record = await self.get(DEFAULT_PREFIX + guid, expand=expand)
            else:
                stripped = guid.split(DEFAULT_PREFIX, 1)[1]
                record = await self.get(stripped, expand=expand)

        return record

    async def update(self, auth, did, rev, changing_fields):
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

        async with self.session as session:
            query = select(Record).filter(Record.guid == did)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no Record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if rev != record.rev:
                raise RevisionMismatch("Revision mismatch")

            if "urls" in changing_fields:
                record.urls = list(set(changing_fields["urls"]))

            if "acl" in changing_fields:
                record.acl = list(set(changing_fields["acl"]))

            all_authz = list(set(record.authz)) if record.authz else []
            if "authz" in changing_fields:
                new_authz = list(set(changing_fields["authz"]))
                all_authz += new_authz
                record.authz = new_authz

            try:
                await auth.authorize("update", all_authz)
            except AuthError:
                self.logger.error(authz_err_msg.format("update", all_authz))
                raise

            if "metadata" in changing_fields:
                record.record_metadata = changing_fields["metadata"]

            if "urls_metadata" in changing_fields:
                check_url_metadata(changing_fields["urls_metadata"], record)
                record.url_metadata = changing_fields["urls_metadata"]

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
                    setattr(record, key, value)

            record.rev = str(uuid.uuid4())[:8]
            record.updated_date = datetime.datetime.utcnow()

            session.add(record)

            return record.guid, record.baseid, record.rev

    async def delete(self, auth, guid, rev):
        async with self.session as session:
            query = select(Record).filter(Record.guid == guid)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if rev != record.rev:
                raise RevisionMismatch("revision mismatch")

            await auth.authorize("delete", [u.resource for u in record.authz])

            size = record.size if record.size is not None else 0
            await update_stats(session, -1, -1 * size)

            await session.delete(record)

    async def add_version(
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
        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        async with self.session as session:
            query = select(Record).filter_by(guid=current_guid)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

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
            record.urls = urls
            record.acl = acl
            record.authz = authz
            record.hashes = hashes
            record.record_metadata = metadata

            self._validate_and_set_content_dates(
                record=record,
                content_created_date=content_created_date,
                content_updated_date=content_updated_date,
            )

            check_url_metadata(urls_metadata, record)
            record.url_metadata = urls_metadata

            try:
                session.add(record)
                await update_stats(session, 1, record.size)
                await session.commit()
            except IntegrityError:
                raise MultipleRecordsFound("{guid} already exists".format(guid=guid))

            return record.guid, record.baseid, record.rev

    async def add_blank_version(
        self,
        auth,
        current_guid,
        new_did=None,
        file_name=None,
        uploader=None,
        authz=None,
    ):
        authz_err_msg = "Auth error when attempting to update a record. User must have '{}' access on '{}' for service 'indexd'."
        if authz:
            await auth.authorize("create", authz, throw=True)

        async with self.session as session:
            query = select(Record).filter_by(guid=current_guid)
            result = await session.execute(query)

            try:
                old_record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            old_authz = old_record.authz if old_record.authz else []

            await auth.authorize("update", old_authz, throw=True)

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
            new_record.rev = str(uuid.uuid4())[:8]
            new_record.file_name = file_name
            new_record.uploader = uploader
            new_record.acl = []
            if not authz:
                authz = old_authz
                old_acl = old_record.acl
                new_record.acl = old_acl
            new_record.authz = authz

            try:
                session.add(new_record)
                await update_stats(session, 1, 0)
                await session.commit()
            except IntegrityError:
                raise MultipleRecordsFound("{guid} already exists".format(guid=guid))

            return new_record.guid, new_record.baseid, new_record.rev

    async def get_all_versions(self, guid):
        ret = dict()
        async with self.session as session:
            query = select(Record).filter(Record.guid == guid)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
                baseid = record.baseid
            except NoResultFound:
                base_query = select(Record).filter_by(baseid=guid)
                base_result = await session.execute(base_query)
                record = base_result.scalars().first()
                if not record:
                    raise NoRecordFound("no record found")
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = (
                select(Record)
                .filter(Record.baseid == baseid)
                .order_by(Record.created_date.asc())
            )
            result = await session.execute(query)
            records = result.scalars().all()

            for idx, record in enumerate(records):
                ret[idx] = record.to_document_dict()

        return ret

    async def update_all_versions(self, auth, guid, acl=None, authz=None):

        async with self.session as session:
            query = select(Record).filter(Record.guid == guid)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
                baseid = record.baseid
            except NoResultFound:
                base_query = select(Record).filter_by(baseid=guid)
                base_result = await session.execute(base_query)
                record = base_result.scalars().first()
                if not record:
                    raise NoRecordFound("no record found")
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = (
                select(Record)
                .filter(Record.baseid == baseid)
                .order_by(Record.created_date.asc())
            )
            result = await session.execute(query)
            records = result.scalars().all()

            all_resources = []
            for rec in records:
                all_resources += rec.authz or []
            await auth.authorize("update", list(all_resources))

            ret = []
            for record in records:
                record.acl = list(set(acl)) if acl else None
                record.authz = list(set(authz)) if authz else None

                record.rev = str(uuid.uuid4())[:8]
                ret.append(
                    {"did": record.guid, "baseid": record.baseid, "rev": record.rev}
                )
            await session.commit()
            return ret

    async def get_latest_version(self, guid, has_version=None):
        async with self.session as session:
            query = select(Record).filter(Record.guid == guid)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
                baseid = record.baseid
            except NoResultFound:
                baseid = guid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = (
                select(Record)
                .filter(Record.baseid == baseid)
                .order_by(Record.created_date.desc())
            )

            if has_version:
                query = query.filter(Record.version.isnot(None))

            result = await session.execute(query)
            record = result.scalars().first()
            if not record:
                raise NoRecordFound("no record found")

            return record.to_document_dict()

    async def health_check(self):
        async with self.session as session:
            try:
                await session.execute(text("SELECT 1"))
            except Exception:
                raise UnhealthyCheck()

            return True

    async def has_record(self, record):
        """
        Async replacement for the synchronous __contains__ magic method.
        """
        async with self.session as session:
            query = select(Record).filter(Record.guid == record)
            result = await session.execute(select(query.exists()))
            return result.scalar()

    async def __aiter__(self):
        """
        Async replacement for the synchronous __iter__ magic method.
        """
        async with self.session as session:
            result = await session.stream_scalars(select(Record))
            async for i in result:
                yield i.guid

    async def totalbytes(self):
        async with self.session as session:
            result = await session.execute(select(func.sum(Record.size)))
            val = result.scalar()
            if val is None:
                return 0
            return int(val)

    async def len(self):
        async with self.session as session:
            result = await session.execute(select(func.count()).select_from(Record))
            return result.scalar()

    async def get_stats(self, month=None, year=None):
        async with self.session as session:
            return await get_stats(session, month, year)

    async def add_bundle(
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
        async with self.session as session:
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
                await session.commit()
            except IntegrityError:
                raise MultipleRecordsFound(
                    'bundle id "{bundle_id}" already exists'.format(
                        bundle_id=record.bundle_id
                    )
                )

            return record.bundle_id, record.name, record.bundle_data

    async def get_bundle_list(self, start=None, limit=100, page=None):
        async with self.session as session:
            query = select(DrsBundleRecord).limit(limit)

            if start is not None:
                query = query.filter(DrsBundleRecord.bundle_id > start)

            if page is not None:
                query = query.offset(limit * page)

            result = await session.execute(query)
            return [i.to_document_dict() for i in result.scalars().all()]

    async def get_bundle(self, bundle_id, expand=False):
        async with self.session as session:
            query = (
                select(DrsBundleRecord)
                .filter(or_(DrsBundleRecord.bundle_id == bundle_id))
                .order_by(DrsBundleRecord.created_time.desc())
            )

            result = await session.execute(query)
            record = result.scalars().first()
            if record is None:
                raise NoRecordFound("No bundle found")

            doc = record.to_document_dict(expand)
            return doc

    async def get_bundle_and_object_list(
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
        limit = int((limit / 2) + 1)
        bundle = await self.get_bundle_list(start=start, limit=limit, page=page)
        objects = await self.ids(
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

    async def delete_bundle(self, bundle_id):
        async with self.session as session:
            query = select(DrsBundleRecord).filter(
                DrsBundleRecord.bundle_id == bundle_id
            )
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("No bundle found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("Multiple bundles found")

            await session.delete(record)

    async def query_urls(
        self,
        exclude=None,
        include=None,
        versioned=None,
        offset=0,
        limit=1000,
        fields="did,urls",
        **kwargs,
    ):
        if kwargs:
            raise UserError(
                "Unexpected query parameter(s) {}".format(list(kwargs.keys()))
            )

        versioned = (
            versioned.lower() in ["true", "t", "yes", "y"] if versioned else None
        )

        async with self.session as session:
            query = select(Record.guid, Record.urls)

            if versioned is True:
                query = query.filter(Record.version.isnot(None))
            elif versioned is False:
                query = query.filter(Record.version.is_(None))

            query = query.group_by(Record.guid)

            if include and exclude:
                query = query.having(
                    and_(
                        not_(func.array_to_string(Record.urls, ",").contains(exclude)),
                        func.array_to_string(Record.urls, ",").contains(include),
                    )
                )
            elif include:
                query = query.having(
                    func.array_to_string(Record.urls, ",").contains(include)
                )
            elif exclude:
                query = query.having(
                    not_(func.array_to_string(Record.urls, ",").contains(exclude))
                )

            query = query.order_by(Record.guid.asc()).offset(offset).limit(limit)
            result = await session.execute(query)
            record_list = result.all()

        return self._format_response(fields, record_list)

    async def query_metadata_by_key(
        self,
        key,
        value,
        url=None,
        versioned=None,
        offset=0,
        limit=1000,
        fields="did,urls,rev",
        **kwargs,
    ):
        if kwargs:
            raise UserError(
                "Unexpected query parameter(s) {}".format(list(kwargs.keys()))
            )

        versioned = (
            versioned.lower() in ["true", "t", "yes", "y"] if versioned else None
        )
        async with self.session as session:
            query = select(Record.guid, Record.urls, Record.rev)

            query = query.filter(
                func.jsonb_path_exists(
                    Record.url_metadata, cast(f'$.* ? (@.{key} == "{value}")', JSONPATH)
                )
            )

            if versioned is True:
                query = query.filter(Record.version.isnot(None))
            elif versioned is False:
                query = query.filter(Record.version.is_(None))

            if url:
                query = query.filter(
                    func.array_to_string(Record.urls, ",").contains(url)
                )

            query = query.order_by(Record.guid.asc()).offset(offset).limit(limit)
            result = await session.execute(query)
            record_list = result.all()

        return self._format_response(fields, record_list)

    @staticmethod
    def _format_response(requested_fields, record_list):
        result = []
        provided_fields_dict = {k: 1 for k in requested_fields.split(",")}
        for record in record_list:
            resp_dict = {}
            if provided_fields_dict.get("did"):
                resp_dict["did"] = record[0]
            if provided_fields_dict.get("urls"):
                resp_dict["urls"] = record[1] if record[1] else []

            if provided_fields_dict.get("rev") and len(record) == 3:
                resp_dict["rev"] = record[2]
            result.append(resp_dict)
        return result


def check_url_metadata(url_metadata, record):
    """
    create url metadata record in database
    """
    urls = {u for u in record.urls}
    for url in url_metadata:
        if url not in urls:
            raise UserError("url {} in url_metadata does not exist".format(url))


def generate_url_metadata(record_url_metadata, urls):
    """
    Genrates url_metadata for an indexd record. Pulls urls information from urls if urls_metadata is empty.
    """
    urls = urls or []
    record_url_metadata = record_url_metadata or {}
    for url in urls:
        if url not in record_url_metadata:
            record_url_metadata[url] = {}
    return record_url_metadata


async def get_record_if_exists(did, session):
    """
    Searches for a record with this did and returns it.
    If no record found, returns None.
    """
    query = select(Record).filter(Record.guid == did)
    result = await session.execute(query)
    return result.scalar_one_or_none()
