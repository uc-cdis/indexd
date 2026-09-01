import datetime
import uuid
import json
from contextlib import asynccontextmanager
from cdislogging import get_logger
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    and_,
    func,
    or_,
    select,
    delete,
    text,
    cast,
    not_,
)
from sqlalchemy.dialects.postgresql import JSONPATH
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload, relationship
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.pool import NullPool

from indexd import auth
from indexd.errors import UserError
from indexd.index.driver import IndexDriverABC
from indexd.auth.errors import AuthError
from indexd.index.errors import (
    MultipleRecordsFound,
    NoRecordFound,
    RevisionMismatch,
    UnhealthyCheck,
)
from indexd.utils import migrate_database

Base = declarative_base()


class BaseVersion(Base):
    """
    Base index record version representation.
    """

    __tablename__ = "base_version"

    baseid = Column(String, primary_key=True)
    dids = relationship(
        "IndexRecord",
        backref="base_version",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class IndexSchemaVersion(Base):
    """
    Table to track current database's schema version
    """

    __tablename__ = "index_schema_version"
    version = Column(Integer, default=0, primary_key=True)


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

    urls = relationship(
        "IndexRecordUrl",
        backref="index_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    acl = relationship(
        "IndexRecordACE",
        backref="index_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    authz = relationship(
        "IndexRecordAuthz",
        backref="index_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    hashes = relationship(
        "IndexRecordHash",
        backref="index_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    index_metadata = relationship(
        "IndexRecordMetadata",
        backref="index_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    aliases = relationship(
        "IndexRecordAlias",
        backref="index_record",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def to_document_dict(self):
        """
        Get the full index document
        """
        urls = [u.url for u in self.urls]
        acl = [u.ace for u in self.acl]
        authz = [u.resource for u in self.authz]
        hashes = {h.hash_type: h.hash_value for h in self.hashes}
        metadata = {m.key: m.value for m in self.index_metadata}

        urls_metadata = {
            u.url: {m.key: m.value for m in u.url_metadata} for u in self.urls
        }
        created_date = self.created_date.isoformat()
        updated_date = self.updated_date.isoformat()
        content_created_date = (
            self.content_created_date.isoformat()
            if self.content_created_date is not None
            else None
        )
        content_updated_date = (
            self.content_updated_date.isoformat()
            if self.content_created_date is not None
            else None
        )

        return {
            "did": self.did,
            "baseid": self.baseid,
            "rev": self.rev,
            "size": self.size,
            "file_name": self.file_name,
            "version": self.version,
            "uploader": self.uploader,
            "urls": urls,
            "urls_metadata": urls_metadata,
            "acl": acl,
            "authz": authz,
            "hashes": hashes,
            "metadata": metadata,
            "form": self.form,
            "created_date": created_date,
            "updated_date": updated_date,
            "description": self.description,
            "content_created_date": content_created_date,
            "content_updated_date": content_updated_date,
        }


class IndexRecordAlias(Base):
    __tablename__ = "index_record_alias"
    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    name = Column(String, primary_key=True, unique=True)
    __table_args__ = (
        Index("index_record_alias_idx", "did"),
        Index("index_record_alias_name", "name"),
    )


class IndexRecordUrl(Base):
    __tablename__ = "index_record_url"
    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    url = Column(String, primary_key=True)
    url_metadata = relationship(
        "IndexRecordUrlMetadata",
        backref="index_record_url",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    __table_args__ = (Index("index_record_url_idx", "did"),)


class IndexRecordACE(Base):
    __tablename__ = "index_record_ace"
    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    ace = Column(String, primary_key=True)
    __table_args__ = (Index("index_record_ace_idx", "did"),)


class IndexRecordAuthz(Base):
    __tablename__ = "index_record_authz"
    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    resource = Column(String, primary_key=True)
    __table_args__ = (Index("index_record_authz_idx", "did"),)


class IndexRecordMetadata(Base):
    __tablename__ = "index_record_metadata"
    key = Column(String, primary_key=True)
    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    value = Column(String)
    __table_args__ = (Index("index_record_metadata_idx", "did"),)


class IndexRecordUrlMetadata(Base):
    __tablename__ = "index_record_url_metadata"
    key = Column(String, primary_key=True)
    url = Column(String, primary_key=True)
    did = Column(String, index=True, primary_key=True)
    value = Column(String)
    __table_args__ = (
        ForeignKeyConstraint(
            ["did", "url"], ["index_record_url.did", "index_record_url.url"]
        ),
        Index("index_record_url_metadata_idx", "did"),
    )


class IndexRecordHash(Base):
    __tablename__ = "index_record_hash"
    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    hash_type = Column(String, primary_key=True)
    hash_value = Column(String)
    __table_args__ = (
        Index("index_record_hash_idx", "did"),
        Index("index_record_hash_type_value_idx", "hash_value", "hash_type"),
    )


class DrsBundleRecord(Base):
    __tablename__ = "drs_bundle_record"
    bundle_id = Column(String, primary_key=True)
    name = Column(String)
    created_time = Column(DateTime, default=datetime.datetime.utcnow)
    updated_time = Column(DateTime, default=datetime.datetime.utcnow)
    checksum = Column(String)
    size = Column(BigInteger)
    bundle_data = Column(Text)
    description = Column(Text)
    version = Column(String)
    aliases = Column(String)

    def to_document_dict(self, expand=False):
        ret = {
            "id": self.bundle_id,
            "name": self.name,
            "created_time": self.created_time.isoformat(),
            "updated_time": self.updated_time.isoformat(),
            "checksum": self.checksum,
            "size": self.size,
            "form": "bundle",
            "version": self.version,
            "description": self.description,
            "aliases": self.aliases,
        }
        if expand:
            bundle_data = json.loads(self.bundle_data)
            ret["bundle_data"] = bundle_data
        return ret


class StatsRecord(Base):
    """
    Stats table row representation.
    """

    __tablename__ = "stats"
    total_record_count = Column(BigInteger)
    total_record_bytes = Column(BigInteger)
    month = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)


def create_urls_metadata(urls_metadata, record, session):
    """
    create url metadata record in database
    """
    urls = {u.url for u in record.urls}
    for url, url_metadata in urls_metadata.items():
        if url not in urls:
            raise UserError("url {} in urls_metadata does not exist".format(url))
        for k, v in url_metadata.items():
            str_v = str(v) if v is not None else None
            session.add(
                IndexRecordUrlMetadata(url=url, key=k, value=str_v, did=record.did)
            )


async def get_record_if_exists(did, session):
    query = select(IndexRecord).filter(IndexRecord.did == did)
    result = await session.execute(query)
    return result.scalar_one_or_none()


async def update_stats(session, additional_records, additional_bytes):
    if additional_bytes is None:
        additional_bytes = 0
    now = datetime.datetime.now()

    query = (
        select(StatsRecord)
        .filter(
            or_(
                and_(
                    StatsRecord.month <= now.month,
                    StatsRecord.year == now.year,
                ),
                StatsRecord.year < now.year,
            )
        )
        .order_by(StatsRecord.year.desc(), StatsRecord.month.desc())
        .with_for_update()
    )

    result = await session.execute(query)
    record = result.scalar_one_or_none()

    if record and record.month == now.month and record.year == now.year:
        record.total_record_count += additional_records
        record.total_record_bytes += additional_bytes
    else:
        new_record = StatsRecord()
        new_record.month = now.month
        new_record.year = now.year
        new_record.total_record_bytes = additional_bytes
        new_record.total_record_count = additional_records

        if record:
            new_record.total_record_bytes += record.total_record_bytes
            new_record.total_record_count += record.total_record_count

        session.add(new_record)


async def get_stats(session, month=None, year=None):
    now = datetime.datetime.now()
    if month is None and year is None:
        month = now.month
        year = now.year

    query = (
        select(StatsRecord)
        .filter(
            or_(
                and_(
                    StatsRecord.month <= int(month),
                    StatsRecord.year == int(year),
                ),
                StatsRecord.year < int(year),
            )
        )
        .order_by(StatsRecord.year.desc(), StatsRecord.month.desc())
    )
    result = await session.execute(query)
    stats = result.scalars().first()

    if stats is None:
        return (0, 0)
    return (stats.total_record_count, stats.total_record_bytes)


class SQLAlchemyIndexDriver(IndexDriverABC):
    def __init__(self, conn, logger=None, index_config=None, **config):
        super().__init__(conn, **config)
        self.logger = logger or get_logger("SQLAlchemyIndexDriver")
        self.config = index_config or {}

        # Async Engine Initialization
        self.engine = create_async_engine(conn, poolclass=NullPool)
        Base.metadata.bind = self.engine

        # Async Session Factory
        self.Session = async_sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def migrate_index_database(self):
        # NOTE: If migrate_database performs sync operations, it needs async refactoring too.
        await migrate_database(
            driver=self,
            migrate_functions=SCHEMA_MIGRATION_FUNCTIONS,
            current_schema_version=CURRENT_SCHEMA_VERSION,
            model=IndexSchemaVersion,
        )

    @property
    @asynccontextmanager
    async def session(self):
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
        async with self.session as session:
            query = select(IndexRecord)

            # Joinedload logic replicated with selectinload in async
            query = query.options(
                selectinload(IndexRecord.urls).selectinload(
                    IndexRecordUrl.url_metadata
                ),
                selectinload(IndexRecord.acl),
                selectinload(IndexRecord.authz),
                selectinload(IndexRecord.hashes),
                selectinload(IndexRecord.index_metadata),
                selectinload(IndexRecord.aliases),
            )

            if start is not None:
                query = query.filter(IndexRecord.did > start)
            if size is not None:
                query = query.filter(IndexRecord.size == size)
            if file_name is not None:
                query = query.filter(IndexRecord.file_name == file_name)
            if version is not None:
                query = query.filter(IndexRecord.version == version)
            if uploader is not None:
                query = query.filter(IndexRecord.uploader == uploader)

            if urls:
                for u in urls:
                    sub = select(IndexRecordUrl.did).filter(IndexRecordUrl.url == u)
                    query = query.filter(IndexRecord.did.in_(sub))

            if acl:
                for u in acl:
                    sub = select(IndexRecordACE.did).filter(IndexRecordACE.ace == u)
                    query = query.filter(IndexRecord.did.in_(sub))
            elif acl == []:
                query = query.filter(IndexRecord.acl == None)

            if authz:
                for u in authz:
                    sub = select(IndexRecordAuthz.did).filter(
                        IndexRecordAuthz.resource == u
                    )
                    query = query.filter(IndexRecord.did.in_(sub))
            elif authz == []:
                query = query.filter(IndexRecord.authz == None)

            if hashes:
                for h, v in hashes.items():
                    sub = select(IndexRecordHash.did).filter(
                        and_(
                            IndexRecordHash.hash_type == h,
                            IndexRecordHash.hash_value == v,
                        )
                    )
                    query = query.filter(IndexRecord.did.in_(sub))

            if metadata:
                for k, v in metadata.items():
                    sub = select(IndexRecordMetadata.did).filter(
                        and_(
                            IndexRecordMetadata.key == k, IndexRecordMetadata.value == v
                        )
                    )
                    query = query.filter(IndexRecord.did.in_(sub))

            if urls_metadata:
                query = query.join(IndexRecord.urls).join(IndexRecordUrl.url_metadata)
                for url_key, url_dict in urls_metadata.items():
                    query = query.filter(IndexRecordUrlMetadata.url.contains(url_key))
                    for k, v in url_dict.items():
                        query = query.filter(
                            IndexRecordUrl.url_metadata.any(
                                and_(
                                    IndexRecordUrlMetadata.key == k,
                                    IndexRecordUrlMetadata.value == v,
                                )
                            )
                        )

            if negate_params:
                query = self._negate_filter(session, query, **negate_params)

            if urls_metadata or negate_params:
                query = query.distinct(IndexRecord.did)

            if page is not None:
                query = query.order_by(IndexRecord.updated_date)
            else:
                query = query.order_by(IndexRecord.did)

            if ids:
                DEFAULT_PREFIX = self.config.get("DEFAULT_PREFIX")
                found_ids = []
                new_ids = []

                if not DEFAULT_PREFIX:
                    self.logger.info("NO DEFAULT_PREFIX")
                else:
                    subquery = select(IndexRecord.did).filter(IndexRecord.did.in_(ids))
                    result = await session.execute(subquery)
                    found_ids = list(result.scalars().all())

                    for i in ids:
                        if i not in found_ids:
                            if not i.startswith(DEFAULT_PREFIX):
                                new_ids.append(DEFAULT_PREFIX + i)
                            else:
                                stripped = i.split(DEFAULT_PREFIX, 1)[1]
                                new_ids.append(stripped)

                query = query.filter(IndexRecord.did.in_(found_ids + new_ids))
            else:
                query = query.limit(limit)

            if page is not None:
                query = query.offset(limit * page)

            result = await session.execute(query)
            return [i.to_document_dict() for i in result.scalars().unique()]

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
            query = query.filter(IndexRecord.file_name != file_name)

        if version is not None:
            query = query.filter(IndexRecord.version != version)

        if urls is not None and urls:
            query = query.join(IndexRecord.urls)
            for u in urls:
                query = query.filter(
                    not_(IndexRecord.urls.any(IndexRecordUrl.url == u))
                )

        if acl is not None and acl:
            query = query.join(IndexRecord.acl)
            for u in acl:
                query = query.filter(not_(IndexRecord.acl.any(IndexRecordACE.ace == u)))

        if authz is not None and authz:
            query = query.join(IndexRecord.authz)
            for u in authz:
                query = query.filter(
                    not_(IndexRecord.authz.any(IndexRecordAuthz.resource == u))
                )

        if metadata is not None and metadata:
            for k, v in metadata.items():
                if not v:
                    query = query.filter(
                        not_(
                            IndexRecord.index_metadata.any(IndexRecordMetadata.key == k)
                        )
                    )
                else:
                    sub = select(IndexRecordMetadata.did).filter(
                        and_(
                            IndexRecordMetadata.key == k, IndexRecordMetadata.value == v
                        )
                    )
                    query = query.filter(not_(IndexRecord.did.in_(sub)))

        if urls_metadata is not None and urls_metadata:
            query = query.join(IndexRecord.urls).join(IndexRecordUrl.url_metadata)
            for url_key, url_dict in urls_metadata.items():
                if not url_dict:
                    query = query.filter(
                        not_(IndexRecordUrlMetadata.url.contains(url_key))
                    )
                else:
                    for k, v in url_dict.items():
                        if not v:
                            query = query.filter(
                                not_(
                                    IndexRecordUrl.url_metadata.any(
                                        and_(
                                            IndexRecordUrlMetadata.key == k,
                                            IndexRecordUrlMetadata.url.contains(
                                                url_key
                                            ),
                                        )
                                    )
                                )
                            )
                        else:
                            sub = select(IndexRecordUrlMetadata.did).filter(
                                and_(
                                    IndexRecordUrlMetadata.url.contains(url_key),
                                    IndexRecordUrlMetadata.key == k,
                                    IndexRecordUrlMetadata.value == v,
                                )
                            )
                            query = query.filter(not_(IndexRecord.did.in_(sub)))
        return query

    async def get_urls(self, size=None, hashes=None, ids=None, start=0, limit=100):
        if size is None and hashes is None and ids is None:
            raise UserError("Please provide size/hashes/ids to filter")

        async with self.session as session:
            query = select(IndexRecordUrl).join(IndexRecordUrl.index_record)

            if size:
                query = query.filter(IndexRecord.size == size)
            if hashes:
                for h, v in hashes.items():
                    sub = select(IndexRecordHash.did).filter(
                        and_(
                            IndexRecordHash.hash_type == h,
                            IndexRecordHash.hash_value == v,
                        )
                    )
                    query = query.filter(IndexRecordUrl.did.in_(sub))
            if ids:
                query = query.filter(IndexRecordUrl.did.in_(ids))

            query = query.distinct().offset(start).limit(limit)
            result = await session.execute(query)

            return [
                {"url": r.url, "metadata": {m.key: m.value for m in r.url_metadata}}
                for r in result.scalars().unique()
            ]

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
        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        async with self.session as session:
            record = IndexRecord()
            base_version = BaseVersion()

            if not baseid:
                baseid = str(uuid.uuid4())

            base_version.baseid = baseid
            record.baseid = baseid
            record.file_name = file_name
            record.version = version

            if did:
                record.did = did
            else:
                new_did = str(uuid.uuid4())
                if self.config.get("PREPEND_PREFIX"):
                    new_did = self.config["DEFAULT_PREFIX"] + new_did
                record.did = new_did

            record.rev = str(uuid.uuid4())[:8]
            record.form, record.size = form, size
            record.uploader = uploader

            record.urls = [IndexRecordUrl(did=record.did, url=url) for url in urls]
            record.acl = [IndexRecordACE(did=record.did, ace=ace) for ace in set(acl)]
            record.authz = [
                IndexRecordAuthz(did=record.did, resource=resource)
                for resource in set(authz)
            ]
            record.hashes = [
                IndexRecordHash(did=record.did, hash_type=h, hash_value=v)
                for h, v in hashes.items()
            ]
            record.index_metadata = [
                IndexRecordMetadata(did=record.did, key=m_key, value=m_value)
                for m_key, m_value in metadata.items()
            ]
            record.description = description

            self._validate_and_set_content_dates(
                record=record,
                content_created_date=content_created_date,
                content_updated_date=content_updated_date,
            )

            await session.merge(base_version)

            try:
                session.add(record)
                create_urls_metadata(urls_metadata, record, session)

                if self.config.get("ADD_PREFIX_ALIAS"):
                    self.add_prefix_alias(record, session)
                await update_stats(session, 1, size)
                await session.commit()
            except IntegrityError:
                raise MultipleRecordsFound(
                    'did "{did}" already exists'.format(did=record.did)
                )

            return record.did, record.rev, record.baseid

    async def add_blank_record(self, uploader, file_name=None, authz=None):

        async with self.Session() as session:
            record = IndexRecord()
            base_version = BaseVersion()

            did = str(uuid.uuid4())
            baseid = str(uuid.uuid4())
            if self.config.get("PREPEND_PREFIX"):
                did = self.config["DEFAULT_PREFIX"] + did

            record.did = did
            base_version.baseid = baseid

            record.rev = str(uuid.uuid4())[:8]
            record.baseid = baseid
            record.uploader = uploader
            record.file_name = file_name

            if authz:
                record.authz = [
                    IndexRecordAuthz(did=record.did, resource=resource)
                    for resource in set(authz)
                ]

            session.add(base_version)
            session.add(record)
            await update_stats(session, 1, 0)
            await session.commit()

            return record.did, record.rev, record.baseid

    async def add_blank_bundle(self):
        async with self.session as session:
            record = DrsBundleRecord()
            base_version = BaseVersion()

            bundle_id = str(uuid.uuid4())
            record.bundle_id = bundle_id
            base_version.baseid = bundle_id

            session.add(base_version)
            session.add(record)
            await session.commit()

            return record.bundle_id

    async def update_blank_record(self, auth, did, rev, size, hashes, urls, authz=None):
        hashes = hashes or {}
        urls = urls or []

        async with self.session as session:
            query = select(IndexRecord).filter(IndexRecord.did == did)
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
            record.hashes = [
                IndexRecordHash(did=record.did, hash_type=h, hash_value=v)
                for h, v in hashes.items()
            ]
            record.urls = [IndexRecordUrl(did=record.did, url=url) for url in urls]

            authorized = False
            authz_err_msg = "Auth error when attempting to update a blank record. User must have '{}' access on '{}' for service 'indexd'."
            if authz:
                old_authz = [u.resource for u in record.authz]
                all_authz = old_authz + authz
                try:
                    authorized = await auth.authorize("update", all_authz, False)
                except AuthError as err:
                    self.logger.error(
                        authz_err_msg.format("update", all_authz)
                        + " Falling back to 'file_upload' on '/data_file'."
                    )

                record.authz = [
                    IndexRecordAuthz(did=record.did, resource=resource)
                    for resource in set(authz)
                ]

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

            return record.did, record.rev, record.baseid

    def add_prefix_alias(self, record, session):
        prefix = self.config["DEFAULT_PREFIX"]
        alias = IndexRecordAlias(did=record.did, name=prefix + record.did)
        session.add(alias)

    async def get_by_alias(self, alias):
        async with self.session as session:
            query = select(IndexRecord).filter(IndexRecord.aliases.any(name=alias))
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

            query = select(IndexRecordAlias).filter(IndexRecordAlias.did == did)
            result = await session.execute(query)
            return [i.name for i in result.scalars().all()]

    async def append_aliases_for_did(self, auth, aliases, did):

        async with self.session as session:
            self.logger.info(
                f"Trying to append new aliases {aliases} to aliases for did {did}..."
            )

            index_record = await get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            try:
                resources = [u.resource for u in index_record.authz]
                await auth.authorize("update", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while appending aliases to did {did}: User not authorized to update one or more of these resources: {resources}"
                )
                raise err

            index_record_aliases = [
                IndexRecordAlias(did=did, name=alias) for alias in aliases
            ]
            try:
                session.add_all(index_record_aliases)
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
                resources = [u.resource for u in index_record.authz]
                await auth.authorize("update", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while replacing aliases for did {did}: User not authorized to update one or more of these resources: {resources}"
                )
                raise err

            try:
                delete_query = delete(IndexRecordAlias).filter(
                    IndexRecordAlias.did == did
                )
                await session.execute(
                    delete_query, execution_options={"synchronize_session": "evaluate"}
                )

                index_record_aliases = [
                    IndexRecordAlias(did=did, name=alias) for alias in aliases
                ]
                session.add_all(index_record_aliases)
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
                resources = [u.resource for u in index_record.authz]
                await auth.authorize("delete", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error while deleting all aliases for did {did}: User not authorized to delete one or more of these resources: {resources}"
                )
                raise err

            query = delete(IndexRecordAlias).filter(IndexRecordAlias.did == did)
            await session.execute(
                query, execution_options={"synchronize_session": "evaluate"}
            )

            self.logger.info(f"Deleted all aliases for did {did}.")

    async def delete_one_alias_for_did(self, auth, alias, did):

        async with self.session as session:
            self.logger.info(f"Trying to delete alias {alias} for did {did}...")

            index_record = await get_record_if_exists(did, session)
            if index_record is None:
                self.logger.warning(f"No record found for did {did}")
                raise NoRecordFound(did)

            try:
                resources = [u.resource for u in index_record.authz]
                await auth.authorize("delete", resources)
            except AuthError as err:
                self.logger.warning(
                    f"Auth error deleting alias {alias} for did {did}: User not authorized to delete one or more of these resources: {resources}"
                )
                raise err

            query = delete(IndexRecordAlias).filter(
                IndexRecordAlias.did == did, IndexRecordAlias.name == alias
            )
            result = await session.execute(
                query, execution_options={"synchronize_session": "evaluate"}
            )

            if result.rowcount == 0:
                self.logger.warning(f"No alias {alias} found for did {did}")
                raise NoRecordFound(alias)

            self.logger.info(f"Deleted alias {alias} for did {did}.")

    async def get(self, did, expand=True):
        async with self.session as session:
            query = (
                select(IndexRecord)
                .filter(or_(IndexRecord.did == did, IndexRecord.baseid == did))
                .order_by(IndexRecord.created_date.desc())
            )

            result = await session.execute(query)
            record = result.scalars().first()

            if record is None:
                try:
                    record = await self.get_bundle(bundle_id=did, expand=expand)
                    return record
                except NoRecordFound:
                    raise NoRecordFound("no record found")

            return record.to_document_dict()

    async def get_bulk(self, guid_list, expand=True):
        async with self.session as session:
            query = select(IndexRecord).filter(IndexRecord.did.in_(guid_list))
            result = await session.execute(query)
            return [q.to_document_dict() for q in result.scalars().unique()]

    async def get_with_nonstrict_prefix(self, did, expand=True):
        try:
            record = await self.get(did, expand=expand)
        except NoRecordFound as e:
            DEFAULT_PREFIX = self.config.get("DEFAULT_PREFIX")
            if not DEFAULT_PREFIX:
                raise e

            if not did.startswith(DEFAULT_PREFIX):
                record = await self.get(DEFAULT_PREFIX + did, expand=expand)
            else:
                stripped = did.split(DEFAULT_PREFIX, 1)[1]
                record = await self.get(stripped, expand=expand)

        return record

    async def update(self, auth, did, rev, changing_fields):
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

        async with self.session as session:
            query = select(IndexRecord).filter(IndexRecord.did == did)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if rev != record.rev:
                raise RevisionMismatch("revision mismatch")

            if "urls" in changing_fields:
                for url in record.urls:
                    await session.delete(url)
                record.urls = [
                    IndexRecordUrl(did=record.did, url=url)
                    for url in changing_fields["urls"]
                ]

            if "acl" in changing_fields:
                for ace in record.acl:
                    await session.delete(ace)
                record.acl = [
                    IndexRecordACE(did=record.did, ace=ace)
                    for ace in set(changing_fields["acl"])
                ]

            all_authz = [u.resource for u in record.authz]
            if "authz" in changing_fields:
                new_authz = list(set(changing_fields["authz"]))
                all_authz += new_authz

                for resource in record.authz:
                    await session.delete(resource)

                record.authz = [
                    IndexRecordAuthz(did=record.did, resource=resource)
                    for resource in new_authz
                ]

            try:
                await auth.authorize("update", all_authz)
            except AuthError:
                self.logger.error(authz_err_msg.format("update", all_authz))
                raise

            if "metadata" in changing_fields:
                for md_record in record.index_metadata:
                    await session.delete(md_record)

                record.index_metadata = [
                    IndexRecordMetadata(did=record.did, key=m_key, value=m_value)
                    for m_key, m_value in changing_fields["metadata"].items()
                ]

            if "urls_metadata" in changing_fields:
                for url in record.urls:
                    for url_metadata in url.url_metadata:
                        await session.delete(url_metadata)

                create_urls_metadata(changing_fields["urls_metadata"], record, session)

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
                    setattr(record, key, value)

            record.rev = str(uuid.uuid4())[:8]
            record.updated_date = datetime.datetime.utcnow()

            session.add(record)
            return record.did, record.baseid, record.rev

    async def delete(self, auth, did, rev):

        async with self.session as session:
            query = select(IndexRecord).filter(IndexRecord.did == did)
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
        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        async with self.session as session:
            query = select(IndexRecord).filter_by(did=current_did)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            baseid = record.baseid
            record = IndexRecord()
            did = new_did
            if not did:
                did = str(uuid.uuid4())
                if self.config.get("PREPEND_PREFIX"):
                    did = self.config["DEFAULT_PREFIX"] + did

            record.did = did
            record.baseid = baseid
            record.rev = str(uuid.uuid4())[:8]
            record.form = form
            record.size = size
            record.file_name = file_name
            record.version = version
            record.description = description

            record.urls = [IndexRecordUrl(did=record.did, url=url) for url in urls]
            record.acl = [IndexRecordACE(did=record.did, ace=ace) for ace in set(acl)]
            record.authz = [
                IndexRecordAuthz(did=record.did, resource=resource)
                for resource in set(authz)
            ]
            record.hashes = [
                IndexRecordHash(did=record.did, hash_type=h, hash_value=v)
                for h, v in hashes.items()
            ]
            record.index_metadata = [
                IndexRecordMetadata(did=record.did, key=m_key, value=m_value)
                for m_key, m_value in metadata.items()
            ]

            self._validate_and_set_content_dates(
                record=record,
                content_created_date=content_created_date,
                content_updated_date=content_updated_date,
            )

            try:
                session.add(record)
                create_urls_metadata(urls_metadata, record, session)
                await update_stats(session, 1, record.size)
                await session.commit()
            except IntegrityError:
                raise MultipleRecordsFound("{did} already exists".format(did=did))

            return record.did, record.baseid, record.rev

    async def add_blank_version(
        self,
        auth,
        current_did,
        new_did=None,
        file_name=None,
        uploader=None,
        authz=None,
    ):

        authz_err_msg = "Auth error when attempting to update a record. User must have '{}' access on '{}' for service 'indexd'."
        if authz:
            try:
                await auth.authorize("create", authz)
            except AuthError as err:
                self.logger.error(authz_err_msg.format("create", authz))
                raise

        async with self.session as session:
            query = select(IndexRecord).filter_by(did=current_did)
            result = await session.execute(query)

            try:
                old_record = result.scalar_one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            old_authz = [u.resource for u in old_record.authz]
            try:
                await auth.authorize("update", old_authz)
            except AuthError as err:
                self.logger.error(authz_err_msg.format("update", old_authz))
                raise

            if new_did == old_record.did:
                raise MultipleRecordsFound("{did} already exists".format(did=new_did))

            new_record = IndexRecord()
            did = new_did
            if not did:
                did = str(uuid.uuid4())
                if self.config.get("PREPEND_PREFIX"):
                    did = self.config["DEFAULT_PREFIX"] + did

            new_record.did = did
            new_record.baseid = old_record.baseid
            new_record.rev = str(uuid.uuid4())[:8]
            new_record.file_name = file_name
            new_record.uploader = uploader

            new_record.acl = []
            if not authz:
                authz = old_authz
                old_acl = [u.ace for u in old_record.acl]
                new_record.acl = [
                    IndexRecordACE(did=did, ace=ace) for ace in set(old_acl)
                ]
            new_record.authz = [
                IndexRecordAuthz(did=did, resource=resource) for resource in set(authz)
            ]

            try:
                session.add(new_record)
                await update_stats(session, 1, 0)
                await session.commit()
            except IntegrityError:
                raise MultipleRecordsFound("{did} already exists".format(did=did))

            return new_record.did, new_record.baseid, new_record.rev

    async def get_all_versions(self, did):
        ret = dict()
        async with self.session as session:
            query = select(IndexRecord).filter(IndexRecord.did == did)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
                baseid = record.baseid
            except NoResultFound:
                base_query = select(BaseVersion).filter_by(baseid=did)
                base_result = await session.execute(base_query)
                record = base_result.scalar_one_or_none()
                if not record:
                    raise NoRecordFound("no record found")
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = (
                select(IndexRecord)
                .filter(IndexRecord.baseid == baseid)
                .order_by(IndexRecord.created_date.asc())
            )
            result = await session.execute(query)
            records = result.scalars().unique().all()

            for idx, record in enumerate(records):
                ret[idx] = record.to_document_dict()

        return ret

    async def update_all_versions(self, auth, did, acl=None, authz=None):

        async with self.session as session:
            query = select(IndexRecord).filter_by(did=did)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
                baseid = record.baseid
            except NoResultFound:
                base_query = select(BaseVersion).filter_by(baseid=did)
                base_result = await session.execute(base_query)
                record = base_result.scalar_one_or_none()
                if not record:
                    raise NoRecordFound("no record found")
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = (
                select(IndexRecord)
                .filter(IndexRecord.baseid == baseid)
                .order_by(IndexRecord.created_date.asc())
            )
            result = await session.execute(query)
            records = result.scalars().unique().all()

            all_resources = {r.resource for rec in records for r in rec.authz}
            await auth.authorize("update", list(all_resources))

            ret = []
            for record in records:
                if acl:
                    record.acl = [
                        IndexRecordACE(did=record.did, ace=ace) for ace in set(acl)
                    ]
                if authz:
                    record.authz = [
                        IndexRecordAuthz(did=record.did, resource=resource)
                        for resource in set(authz)
                    ]
                record.rev = str(uuid.uuid4())[:8]
                ret.append(
                    {"did": record.did, "baseid": record.baseid, "rev": record.rev}
                )
            await session.commit()
            return ret

    async def get_latest_version(self, did, has_version=None):
        async with self.session as session:
            query = select(IndexRecord).filter(IndexRecord.did == did)
            result = await session.execute(query)

            try:
                record = result.scalar_one()
                baseid = record.baseid
            except NoResultFound:
                baseid = did
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = (
                select(IndexRecord)
                .filter(IndexRecord.baseid == baseid)
                .order_by(IndexRecord.created_date.desc())
            )
            if has_version:
                query = query.filter(IndexRecord.version.isnot(None))

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
            query = select(IndexRecord).filter(IndexRecord.did == record)
            result = await session.execute(select(query.exists()))
            return result.scalar()

    async def __aiter__(self):
        """
        Async replacement for __iter__. Allows iteration via `async for item in obj`.
        """
        async with self.session as session:
            result = await session.stream_scalars(select(IndexRecord))
            async for i in result:
                yield i.did

    async def totalbytes(self):
        async with self.session as session:
            result = await session.execute(select(func.sum(IndexRecord.size)))
            val = result.scalar()
            if val is None:
                return 0
            return int(val)

    async def len(self):
        async with self.session as session:
            result = await session.execute(
                select(func.count()).select_from(IndexRecord)
            )
            return result.scalar()

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

    async def get_stats(self, month=None, year=None):
        async with self.session as session:
            return await get_stats(session, month, year)


async def migrate_1(session, **kwargs):
    await session.execute(
        text(
            "ALTER TABLE {} ALTER COLUMN size TYPE bigint;".format(
                IndexRecord.__tablename__
            )
        )
    )


async def migrate_2(session, **kwargs):
    try:
        await session.execute(
            text(
                "ALTER TABLE {} \
                ADD COLUMN baseid VARCHAR DEFAULT NULL, \
                ADD COLUMN created_date TIMESTAMP DEFAULT NOW(), \
                ADD COLUMN updated_date TIMESTAMP DEFAULT NOW()".format(
                    IndexRecord.__tablename__
                )
            )
        )
    except ProgrammingError:
        await session.rollback()
    await session.commit()

    count_res = await session.execute(
        text("SELECT COUNT(*) FROM {};".format(IndexRecord.__tablename__))
    )
    count = count_res.fetchone()[0]

    try:
        await session.execute(
            text(
                "CREATE TABLE tmp_index_record AS SELECT did, ROW_NUMBER() OVER (ORDER BY did) AS RowNumber \
            FROM {}".format(
                    IndexRecord.__tablename__
                )
            )
        )
    except ProgrammingError:
        await session.rollback()

    for loop in range(count):
        baseid = str(uuid.uuid4())
        await session.execute(
            text(
                "UPDATE index_record SET baseid = '{}'\
             WHERE did =  (SELECT did FROM tmp_index_record WHERE RowNumber = {});".format(
                    baseid, loop + 1
                )
            )
        )
        await session.execute(
            text(
                "INSERT INTO {}(baseid) VALUES('{}');".format(
                    BaseVersion.__tablename__, baseid
                )
            )
        )

    await session.execute(
        text(
            "ALTER TABLE {} \
         ADD CONSTRAINT baseid_FK FOREIGN KEY (baseid) references base_version(baseid);".format(
                IndexRecord.__tablename__
            )
        )
    )

    await session.execute(text("DROP TABLE IF EXISTS tmp_index_record;"))


async def migrate_3(session, **kwargs):
    await session.execute(
        text(
            "ALTER TABLE {} ADD COLUMN file_name VARCHAR;".format(
                IndexRecord.__tablename__
            )
        )
    )
    await session.execute(
        text(
            "x INDEX {tb}__file_name_idx ON {tb} ( file_name )".format(
                tb=IndexRecord.__tablename__
            )
        )
    )


async def migrate_4(session, **kwargs):
    await session.execute(
        text(
            "ALTER TABLE {} ADD COLUMN version VARCHAR;".format(
                IndexRecord.__tablename__
            )
        )
    )
    await session.execute(
        text(
            "CREATE INDEX {tb}__version_idx ON {tb} ( version )".format(
                tb=IndexRecord.__tablename__
            )
        )
    )


async def migrate_5(session, **kwargs):
    await session.execute(
        text(
            "CREATE INDEX {tb}_idx ON {tb} ( did )".format(
                tb=IndexRecordUrl.__tablename__
            )
        )
    )
    await session.execute(
        text(
            "CREATE INDEX {tb}_idx ON {tb} ( did )".format(
                tb=IndexRecordHash.__tablename__
            )
        )
    )
    await session.execute(
        text(
            "CREATE INDEX {tb}_idx ON {tb} ( did )".format(
                tb=IndexRecordMetadata.__tablename__
            )
        )
    )
    await session.execute(
        text(
            "CREATE INDEX {tb}_idx ON {tb} ( did )".format(
                tb=IndexRecordUrlMetadata.__tablename__
            )
        )
    )


async def migrate_6(session, **kwargs):
    pass


async def migrate_7(session, **kwargs):
    to_delete = []
    query = select(IndexRecordMetadata).filter_by(key="acls")

    result = await session.stream_scalars(query.execution_options(yield_per=1000))

    async for metadata in result:
        acl = metadata.value.split(",")
        for ace in acl:
            entry = IndexRecordACE(did=metadata.did, ace=ace)
            session.add(entry)
        to_delete.append(metadata)

    for metadata in to_delete:
        await session.delete(metadata)


async def migrate_8(session, **kwargs):
    await session.execute(
        text(
            "CREATE INDEX ix_{tb}_baseid ON {tb} ( baseid )".format(
                tb=IndexRecord.__tablename__
            )
        )
    )


async def migrate_9(session, **kwargs):
    await session.execute(
        text(
            "CREATE INDEX ix_{tb}_size ON {tb} ( size )".format(
                tb=IndexRecord.__tablename__
            )
        )
    )
    await session.execute(
        text(
            "CREATE INDEX index_record_hash_type_value_idx ON {tb} ( hash_value, hash_type )".format(
                tb=IndexRecordHash.__tablename__
            )
        )
    )


async def migrate_10(session, **kwargs):
    await session.execute(
        text(
            "ALTER TABLE {} ADD COLUMN uploader VARCHAR;".format(
                IndexRecord.__tablename__
            )
        )
    )
    await session.execute(
        text(
            "CREATE INDEX {tb}__uploader_idx ON {tb} ( uploader )".format(
                tb=IndexRecord.__tablename__
            )
        )
    )


async def migrate_11(session, **kwargs):
    await session.execute(
        text(
            "ALTER TABLE {} ADD COLUMN rbac VARCHAR;".format(IndexRecord.__tablename__)
        )
    )


async def migrate_12(session, **kwargs):
    await session.execute(
        text("ALTER TABLE {} DROP COLUMN rbac;".format(IndexRecord.__tablename__))
    )


async def migrate_13(session, **kwargs):
    await session.execute(
        text(
            "ALTER TABLE {} ADD UNIQUE ( name )".format(IndexRecordAlias.__tablename__)
        )
    )


SCHEMA_MIGRATION_FUNCTIONS = [
    migrate_1,
    migrate_2,
    migrate_3,
    migrate_4,
    migrate_5,
    migrate_6,
    migrate_7,
    migrate_8,
    migrate_9,
    migrate_10,
    migrate_11,
    migrate_12,
    migrate_13,
]
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
