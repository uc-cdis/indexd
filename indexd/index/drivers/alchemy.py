import datetime
import uuid
from contextlib import contextmanager

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
    and_,
    func,
    or_,
    select,
)
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import joinedload, relationship, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from indexd import auth
from indexd.errors import UserError
from indexd.index.driver import IndexDriverABC
from indexd.index.errors import (
    MultipleRecordsFound,
    NoRecordFound,
    RevisionMismatch,
    UnhealthyCheck,
)
from indexd.utils import init_schema_version, is_empty_database, migrate_database

Base = declarative_base()


class BaseVersion(Base):
    """
    Base index record version representation.
    """

    __tablename__ = "base_version"

    baseid = Column(String, primary_key=True)
    dids = relationship(
        "IndexRecord", backref="base_version", cascade="all, delete-orphan"
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

    urls = relationship(
        "IndexRecordUrl", backref="index_record", cascade="all, delete-orphan"
    )

    acl = relationship(
        "IndexRecordACE", backref="index_record", cascade="all, delete-orphan"
    )

    authz = relationship(
        "IndexRecordAuthz", backref="index_record", cascade="all, delete-orphan"
    )

    hashes = relationship(
        "IndexRecordHash", backref="index_record", cascade="all, delete-orphan"
    )

    index_metadata = relationship(
        "IndexRecordMetadata", backref="index_record", cascade="all, delete-orphan"
    )

    aliases = relationship(
        "IndexRecordAlias", backref="index_record", cascade="all, delete-orphan"
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
        }


class IndexRecordAlias(Base):
    """
    Alias attached to index record
    """

    __tablename__ = "index_record_alias"

    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    name = Column(String, primary_key=True)

    __table_args__ = (
        Index("index_record_alias_idx", "did"),
        Index("index_record_alias_name", "name"),
    )


class IndexRecordUrl(Base):
    """
    Base index record url representation.
    """

    __tablename__ = "index_record_url"

    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    url = Column(String, primary_key=True)

    url_metadata = relationship(
        "IndexRecordUrlMetadata",
        backref="index_record_url",
        cascade="all, delete-orphan",
    )
    __table_args__ = (Index("index_record_url_idx", "did"),)


class IndexRecordACE(Base):
    """
    index record access control entry representation.
    """

    __tablename__ = "index_record_ace"

    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    # access control entry
    ace = Column(String, primary_key=True)

    __table_args__ = (Index("index_record_ace_idx", "did"),)


class IndexRecordAuthz(Base):
    """
    index record access control (authz) entry representation.
    """

    __tablename__ = "index_record_authz"

    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    resource = Column(String, primary_key=True)

    __table_args__ = (Index("index_record_authz_idx", "did"),)


class IndexRecordMetadata(Base):
    """
    Metadata attached to index document
    """

    __tablename__ = "index_record_metadata"
    key = Column(String, primary_key=True)
    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    value = Column(String)
    __table_args__ = (Index("index_record_metadata_idx", "did"),)


class IndexRecordUrlMetadata(Base):
    """
    Metadata attached to url
    """

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
    """
    Base index record hash representation.
    """

    __tablename__ = "index_record_hash"

    did = Column(String, ForeignKey("index_record.did"), primary_key=True)
    hash_type = Column(String, primary_key=True)
    hash_value = Column(String)
    __table_args__ = (
        Index("index_record_hash_idx", "did"),
        Index("index_record_hash_type_value_idx", "hash_value", "hash_type"),
    )


def create_urls_metadata(urls_metadata, record, session):
    """
    create url metadata record in database
    """
    urls = {u.url for u in record.urls}
    for url, url_metadata in urls_metadata.items():
        if url not in urls:
            raise UserError("url {} in urls_metadata does not exist".format(url))
        for k, v in url_metadata.items():
            session.add(IndexRecordUrlMetadata(url=url, key=k, value=v, did=record.did))


class SQLAlchemyIndexDriver(IndexDriverABC):
    """
    SQLAlchemy implementation of index driver.
    """

    def __init__(
        self, conn, logger=None, auto_migrate=True, index_config=None, **config
    ):
        """
        Initialize the SQLAlchemy database driver.
        """
        super().__init__(conn, **config)
        self.logger = logger or get_logger("SQLAlchemyIndexDriver")
        self.config = index_config or {}

        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)

        is_empty_db = is_empty_database(driver=self)
        Base.metadata.create_all()
        if is_empty_db:
            init_schema_version(
                driver=self, model=IndexSchemaVersion, version=CURRENT_SCHEMA_VERSION
            )

        if auto_migrate:
            self.migrate_index_database()

    def migrate_index_database(self):
        """
        migrate alias database to match CURRENT_SCHEMA_VERSION
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
        except:
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
        """
        Returns list of records stored by the backend.
        """
        with self.session as session:
            query = session.query(IndexRecord)

            # Enable joinedload on all relationships so that we won't have to
            # do a bunch of selects when we assemble our response.
            query = query.options(
                joinedload(IndexRecord.urls).joinedload(IndexRecordUrl.url_metadata)
            )
            query = query.options(joinedload(IndexRecord.acl))
            query = query.options(joinedload(IndexRecord.authz))
            query = query.options(joinedload(IndexRecord.hashes))
            query = query.options(joinedload(IndexRecord.index_metadata))
            query = query.options(joinedload(IndexRecord.aliases))

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
                query = query.join(IndexRecord.urls)
                for u in urls:
                    query = query.filter(IndexRecordUrl.url == u)

            if acl:
                query = query.join(IndexRecord.acl)
                for u in acl:
                    query = query.filter(IndexRecordACE.ace == u)
            elif acl == []:
                query = query.filter(IndexRecord.acl == None)

            if authz:
                query = query.join(IndexRecord.authz)
                for u in authz:
                    query = query.filter(IndexRecordAuthz.resource == u)
            elif authz == []:
                query = query.filter(IndexRecord.authz == None)

            if hashes:
                for h, v in hashes.items():
                    sub = session.query(IndexRecordHash.did)
                    sub = sub.filter(
                        and_(
                            IndexRecordHash.hash_type == h,
                            IndexRecordHash.hash_value == v,
                        )
                    )
                    query = query.filter(IndexRecord.did.in_(sub.subquery()))

            if metadata:
                for k, v in metadata.items():
                    sub = session.query(IndexRecordMetadata.did)
                    sub = sub.filter(
                        and_(
                            IndexRecordMetadata.key == k, IndexRecordMetadata.value == v
                        )
                    )
                    query = query.filter(IndexRecord.did.in_(sub.subquery()))

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

            # joining url metadata will have duplicate results
            # url or acl doesn't have duplicate results for current filter
            # so we don't need to select distinct for these cases
            if urls_metadata or negate_params:
                query = query.distinct(IndexRecord.did)

            query = query.order_by(IndexRecord.did)

            if ids:
                query = query.filter(IndexRecord.did.in_(ids))
            else:
                # only apply limit when ids is not provided
                query = query.limit(limit)

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
            query = query.filter(IndexRecord.file_name != file_name)

        if version is not None:
            query = query.filter(IndexRecord.version != version)

        if urls is not None and urls:
            query = query.join(IndexRecord.urls)
            for u in urls:
                query = query.filter(~IndexRecord.urls.any(IndexRecordUrl.url == u))

        if acl is not None and acl:
            query = query.join(IndexRecord.acl)
            for u in acl:
                query = query.filter(~IndexRecord.acl.any(IndexRecordACE.ace == u))

        if authz is not None and authz:
            query = query.join(IndexRecord.authz)
            for u in authz:
                query = query.filter(
                    ~IndexRecord.authz.any(IndexRecordAuthz.resource == u)
                )

        if metadata is not None and metadata:
            for k, v in metadata.items():
                if not v:
                    query = query.filter(
                        ~IndexRecord.index_metadata.any(IndexRecordMetadata.key == k)
                    )
                else:
                    sub = session.query(IndexRecordMetadata.did)
                    sub = sub.filter(
                        and_(
                            IndexRecordMetadata.key == k, IndexRecordMetadata.value == v
                        )
                    )
                    query = query.filter(~IndexRecord.did.in_(sub.subquery()))

        if urls_metadata is not None and urls_metadata:
            query = query.join(IndexRecord.urls).join(IndexRecordUrl.url_metadata)
            for url_key, url_dict in urls_metadata.items():
                if not url_dict:
                    query = query.filter(~IndexRecordUrlMetadata.url.contains(url_key))
                else:
                    for k, v in url_dict.items():
                        if not v:
                            query = query.filter(
                                ~IndexRecordUrl.url_metadata.any(
                                    and_(
                                        IndexRecordUrlMetadata.key == k,
                                        IndexRecordUrlMetadata.url.contains(url_key),
                                    )
                                )
                            )
                        else:
                            sub = session.query(IndexRecordUrlMetadata.did)
                            sub = sub.filter(
                                and_(
                                    IndexRecordUrlMetadata.url.contains(url_key),
                                    IndexRecordUrlMetadata.key == k,
                                    IndexRecordUrlMetadata.value == v,
                                )
                            )
                            query = query.filter(~IndexRecord.did.in_(sub.subquery()))
        return query

    def get_urls(self, size=None, hashes=None, ids=None, start=0, limit=100):
        """
        Returns a list of urls matching supplied size/hashes/dids.
        """
        if not (size or hashes or ids):
            raise UserError("Please provide size/hashes/ids to filter")

        with self.session as session:
            query = session.query(IndexRecordUrl)

            query = query.join(IndexRecordUrl.index_record)
            if size:
                query = query.filter(IndexRecord.size == size)
            if hashes:
                for h, v in hashes.items():
                    # Select subset that matches given hash.
                    sub = session.query(IndexRecordHash.did)
                    sub = sub.filter(
                        and_(
                            IndexRecordHash.hash_type == h,
                            IndexRecordHash.hash_value == v,
                        )
                    )

                    # Filter anything that does not match.
                    query = query.filter(IndexRecordUrl.did.in_(sub.subquery()))
            if ids:
                query = query.filter(IndexRecordUrl.did.in_(ids))
            # Remove duplicates.
            query = query.distinct()

            # Return only specified window.
            query = query.offset(start)
            query = query.limit(limit)

            return [
                {"url": r.url, "metadata": {m.key: m.value for m in r.url_metadata}}
                for r in query
            ]

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
    ):
        """
        Creates a new record given size, urls, acl, authz, hashes, metadata,
        urls_metadata file name and version
        if did is provided, update the new record with the did otherwise create it
        """

        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        with self.session as session:
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
            session.merge(base_version)

            try:
                session.add(record)
                create_urls_metadata(urls_metadata, record, session)

                if self.config.get("ADD_PREFIX_ALIAS"):
                    self.add_prefix_alias(record, session)
                session.commit()
            except IntegrityError:
                raise UserError(
                    'did "{did}" already exists'.format(did=record.did), 400
                )

            return record.did, record.rev, record.baseid

    def add_blank_record(self, uploader, file_name=None):
        """
        Create a new blank record with only uploader and optionally
        file_name fields filled
        """
        with self.session as session:
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

            session.add(base_version)
            session.add(record)
            session.commit()

            return record.did, record.rev, record.baseid

    def update_blank_record(self, did, rev, size, hashes, urls):
        """
        Update a blank record with size and hashes, raise exception
        if the record is non-empty or the revision is not matched
        """
        hashes = hashes or {}
        urls = urls or []

        if not size or not hashes:
            raise UserError("No size or hashes provided")

        with self.session as session:
            query = session.query(IndexRecord).filter(IndexRecord.did == did)

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
            record.hashes = [
                IndexRecordHash(did=record.did, hash_type=h, hash_value=v)
                for h, v in hashes.items()
            ]
            record.urls = [IndexRecordUrl(did=record.did, url=url) for url in urls]

            record.rev = str(uuid.uuid4())[:8]

            session.add(record)
            session.commit()

            return record.did, record.rev, record.baseid

    def add_prefix_alias(self, record, session):
        """
        Create a index alias with the alias as {prefix:did}
        """
        prefix = self.config["DEFAULT_PREFIX"]
        alias = IndexRecordAlias(did=record.did, name=prefix + record.did)
        session.add(alias)

    def get_by_alias(self, alias):
        """
        Gets a record given a record alias
        """
        with self.session as session:
            try:
                record = (
                    session.query(IndexRecord)
                    .filter(IndexRecord.aliases.any(name=alias))
                    .one()
                )
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")
            return record.to_document_dict()

    def get_aliases_for_did(self, did):
        """
        Gets the aliases for a did
        """
        with self.session as session:
            query = session.query(IndexRecordAlias).filter(IndexRecordAlias.did == did)
            return [i.name for i in query]

    def get(self, did):
        """
        Gets a record given the record id or baseid.
        If the given id is a baseid, it will return the latest version
        """
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(
                or_(IndexRecord.did == did, IndexRecord.baseid == did)
            ).order_by(IndexRecord.created_date.desc())

            record = query.first()
            if record is None:
                raise NoRecordFound("no record found")
            return record.to_document_dict()

    def update(self, did, rev, changing_fields):
        """
        Updates an existing record with new values.
        """

        composite_fields = ["urls", "acl", "authz", "metadata", "urls_metadata"]

        with self.session as session:
            query = session.query(IndexRecord).filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            if rev != record.rev:
                raise RevisionMismatch("revision mismatch")

            auth.authorize("update", [u.resource for u in record.authz])
            # Some operations are dependant on other operations. For example
            # urls has to be updated before urls_metadata because of schema
            # constraints.
            if "urls" in changing_fields:
                for url in record.urls:
                    session.delete(url)

                record.urls = [
                    IndexRecordUrl(did=record.did, url=url)
                    for url in changing_fields["urls"]
                ]

            if "acl" in changing_fields:
                for ace in record.acl:
                    session.delete(ace)

                record.acl = [
                    IndexRecordACE(did=record.did, ace=ace)
                    for ace in set(changing_fields["acl"])
                ]

            if "authz" in changing_fields:
                for resource in record.authz:
                    session.delete(resource)

                record.authz = [
                    IndexRecordAuthz(did=record.did, resource=resource)
                    for resource in set(changing_fields["authz"])
                ]

            if "metadata" in changing_fields:
                for md_record in record.index_metadata:
                    session.delete(md_record)

                record.index_metadata = [
                    IndexRecordMetadata(did=record.did, key=m_key, value=m_value)
                    for m_key, m_value in changing_fields["metadata"].items()
                ]

            if "urls_metadata" in changing_fields:
                for url in record.urls:
                    for url_metadata in url.url_metadata:
                        session.delete(url_metadata)

                create_urls_metadata(changing_fields["urls_metadata"], record, session)

            for key, value in changing_fields.items():
                if key not in composite_fields:
                    # No special logic needed for other updates.
                    # ie file_name, version, etc
                    setattr(record, key, value)

            record.rev = str(uuid.uuid4())[:8]

            session.add(record)

            return record.did, record.baseid, record.rev

    def delete(self, did, rev):
        """
        Removes record if stored by backend.
        """
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

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
    ):
        """
        Add a record version given did
        """
        urls = urls or []
        acl = acl or []
        authz = authz or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        with self.session as session:
            query = session.query(IndexRecord).filter_by(did=current_did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound("no record found")
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            auth.authorize("update", [u.resource for u in record.authz] + authz)

            baseid = record.baseid
            record = IndexRecord()
            did = new_did or str(uuid.uuid4())

            record.did = did
            record.baseid = baseid
            record.rev = str(uuid.uuid4())[:8]
            record.form = form
            record.size = size
            record.file_name = file_name
            record.version = version

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

            try:
                session.add(record)
                create_urls_metadata(urls_metadata, record, session)
                session.commit()
            except IntegrityError:
                raise UserError("{did} already exists".format(did=did), 400)

            return record.did, record.baseid, record.rev

    def get_all_versions(self, did):
        """
        Get all record versions given did
        """
        ret = dict()
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
                baseid = record.baseid
            except NoResultFound:
                record = session.query(BaseVersion).filter_by(baseid=did).first()
                if not record:
                    raise NoRecordFound("no record found")
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = session.query(IndexRecord)
            records = query.filter(IndexRecord.baseid == baseid).all()

            for idx, record in enumerate(records):

                ret[idx] = record.to_document_dict()

        return ret

    def get_latest_version(self, did, has_version=None):
        """
        Get the lattest record version given did
        """
        with self.session as session:
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == did)

            try:
                record = query.one()
                baseid = record.baseid
            except NoResultFound:
                baseid = did
            except MultipleResultsFound:
                raise MultipleRecordsFound("multiple records found")

            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.baseid == baseid).order_by(
                IndexRecord.created_date.desc()
            )
            if has_version:
                query = query.filter(IndexRecord.version.isnot(None))
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
            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.did == record)

            return query.exists()

    def __iter__(self):
        """
        Iterator over unique records stored by backend.
        """
        with self.session as session:
            for i in session.query(IndexRecord):
                yield i.did

    def totalbytes(self):
        """
        Total number of bytes of data represented in the index.
        """
        with self.session as session:
            result = session.execute(select([func.sum(IndexRecord.size)])).scalar()
            if result is None:
                return 0
            return int(result)

    def len(self):
        """
        Number of unique records stored by backend.
        """
        with self.session as session:

            return session.execute(
                select([func.count()]).select_from(IndexRecord)
            ).scalar()


def migrate_1(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ALTER COLUMN size TYPE bigint;".format(
            IndexRecord.__tablename__
        )
    )


def migrate_2(session, **kwargs):
    """
    Migrate db from version 1 -> 2
    """
    try:
        session.execute(
            "ALTER TABLE {} \
                ADD COLUMN baseid VARCHAR DEFAULT NULL, \
                ADD COLUMN created_date TIMESTAMP DEFAULT NOW(), \
                ADD COLUMN updated_date TIMESTAMP DEFAULT NOW()".format(
                IndexRecord.__tablename__
            )
        )
    except ProgrammingError:
        session.rollback()
    session.commit()

    count = session.execute(
        "SELECT COUNT(*) FROM {};".format(IndexRecord.__tablename__)
    ).fetchone()[0]

    # create tmp_index_record table for fast retrival
    try:
        session.execute(
            "CREATE TABLE tmp_index_record AS SELECT did, ROW_NUMBER() OVER (ORDER BY did) AS RowNumber \
            FROM {}".format(
                IndexRecord.__tablename__
            )
        )
    except ProgrammingError:
        session.rollback()

    for loop in range(count):
        baseid = str(uuid.uuid4())
        session.execute(
            "UPDATE index_record SET baseid = '{}'\
             WHERE did =  (SELECT did FROM tmp_index_record WHERE RowNumber = {});".format(
                baseid, loop + 1
            )
        )
        session.execute(
            "INSERT INTO {}(baseid) VALUES('{}');".format(
                BaseVersion.__tablename__, baseid
            )
        )

    session.execute(
        "ALTER TABLE {} \
         ADD CONSTRAINT baseid_FK FOREIGN KEY (baseid) references base_version(baseid);".format(
            IndexRecord.__tablename__
        )
    )

    # drop tmp table
    session.execute("DROP TABLE IF EXISTS tmp_index_record;")


def migrate_3(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ADD COLUMN file_name VARCHAR;".format(IndexRecord.__tablename__)
    )

    session.execute(
        "CREATE INDEX {tb}__file_name_idx ON {tb} ( file_name )".format(
            tb=IndexRecord.__tablename__
        )
    )


def migrate_4(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ADD COLUMN version VARCHAR;".format(IndexRecord.__tablename__)
    )

    session.execute(
        "CREATE INDEX {tb}__version_idx ON {tb} ( version )".format(
            tb=IndexRecord.__tablename__
        )
    )


def migrate_5(session, **kwargs):
    """
    Create Index did on IndexRecordUrl, IndexRecordMetadata and
    IndexRecordUrlMetadata tables
    """
    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )".format(tb=IndexRecordUrl.__tablename__)
    )

    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )".format(tb=IndexRecordHash.__tablename__)
    )

    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )".format(
            tb=IndexRecordMetadata.__tablename__
        )
    )

    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )".format(
            tb=IndexRecordUrlMetadata.__tablename__
        )
    )


def migrate_6(session, **kwargs):
    pass


def migrate_7(session, **kwargs):
    existing_acls = (
        session.query(IndexRecordMetadata).filter_by(key="acls").yield_per(1000)
    )
    for metadata in existing_acls:
        acl = metadata.value.split(",")
        for ace in acl:
            entry = IndexRecordACE(did=metadata.did, ace=ace)
            session.add(entry)
            session.delete(metadata)


def migrate_8(session, **kwargs):
    """
    create index on IndexRecord.baseid
    """
    session.execute(
        "CREATE INDEX ix_{tb}_baseid ON {tb} ( baseid )".format(
            tb=IndexRecord.__tablename__
        )
    )


def migrate_9(session, **kwargs):
    """
    create index on IndexRecordHash.hash_value
    create index on IndexRecord.size
    """
    session.execute(
        "CREATE INDEX ix_{tb}_size ON {tb} ( size )".format(
            tb=IndexRecord.__tablename__
        )
    )

    session.execute(
        "CREATE INDEX index_record_hash_type_value_idx ON {tb} ( hash_value, hash_type )".format(
            tb=IndexRecordHash.__tablename__
        )
    )


def migrate_10(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ADD COLUMN uploader VARCHAR;".format(IndexRecord.__tablename__)
    )

    session.execute(
        "CREATE INDEX {tb}__uploader_idx ON {tb} ( uploader )".format(
            tb=IndexRecord.__tablename__
        )
    )


def migrate_11(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ADD COLUMN rbac VARCHAR;".format(IndexRecord.__tablename__)
    )


def migrate_12(session, **kwargs):
    session.execute(
        "ALTER TABLE {} DROP COLUMN rbac;".format(IndexRecord.__tablename__)
    )


# ordered schema migration functions that the index should correspond to
# CURRENT_SCHEMA_VERSION - 1 when it's written
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
]
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
