import copy
import datetime
import uuid
from contextlib import contextmanager

from cdislogging import get_logger
from future.utils import iteritems
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
    not_,
    or_,
    select,
)
from sqlalchemy.dialects.postgres import JSONB
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import joinedload, relationship, sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from indexd.errors import UserError
from indexd.index.blueprint import separate_metadata
from indexd.index.driver import IndexDriverABC
from indexd.index.errors import (
    MultipleRecordsFound,
    NoRecordFound,
    RevisionMismatch,
    UnhealthyCheck,
)
from indexd.utils import (
    init_schema_version,
    is_empty_database,
    migrate_database,
)

Base = declarative_base()


class BaseVersion(Base):
    """
    Base index record version representation.

    This class needs to exist so the migration functions work. The class and
    table will continue to exist until the migration functions are modified to
    create tables as they were originally designed for the specific migration
    function version.
    """
    __tablename__ = 'base_version'

    baseid = Column(String, primary_key=True)


class IndexSchemaVersion(Base):
    """
    Table to track current database's schema version
    """
    __tablename__ = 'index_schema_version'
    version = Column(Integer, default=0, primary_key=True)


class IndexRecord(Base):
    """
    Base index record representation.
    """
    __tablename__ = 'index_record'

    did = Column(String, primary_key=True)

    baseid = Column(String, index=True)
    rev = Column(String)
    form = Column(String)
    size = Column(BigInteger, index=True)
    release_number = Column(String, index=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow)
    file_name = Column(String, index=True)
    version = Column(String, index=True)
    uploader = Column(String, index=True)
    index_metadata = Column(JSONB)

    urls_metadata = relationship(
        'IndexRecordUrlMetadataJsonb',
        backref='index_record',
        cascade='all, delete-orphan',
    )

    acl = relationship(
        'IndexRecordACE',
        backref='index_record',
        cascade='all, delete-orphan',
    )

    hashes = relationship(
        'IndexRecordHash',
        backref='index_record',
        cascade='all, delete-orphan',
    )

    aliases = relationship(
        'IndexRecordAlias',
        backref='index_record',
        cascade='all, delete-orphan',
    )

    def to_document_dict(self):
        """
        Get the full index document
        """
        urls = [u.url for u in self.urls_metadata]
        acl = [u.ace for u in self.acl]
        hashes = {h.hash_type: h.hash_value for h in self.hashes}
        metadata = self.index_metadata or {}

        # Add this field back to the returned metadata json section but also
        # return it separately in the main section of the json blob. This
        # allows current clients to function without change but lets new
        # clients use the release_number field directly.
        release_number = self.release_number
        if release_number:
            # If it doesn't exist then don't return the key in the json.
            metadata['release_number'] = release_number

        urls_metadata = extract_urls_metadata(self.urls_metadata)
        created_date = self.created_date.isoformat()
        updated_date = self.updated_date.isoformat()

        return {
            'did': self.did,
            'baseid': self.baseid,
            'rev': self.rev,
            'size': self.size,
            'file_name': self.file_name,
            'version': self.version,
            'uploader': self.uploader,
            'urls': urls,
            'urls_metadata': urls_metadata,
            'acl': acl,
            'hashes': hashes,
            'release_number': release_number,
            'metadata': metadata,
            'form': self.form,
            'created_date': created_date,
            "updated_date": updated_date,
        }


class IndexRecordAlias(Base):
    """
    Alias attached to index record
    """

    __tablename__ = 'index_record_alias'

    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    name = Column(String, primary_key=True)

    __table_args__ = (
        Index('index_record_alias_idx', 'did'),
        Index('index_record_alias_name', 'name'),
    )


class IndexRecordUrl(Base):
    """
    Base index record url representation.
    """

    __tablename__ = 'index_record_url'

    did = Column(String, primary_key=True)
    url = Column(String, primary_key=True)
    __table_args__ = (
        Index('index_record_url_idx', 'did'),
    )


class IndexRecordACE(Base):
    """
    index record access control entry representation.
    """

    __tablename__ = 'index_record_ace'

    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    # access control entry
    ace = Column(String, primary_key=True)

    __table_args__ = (
        Index('index_record_ace_idx', 'did'),
    )


class IndexRecordMetadata(Base):
    """
    Metadata attached to index document
    """

    __tablename__ = 'index_record_metadata'
    key = Column(String, primary_key=True)
    did = Column(String, primary_key=True)
    value = Column(String)
    __table_args__ = (
        Index('index_record_metadata_idx', 'did'),
    )


class IndexRecordUrlMetadata(Base):
    """
    Metadata attached to url
    """

    __tablename__ = 'index_record_url_metadata'
    did = Column(String, index=True, primary_key=True)
    url = Column(String, primary_key=True)
    key = Column(String, primary_key=True)
    value = Column(String)
    __table_args__ = (
        ForeignKeyConstraint(['did', 'url'],
                             ['index_record_url.did', 'index_record_url.url']),
        Index('index_record_url_metadata_idx', 'did'),
    )


class IndexRecordUrlMetadataJsonb(Base):
    """
    Metadata attached to url in jsonb format
    """

    __tablename__ = 'index_record_url_metadata_jsonb'
    did = Column(String, primary_key=True)
    url = Column(String, primary_key=True)
    type = Column(String, index=True)
    state = Column(String, index=True)
    urls_metadata = Column(JSONB)
    __table_args__ = (ForeignKeyConstraint(['did'], ['index_record.did']),)


class IndexRecordHash(Base):
    """
    Base index record hash representation.
    """
    __tablename__ = 'index_record_hash'
    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    hash_type = Column(String, primary_key=True)
    hash_value = Column(String)
    __table_args__ = (
        Index('index_record_hash_idx', 'did'),
        Index('index_record_hash_type_value_idx', 'hash_value', 'hash_type'),
    )


def separate_urls_metadata(urls_metadata):
    """Separate type and state from urls_metadata record.

    Type and state are removed from the urls_metadata key value pair/jsonb
    object. To keep backwards compatibility these are still ingested
    through the urls_metadata field. We have to manually separate them and
    later combine them to maintain compatibility with the current indexclient.
    """
    urls_metadata = copy.deepcopy(urls_metadata)

    # If these fields are given, then remove them from the json
    # blob so it doesn't get put in the urls_metadata table.
    u_type = urls_metadata.pop('type', None)
    u_state = urls_metadata.pop('state', None)

    return u_type, u_state, urls_metadata


def create_urls_metadata(did, metadata_fields):
    """
    Create url metadata record in database.

    Each row is: DID | URL | TYPE | STATE | METADATA
    """
    rows = []
    for url, urls_metadata in iteritems(metadata_fields):
        u_type, u_state, urls_metadata = separate_urls_metadata(urls_metadata)
        rows.append(IndexRecordUrlMetadataJsonb(
            did=did,
            url=url,
            type=u_type,
            state=u_state,
            urls_metadata=urls_metadata,
        ))

    return rows


class SQLAlchemyIndexDriver(IndexDriverABC):
    """
    SQLAlchemy implementation of index driver.
    """

    def __init__(
            self, conn, logger=None, auto_migrate=True,
            index_config=None, **config):
        """
        Initialize the SQLAlchemy database driver.
        """
        super(SQLAlchemyIndexDriver, self).__init__(conn, **config)
        self.logger = logger or get_logger('SQLAlchemyIndexDriver')
        self.config = index_config or {}

        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)

        is_empty_db = is_empty_database(driver=self)
        Base.metadata.create_all()
        if is_empty_db:
            init_schema_version(
                driver=self,
                model=IndexSchemaVersion,
                version=CURRENT_SCHEMA_VERSION)

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
            model=IndexSchemaVersion)

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

    def ids(self,
            limit=100,
            start=None,
            size=None,
            urls=None,
            acl=None,
            hashes=None,
            file_name=None,
            version=None,
            uploader=None,
            release_number=None,
            metadata=None,
            ids=None,
            urls_metadata=None,
            negate_params=None):
        """
        Returns list of records stored by the backend.
        """
        with self.session as session:
            query = session.query(IndexRecord)

            # Enable joinedload on all relationships so that we won't have to
            # do a bunch of selects when we assemble our response.
            query = query.options(joinedload(IndexRecord.urls_metadata))
            query = query.options(joinedload(IndexRecord.acl))
            query = query.options(joinedload(IndexRecord.hashes))
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
                query = query.join(IndexRecord.urls_metadata)
                for u in urls:
                    query = query.filter(IndexRecordUrlMetadataJsonb.url == u)

            if acl:
                query = query.join(IndexRecord.acl)
                for u in acl:
                    query = query.filter(IndexRecordACE.ace == u)
            elif acl == []:
                query = query.filter(IndexRecord.acl == None)

            if hashes:
                for h, v in hashes.items():
                    sub = session.query(IndexRecordHash.did)
                    sub = sub.filter(and_(
                        IndexRecordHash.hash_type == h,
                        IndexRecordHash.hash_value == v,
                    ))
                    query = query.filter(IndexRecord.did.in_(sub.subquery()))

            if release_number:
                query = query.filter(IndexRecord.release_number == release_number)

            if metadata:
                for k, v in metadata.items():
                    query = query.filter(
                        IndexRecord.index_metadata[k].astext == v
                    )

            if urls_metadata:

                query = query.join(IndexRecord.urls_metadata)
                for url_key, url_dict in urls_metadata.items():
                    u_type, u_state, url_dict = separate_urls_metadata(url_dict)

                    query = query.filter(
                        IndexRecordUrlMetadataJsonb.url.contains(url_key))
                    for k, v in url_dict.items():
                        query = query.filter(
                            IndexRecordUrlMetadataJsonb.urls_metadata[k].astext == v
                        )
                    if u_type:
                        query = query.filter(
                            IndexRecordUrlMetadataJsonb.type == u_type)
                    if u_state:
                        query = query.filter(
                            IndexRecordUrlMetadataJsonb.state == u_state)

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
    def _negate_filter(session,
                       query,
                       urls=None,
                       acl=None,
                       file_name=None,
                       version=None,
                       metadata=None,
                       urls_metadata=None):
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
            query = query.join(IndexRecord.urls_metadata)
            for u in urls:
                query = query.filter(
                    ~IndexRecord.urls_metadata.any(IndexRecordUrlMetadataJsonb.url == u))

        if acl is not None and acl:
            query = query.join(IndexRecord.acl)
            for u in acl:
                query = query.filter(~IndexRecord.acl.any(IndexRecordACE.ace == u))

        if metadata is not None and metadata:
            for k, v in metadata.items():
                if not v:
                    query = query.filter(not_(
                        IndexRecord.index_metadata.has_key(k)
                    ))
                else:
                    query = query.filter(not_(
                        IndexRecord.index_metadata[k].astext == v
                    ))

        if urls_metadata is not None and urls_metadata:
            query = query.join(IndexRecord.urls_metadata)
            for url_key, url_dict in urls_metadata.items():
                if not url_dict:
                    query = query.filter(~IndexRecordUrlMetadataJsonb.url.contains(url_key))
                else:
                    # Filter first on the fields separated out from the
                    # metadata jsonb blob.
                    u_type, u_state, url_dict = separate_urls_metadata(url_dict)
                    if u_type is not None:
                        if not u_type:
                            query = query.filter(~IndexRecord.urls_metadata.any(and_(
                                IndexRecordUrlMetadataJsonb.type == '',
                                IndexRecordUrlMetadataJsonb.url.contains(url_key)
                            )))

                        else:
                            sub = session.query(IndexRecordUrlMetadataJsonb.did)
                            sub = sub.filter(and_(
                                IndexRecordUrlMetadataJsonb.url.contains(url_key),
                                IndexRecordUrlMetadataJsonb.type == u_type
                            ))
                            query = query.filter(~IndexRecord.did.in_(sub.subquery()))

                    if u_state is not None:
                        if not u_state:
                            query = query.filter(~IndexRecord.urls_metadata.any(and_(
                                IndexRecordUrlMetadataJsonb.state == '',
                                IndexRecordUrlMetadataJsonb.url.contains(url_key)
                            )))
                        else:
                            sub = session.query(IndexRecordUrlMetadataJsonb.did)
                            sub = sub.filter(and_(
                                IndexRecordUrlMetadataJsonb.url.contains(url_key),
                                IndexRecordUrlMetadataJsonb.state == u_state
                            ))
                            query = query.filter(~IndexRecord.did.in_(sub.subquery()))

                    for k, v in url_dict.items():
                        if not v:
                            query = query.filter(
                                ~IndexRecord.urls_metadata.any(
                                    and_(
                                        IndexRecordUrlMetadataJsonb.urls_metadata.has_key(k),
                                        IndexRecordUrlMetadataJsonb.url.contains(url_key)
                                    )
                                )
                            )
                        else:
                            sub = session.query(IndexRecordUrlMetadataJsonb.did)
                            sub = sub.filter(
                                and_(
                                    IndexRecordUrlMetadataJsonb.url.contains(url_key),
                                    IndexRecordUrlMetadataJsonb.urls_metadata[k].astext == v
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
            query = session.query(IndexRecordUrlMetadataJsonb)

            query = query.join(IndexRecordUrlMetadataJsonb.index_record)
            if size:
                query = query.filter(IndexRecord.size == size)
            if hashes:
                for h, v in hashes.items():
                    # Select subset that matches given hash.
                    sub = session.query(IndexRecordHash.did)
                    sub = sub.filter(and_(
                        IndexRecordHash.hash_type == h,
                        IndexRecordHash.hash_value == v,
                    ))

                    # Filter anything that does not match.
                    query = query.filter(IndexRecordUrlMetadataJsonb.did.in_(sub.subquery()))
            if ids:
                query = query.filter(IndexRecordUrlMetadataJsonb.did.in_(ids))
            # Remove duplicates.
            query = query.distinct()

            # Return only specified window.
            query = query.offset(start)
            query = query.limit(limit)

            urls_metadata = extract_urls_metadata(query)
            return [
                {'url': url, 'metadata': metadata}
                for url, metadata in urls_metadata.items()
            ]

    def add(self,
            form,
            did=None,
            size=None,
            file_name=None,
            release_number=None,
            metadata=None,
            urls_metadata=None,
            version=None,
            urls=None,
            acl=None,
            hashes=None,
            baseid=None,
            uploader=None):
        """
        Creates a new record given size, urls, acl, hashes, metadata,
        urls_metadata file name and version
        if did is provided, update the new record with the did otherwise create it
        """

        urls = urls or []
        acl = acl or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        with self.session as session:
            record = IndexRecord()

            if not baseid:
                baseid = str(uuid.uuid4())

            record.baseid = baseid
            record.file_name = file_name
            record.version = version

            if did:
                record.did = did
            else:
                new_did = str(uuid.uuid4())
                if self.config.get('PREPEND_PREFIX'):
                    new_did = self.config['DEFAULT_PREFIX'] + new_did
                record.did = new_did

            record.rev = str(uuid.uuid4())[:8]

            record.form, record.size = form, size

            record.uploader = uploader

            record.acl = [IndexRecordACE(
                did=record.did,
                ace=ace,
            ) for ace in acl]

            record.hashes = [IndexRecordHash(
                did=record.did,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]

            record.release_number = release_number
            record.index_metadata = metadata

            record.urls_metadata = create_urls_metadata(
                record.did,
                urls_metadata,
            )

            try:
                session.add(record)

                if self.config.get('ADD_PREFIX_ALIAS'):
                    self.add_prefix_alias(record, session)
                session.commit()
            except IntegrityError:
                raise UserError('did "{did}" already exists'.format(did=record.did), 400)

            return record.did, record.rev, record.baseid

    def add_blank_record(self, uploader, file_name=None):
        """
        Create a new blank record with only uploader and optionally
        file_name fields filled
        """
        with self.session as session:
            record = IndexRecord()

            did = str(uuid.uuid4())
            baseid = str(uuid.uuid4())
            if self.config.get('PREPEND_PREFIX'):
                did = self.config['DEFAULT_PREFIX'] + did

            record.did = did

            record.rev = str(uuid.uuid4())[:8]
            record.baseid = baseid
            record.uploader = uploader
            record.file_name = file_name

            session.add(record)
            session.commit()

            return record.did, record.rev, record.baseid

    def update_blank_record(self, did, rev, size, hashes, urls_metadata):
        """
        Update a blank record with size and hashes, raise exception
        if the record is non-empty or the revision is not matched
        """
        hashes = hashes or {}
        urls_metadata = urls_metadata or {}

        if not size or not hashes:
            raise UserError("No size or hashes provided")

        with self.session as session:
            query = session.query(IndexRecord).filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            if record.size or record.hashes:
                raise UserError("update api is not supported for non-empty record!")

            if rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            record.size = size
            record.hashes = [IndexRecordHash(
                did=record.did,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]
            record.urls_metadata = create_urls_metadata(
                record.did,
                urls_metadata,
            )
            record.rev = str(uuid.uuid4())[:8]

            session.add(record)
            session.commit()

            return record.did, record.rev, record.baseid

    def add_prefix_alias(self, record, session):
        """
        Create a index alias with the alias as {prefix:did}
        """
        prefix = self.config['DEFAULT_PREFIX']
        alias = IndexRecordAlias(did=record.did, name=prefix+record.did)
        session.add(alias)

    def get_by_alias(self, alias):
        """
        Gets a record given a record alias
        """
        with self.session as session:
            try:
                record = (
                    session.query(IndexRecord)
                    .filter(IndexRecord.aliases.any(name=alias)).one()
                )
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')
            return record.to_document_dict()

    def get_aliases_for_did(self, did):
        """
        Gets the aliases for a did
        """
        with self.session as session:
            query = (
                session.query(IndexRecordAlias)
                .filter(IndexRecordAlias.did == did)
            )
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
                raise NoRecordFound('no record found')
            return record.to_document_dict()

    def update(self, did, rev, changing_fields):
        """
        Updates an existing record with new values.
        """

        composite_fields = ['acl', 'metadata', 'urls_metadata', 'hashes']

        with self.session as session:
            query = session.query(IndexRecord).filter(IndexRecord.did == did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            if rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            # Some operations might become dependant on other operations based
            # on future schema constraints.
            if 'acl' in changing_fields:
                for ace in record.acl:
                    session.delete(ace)

                record.acl = [
                    IndexRecordACE(did=record.did, ace=ace)
                    for ace in changing_fields['acl']
                ]

            if 'metadata' in changing_fields:
                release_number, metadata = separate_metadata(changing_fields['metadata'])
                if release_number:
                    record.release_number = release_number
                record.index_metadata = metadata

            if 'hashes' in changing_fields:
                for hash_doc in record.hashes:
                    session.delete(hash_doc)

                record.hashes = [
                    IndexRecordHash(
                        did=record.did,
                        hash_type=hash_type,
                        hash_value=hash_value
                    )
                    for hash_type, hash_value in changing_fields['hashes'].items()]

            if 'urls_metadata' in changing_fields:
                record.urls_metadata = create_urls_metadata(
                    record.did,
                    changing_fields['urls_metadata'],
                )

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
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

            if rev != record.rev:
                raise RevisionMismatch('revision mismatch')

            session.delete(record)

    def add_version(self,
                    current_did,
                    form,
                    new_did=None,
                    size=None,
                    file_name=None,
                    release_number=None,
                    metadata=None,
                    urls_metadata=None,
                    version=None,
                    urls=None,
                    acl=None,
                    hashes=None):
        """
        Add a record version given did
        """
        urls = urls or []
        acl = acl or []
        hashes = hashes or {}
        metadata = metadata or {}
        urls_metadata = urls_metadata or {}

        with self.session as session:
            query = session.query(IndexRecord).filter_by(did=current_did)

            try:
                record = query.one()
            except NoResultFound:
                raise NoRecordFound('no record found')
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

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

            record.urls_metadata = create_urls_metadata(
                record.did,
                urls_metadata,
            )

            record.acl = [IndexRecordACE(
                did=record.did,
                ace=ace,
            ) for ace in set(acl)]

            record.hashes = [IndexRecordHash(
                did=record.did,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]

            record.release_number = release_number
            record.index_metadata = metadata

            record.urls_metadata = create_urls_metadata(record.did, urls_metadata)
            try:
                session.add(record)
                session.commit()
            except IntegrityError:
                raise UserError('{did} already exists'.format(did=did), 400)

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
                record = session.query(IndexRecord).filter_by(baseid=did).first()
                if not record:
                    raise NoRecordFound('no record found')
                else:
                    baseid = record.baseid
            except MultipleResultsFound:
                raise MultipleRecordsFound('multiple records found')

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
                raise MultipleRecordsFound('multiple records found')

            query = session.query(IndexRecord)
            query = query.filter(IndexRecord.baseid == baseid) \
                .order_by(IndexRecord.created_date.desc())
            if has_version:
                query = query.filter(IndexRecord.version.isnot(None))
            record = query.first()
            if (not record):
                raise NoRecordFound('no record found')

            return record.to_document_dict()

    def health_check(self):
        """
        Does a health check of the backend.
        """
        with self.session as session:
            try:
                session.execute('SELECT 1')
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
            return long(result)

    def len(self):
        """
        Number of unique records stored by backend.
        """
        with self.session as session:

            return session.execute(select([func.count()]).select_from(IndexRecord)).scalar()


def extract_urls_metadata(urls_metadata_results):
    """
    These columns are placed back into the urls_metadata dict for returning
    in order to preserve backward compatibility. It's modular enough to be
    easily changed in the future
    """
    urls_metadata = {}
    for url in urls_metadata_results:
        temp_dict = dict(url.urls_metadata or {})
        if url.type is not None:
            temp_dict['type'] = url.type
        if url.state is not None:
            temp_dict['state'] = url.state

        urls_metadata[url.url] = temp_dict

    return urls_metadata


# NOTE: Using a sqlalchemy session to do migrations means that the migration
# logic uses sqlalchemy models defined in this module. That means if a breaking
# change to a model is made, one or more migration steps might not work.
# In the future consider using SQL queries to do the migrations.
def migrate_1(session, **kwargs):
    session.execute("ALTER TABLE index_record ALTER COLUMN size TYPE bigint")


def migrate_2(session, **kwargs):
    """
    Migrate db from version 1 -> 2
    """
    try:
        session.execute(
            "ALTER TABLE index_record \
                ADD COLUMN baseid VARCHAR DEFAULT NULL, \
                ADD COLUMN created_date TIMESTAMP DEFAULT NOW(), \
                ADD COLUMN updated_date TIMESTAMP DEFAULT NOW()")
    except ProgrammingError:
        session.rollback()
    session.commit()

    count = session.execute("SELECT COUNT(*) FROM index_record").fetchone()[0]

    # create tmp_index_record table for fast retrival
    try:
        session.execute("""
            CREATE TABLE tmp_index_record AS
                SELECT did, ROW_NUMBER() OVER (ORDER BY did) AS RowNumber
                FROM index_record
        """)
    except ProgrammingError:
        session.rollback()

    for loop in range(count):
        baseid = str(uuid.uuid4())
        session.execute(
            "UPDATE index_record SET baseid = '{}'\
             WHERE did =  (SELECT did FROM tmp_index_record WHERE RowNumber = {})".format(baseid, loop + 1))
        session.execute(
            "INSERT INTO base_version(baseid) VALUES('{}')".format(baseid))

    session.execute(
        "ALTER TABLE index_record \
         ADD CONSTRAINT baseid_FK FOREIGN KEY (baseid) references base_version(baseid)")

    # drop tmp table
    session.execute(
        "DROP TABLE IF EXISTS tmp_index_record"
    )


def migrate_3(session, **kwargs):
    session.execute("ALTER TABLE index_record ADD COLUMN file_name VARCHAR")

    session.execute(
        "CREATE INDEX index_record__file_name_idx ON index_record ( file_name )")


def migrate_4(session, **kwargs):
    session.execute("ALTER TABLE index_record ADD COLUMN version VARCHAR")

    session.execute(
        "CREATE INDEX index_record__version_idx ON index_record ( version )")


def migrate_5(session, **kwargs):
    """
    Create Index did on IndexRecordUrl, IndexRecordMetadata and
    IndexRecordUrlMetadata tables
    """
    session.execute(
        "CREATE INDEX index_record_url_idx ON index_record_url ( did )")

    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )"
        .format(tb=IndexRecordHash.__tablename__))

    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )"
        .format(tb=IndexRecordMetadata.__tablename__))

    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )"
        .format(tb=IndexRecordUrlMetadata.__tablename__))


def migrate_6(session, **kwargs):
    pass


def migrate_7(session, **kwargs):
    existing_acls = (
        session.query(IndexRecordMetadata)
        .filter_by(key='acls').yield_per(1000)
    )
    for metadata in existing_acls:
        acl = metadata.value.split(',')
        for ace in acl:
            entry = IndexRecordACE(
                did=metadata.did,
                ace=ace)
            session.add(entry)
            session.delete(metadata)


def migrate_8(session, **kwargs):
    """
    create index on IndexRecord.baseid
    """
    session.execute(
        "CREATE INDEX ix_index_record_baseid ON index_record ( baseid )")


def migrate_9(session, **kwargs):
    """
    create index on IndexRecordHash.hash_value
    create index on IndexRecord.size
    """
    session.execute(
        "CREATE INDEX ix_index_record_size ON index_record ( size )")

    session.execute(
        "CREATE INDEX index_record_hash_type_value_idx ON {tb} ( hash_value, hash_type )"
        .format(tb=IndexRecordHash.__tablename__))


def migrate_10(session, **kwargs):
    session.execute("ALTER TABLE index_record ADD COLUMN uploader VARCHAR")

    session.execute(
        "CREATE INDEX index_record__uploader_idx ON index_record ( uploader )")


def migrate_11(session, **kwargs):
    session.execute("ALTER TABLE index_record ADD COLUMN release_number VARCHAR")
    session.execute("ALTER TABLE index_record ADD COLUMN index_metadata jsonb")
    session.execute("ALTER TABLE index_record DROP CONSTRAINT index_record_baseid_fkey")


def migrate_12(session, **kwargs):
    """
    Copy all rows from the IndexRecordUrlMetadata and IndexRecordMetada tables
    to the new JSONB tables.
    """

    # Chunk the migration steps by starting did element. Add an extra char at
    # the end of the final "did" because we are not doing an inclusive or.
    chunk_range = [hex(i)[2:].zfill(2) for i in range(256)] \
        + ['ffffffff-ffff-ffff-ffff-ffffffffffff0']
    for i in range(len(chunk_range) - 1):
        from_chunk = chunk_range[i]
        to_chunk = chunk_range[i + 1]

        # metadata migration to jsonb
        session.execute("""
            UPDATE index_record r
            SET (index_metadata) = (m.meta)
            FROM (
                SELECT did, CAST(json_object_agg(key, value) AS JSONB) AS meta
                FROM index_record_metadata
                WHERE key <> 'release_number' AND did>='{}' AND did<'{}'
                GROUP BY did
            ) AS m
            WHERE r.did=m.did
        """.format(from_chunk, to_chunk))

        session.execute("""
            UPDATE index_record r
            SET (release_number) = (re.release_number)
            FROM (
                SELECT did, value as release_number
                FROM index_record_metadata
                WHERE key = 'release_number' AND did>='{}' AND did<'{}'
            ) AS re
            WHERE r.did=re.did
        """.format(from_chunk, to_chunk))

        # urls metadata migration to jsonb
        session.execute("""
            INSERT INTO index_record_url_metadata_jsonb (did, url)
            SELECT did, url
            FROM index_record_url
            WHERE did>='{}' AND did<'{}'
        """.format(from_chunk, to_chunk))

        session.execute("""
            UPDATE index_record_url_metadata_jsonb as main
            SET (urls_metadata) = (um.meta)
            FROM (
                SELECT did, url, CAST(json_object_agg(key, value) AS JSONB) AS meta
                FROM index_record_url_metadata
                WHERE key NOT IN ('type', 'state') AND did>='{}' AND did<'{}'
                GROUP BY did, url
            ) AS um
            WHERE main.did=um.did and main.url=um.url
        """.format(from_chunk, to_chunk))

        session.execute("""
            UPDATE index_record_url_metadata_jsonb as main
            SET (type) = (t.type)
            FROM (
                SELECT did, url, value AS type
                FROM index_record_url_metadata
                WHERE key = 'type' AND did>='{}' AND did<'{}'
            ) AS t
            WHERE main.did=t.did and main.url=t.url
        """.format(from_chunk, to_chunk))

        session.execute("""
            UPDATE index_record_url_metadata_jsonb as main
            SET (state) = (s.state)
            FROM (
                SELECT did, url, value AS state
                FROM index_record_url_metadata
                WHERE key = 'state' AND did>='{}' AND did<'{}'
            ) AS s
            WHERE main.did=s.did and main.url=s.url
        """.format(from_chunk, to_chunk))


# ordered schema migration functions that the index should correspond to
# CURRENT_SCHEMA_VERSION - 1 when it's written
SCHEMA_MIGRATION_FUNCTIONS = [
    migrate_1, migrate_2, migrate_3, migrate_4, migrate_5,
    migrate_6, migrate_7, migrate_8, migrate_9, migrate_10,
    migrate_11, migrate_12,
]
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
