import datetime
from future.utils import iteritems
import uuid

from cdislogging import get_logger
from contextlib import contextmanager
from sqlalchemy import func, select, and_, or_
from sqlalchemy import String, Column, Integer, BigInteger, DateTime
from sqlalchemy import ForeignKey, ForeignKeyConstraint, Index
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from indexd.index.driver import IndexDriverABC
from indexd.index.errors import NoRecordFound, MultipleRecordsFound, \
    RevisionMismatch, UnhealthyCheck
from indexd.errors import UserError
from indexd.utils import migrate_database, init_schema_version, is_empty_database
from sqlalchemy.exc import ProgrammingError

Base = declarative_base()


class BaseVersion(Base):
    """
    Base index record version representation.
    """
    __tablename__ = 'base_version'

    baseid = Column(String, primary_key=True)
    dids = relationship(
        'IndexRecord',
        backref='base_version',
        cascade='all, delete-orphan')


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

    baseid = Column(String, ForeignKey('base_version.baseid'), index=True)
    rev = Column(String)
    form = Column(String)
    size = Column(BigInteger)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow)
    file_name = Column(String, index=True)
    version = Column(String, index=True)

    urls = relationship(
        'IndexRecordUrl',
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

    index_metadata = relationship(
        'IndexRecordMetadata',
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
        urls = [u.url for u in self.urls]
        acl = [u.ace for u in self.acl]
        hashes = {h.hash_type: h.hash_value for h in self.hashes}
        metadata = {m.key: m.value for m in self.index_metadata}

        urls_metadata = {
            u.url: {m.key: m.value for m in u.url_metadata} for u in self.urls}
        created_date = self.created_date.isoformat()
        updated_date = self.updated_date.isoformat()

        return {
            'did': self.did,
            'baseid': self.baseid,
            'rev': self.rev,
            'size': self.size,
            'file_name': self.file_name,
            'version': self.version,
            'urls': urls,
            'urls_metadata': urls_metadata,
            'acl': acl,
            'hashes': hashes,
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

    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    url = Column(String, primary_key=True)

    url_metadata = relationship(
        'IndexRecordUrlMetadata',
        backref='index_record_url',
        cascade='all, delete-orphan',
    )
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
    did = Column(String, ForeignKey('index_record.did'), primary_key=True)
    value = Column(String)
    __table_args__ = (
        Index('index_record_metadata_idx', 'did'),
    )

class IndexRecordUrlMetadata(Base):
    """
    Metadata attached to url
    """

    __tablename__ = 'index_record_url_metadata'
    key = Column(String, primary_key=True)
    url = Column(String, primary_key=True)
    did = Column(String, index=True, primary_key=True)
    value = Column(String)
    __table_args__ = (
        ForeignKeyConstraint(['did', 'url'],
                             ['index_record_url.did', 'index_record_url.url']),
        Index('index_record_url_metadata_idx', 'did'),
    )



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
    )


def create_urls_metadata(urls_metadata, record, session):
    """
    create url metadata record in database
    """
    urls = {u.url for u in record.urls}
    for url, url_metadata in iteritems(urls_metadata):
        if url not in urls:
            raise UserError(
                'url {} in urls_metadata does not exist'.format(url))
        for k, v in iteritems(url_metadata):
            session.add(IndexRecordUrlMetadata(
                url=url, key=k, value=v, did=record.did))


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
            metadata=None,
            ids=None):
        """
        Returns list of records stored by the backend.
        """
        with self.session as session:
            query = session.query(IndexRecord)

            if start is not None:
                query = query.filter(IndexRecord.did > start)

            if size is not None:
                query = query.filter(IndexRecord.size == size)

            if file_name is not None:
                query = query.filter(IndexRecord.file_name == file_name)

            if version is not None:
                query = query.filter(IndexRecord.version == version)

            if urls is not None and urls:
                query = query.join(IndexRecord.urls)
                for u in urls:
                    query = query.filter(IndexRecordUrl.url == u)

            if acl is not None and acl:
                query = query.join(IndexRecord.acl)
                for u in acl:
                    query = query.filter(IndexRecordACE.ace == u)

            if hashes is not None and hashes:
                for h, v in hashes.items():
                    sub = session.query(IndexRecordHash.did)
                    sub = sub.filter(and_(
                        IndexRecordHash.hash_type == h,
                        IndexRecordHash.hash_value == v,
                    ))
                    query = query.filter(IndexRecord.did.in_(sub.subquery()))

            if metadata is not None and metadata:
                for k, v in metadata.items():
                    sub = session.query(IndexRecordMetadata.did)
                    sub = sub.filter(
                        and_(
                            IndexRecordMetadata.key == k,
                            IndexRecordMetadata.value == v,
                        ))
                    query = query.filter(IndexRecord.did.in_(sub.subquery()))

            query = query.order_by(IndexRecord.did)

            if ids:
                query = query.filter(IndexRecord.did.in_(ids))
            else:
                # only apply limit when ids is not provided
                query = query.limit(limit)

            return [i.to_document_dict() for i in query]

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
                    sub = sub.filter(and_(
                        IndexRecordHash.hash_type == h,
                        IndexRecordHash.hash_value == v,
                    ))

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
                {'url': r.url,
                 'metadata': {m.key: m.value for m in r.url_metadata}}
                for r in query
            ]

    def add(self,
            form,
            did=None,
            size=None,
            file_name=None,
            metadata=None,
            urls_metadata=None,
            version=None,
            urls=None,
            acl=None,
            hashes=None,
            baseid=None):
        """
        Creates a new record given size, urls, acl, hashes, metadata,
        urls_metadata file name and version
        if did is provided, update the new record with the did otherwise create it
        """

        if urls is None:
            urls = []
        if acl is None:
            acl = []
        if hashes is None:
            hashes = {}
        if metadata is None:
            metadata = {}

        if urls_metadata is None:
            urls_metadata = {}
        with self.session as session:
            record = IndexRecord()

            base_version = BaseVersion()
            if not baseid:
                baseid = str(uuid.uuid4())

            base_version.baseid = baseid

            record.baseid = baseid
            record.file_name = file_name
            record.version = version

            record.did = did or str(uuid.uuid4())

            record.rev = str(uuid.uuid4())[:8]

            record.form, record.size = form, size

            record.urls = [IndexRecordUrl(
                did=record.did,
                url=url,
            ) for url in urls]

            record.acl = [IndexRecordACE(
                did=record.did,
                ace=ace,
            ) for ace in acl]

            record.hashes = [IndexRecordHash(
                did=record.did,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]

            record.index_metadata = [IndexRecordMetadata(
                did=record.did,
                key=m_key,
                value=m_value
            ) for m_key, m_value in metadata.items()]
            session.merge(base_version)

            try:
                session.add(record)
                create_urls_metadata(urls_metadata, record, session)

                if self.config.get('ADD_PREFIX_ALIAS'):
                    self.add_prefix_alias(record, session)
                session.commit()
            except IntegrityError:
                raise UserError('did "{did}" already exists'.format(did=record.did), 400)

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

    def update(self,
               did, rev, urls=None, acl=None, file_name=None,
               version=None, metadata=None, urls_metadata=None):
        """
        Updates an existing record with new values.
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

            if urls is not None:
                for url in record.urls:
                    session.delete(url)

                record.urls = [IndexRecordUrl(
                    did=record.did,
                    url=url
                ) for url in urls]

            if acl is not None:
                for ace in record.acl:
                    session.delete(ace)

                record.acl = [IndexRecordACE(
                    did=record.did,
                    ace=ace
                ) for ace in acl]

            if metadata is not None:
                for md_record in record.index_metadata:
                    session.delete(md_record)

                record.index_metadata = [IndexRecordMetadata(
                    did=record.did,
                    key=m_key,
                    value=m_value
                ) for m_key, m_value in metadata.items()]

            if urls_metadata is not None:
                for url in record.urls:
                    for url_metadata in url.url_metadata:
                        session.delete(url_metadata)

                create_urls_metadata(urls_metadata, record, session)

            if file_name is not None:
                record.file_name = file_name

            if version is not None:
                record.version = version

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
                    metadata=None,
                    urls_metadata=None,
                    version=None,
                    urls=None,
                    acl=None,
                    hashes=None):
        """
        Add a record version given did
        """
        if urls is None:
            urls = []
        if acl is None:
            acl = []
        if hashes is None:
            hashes = {}
        if metadata is None:
            metadata = {}
        if urls_metadata is None:
            urls_metadata = {}
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

            record.urls = [IndexRecordUrl(
                did=record.did,
                url=url,
            ) for url in urls]

            record.acl = [IndexRecordACE(
                did=record.did,
                ace=ace,
            ) for ace in acl]

            record.hashes = [IndexRecordHash(
                did=record.did,
                hash_type=h,
                hash_value=v,
            ) for h, v in hashes.items()]

            record.index_metadata = [IndexRecordMetadata(
                did=record.did,
                key=m_key,
                value=m_value
            ) for m_key, m_value in metadata.items()]

            try:
                session.add(record)
                create_urls_metadata(urls_metadata, record, session)
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
                record = session.query(BaseVersion).filter_by(baseid=did).first()
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
                query = session.execute('SELECT 1')
            except Exception as e:
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


def migrate_1(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ALTER COLUMN size TYPE bigint;"
        .format(IndexRecord.__tablename__))


def migrate_2(session, **kwargs):
    """
    Migrate db from version 1 -> 2
    """
    try:
        session.execute(
            "ALTER TABLE {} \
                ADD COLUMN baseid VARCHAR DEFAULT NULL, \
                ADD COLUMN created_date TIMESTAMP DEFAULT NOW(), \
                ADD COLUMN updated_date TIMESTAMP DEFAULT NOW()".format(IndexRecord.__tablename__))
    except ProgrammingError:
        session.rollback()
    session.commit()

    count = session.execute(
        "SELECT COUNT(*) FROM {};"
        .format(IndexRecord.__tablename__)).fetchone()[0]

    # create tmp_index_record table for fast retrival
    try:
        session.execute(
            "CREATE TABLE tmp_index_record AS SELECT did, ROW_NUMBER() OVER (ORDER BY did) AS RowNumber \
            FROM {}".format(IndexRecord.__tablename__))
    except ProgrammingError:
        session.rollback()

    for loop in range(count):
        baseid = str(uuid.uuid4())
        session.execute(
            "UPDATE index_record SET baseid = '{}'\
             WHERE did =  (SELECT did FROM tmp_index_record WHERE RowNumber = {});".format(baseid, loop + 1))
        session.execute(
            "INSERT INTO {}(baseid) VALUES('{}');".format(BaseVersion.__tablename__, baseid))

    session.execute(
        "ALTER TABLE {} \
         ADD CONSTRAINT baseid_FK FOREIGN KEY (baseid) references base_version(baseid);"
        .format(IndexRecord.__tablename__))

    # drop tmp table
    session.execute(
        "DROP TABLE IF EXISTS tmp_index_record;"
        )


def migrate_3(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ADD COLUMN file_name VARCHAR;"
        .format(IndexRecord.__tablename__))

    session.execute(
        "CREATE INDEX {tb}__file_name_idx ON {tb} ( file_name )"
        .format(tb=IndexRecord.__tablename__))


def migrate_4(session, **kwargs):
    session.execute(
        "ALTER TABLE {} ADD COLUMN version VARCHAR;"
        .format(IndexRecord.__tablename__))

    session.execute(
        "CREATE INDEX {tb}__version_idx ON {tb} ( version )"
        .format(tb=IndexRecord.__tablename__))


def migrate_5(session, **kwargs):
    """
    Create Index did on IndexRecordUrl, IndexRecordMetadata and
    IndexRecordUrlMetadata tables
    """
    session.execute(
        "CREATE INDEX {tb}_idx ON {tb} ( did )"
            .format(tb=IndexRecordUrl.__tablename__))

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
        "CREATE INDEX ix_{tb}_baseid ON {tb} ( baseid )"
        .format(tb=IndexRecord.__tablename__))


# ordered schema migration functions that the index should correspond to
# CURRENT_SCHEMA_VERSION - 1 when it's written
SCHEMA_MIGRATION_FUNCTIONS = [
    migrate_1, migrate_2, migrate_3, migrate_4, migrate_5,
    migrate_6, migrate_7, migrate_8]
CURRENT_SCHEMA_VERSION = len(SCHEMA_MIGRATION_FUNCTIONS)
