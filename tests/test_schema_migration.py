import uuid
import sqlite3

from sqlalchemy import create_engine

from indexd.utils import setup_database, create_tables
from indexd.index.drivers.alchemy import (
    SQLAlchemyIndexDriver, IndexSchemaVersion)

from indexd.alias.drivers.alchemy import (
    SQLAlchemyAliasDriver, AliasSchemaVersion)

from indexd.index.drivers.alchemy import migrate_1, migrate_2


INDEX_TABLES = {
    'base_version': [
        (u'baseid', u'character varying'),
    ],
    'index_record': [
        (u'did', u'character varying'),
        (u'rev', u'character varying'),
        (u'form', u'character varying'),
        (u'size', u'bigint'),
        (u'baseid', u'character varying'),
        (u'created_date', u'timestamp without time zone'),
        (u'updated_date', u'timestamp without time zone'),
    ],
    'index_record_hash': [
        (u'did', u'character varying'),
        (u'hash_type', u'character varying'),
        (u'hash_value', u'character varying'),
    ],
    'index_record_url': [
        (u'did', u'character varying'),
        (u'url', u'character varying'),
    ],
}

driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
driver = SQLAlchemyAliasDriver('sqlite:///alias.sq3')

def update_version_table_for_testing(db, tb_name, val):
    with sqlite3.connect(db) as conn:
        conn.execute('''
                DELETE FROM {}
            '''.format(tb_name))
        conn.execute('''
                INSERT INTO {} (version) VALUES ({})
            '''.format(tb_name, val))

def test_migrate_index(monkeypatch):
    called = []

    def mock_migrate(**kwargs):
        called.append(True)

    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION', 2)
    monkeypatch.setattr(
         'indexd.utils.check_engine_for_migrate',
         lambda _: True
    )

    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS',
        [mock_migrate, mock_migrate])

    update_version_table_for_testing('index.sq3', 'index_schema_version', 0)
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')

    assert len(called) == 2
    with driver.session as s:
        v = s.query(IndexSchemaVersion).first()
        assert v.version == 2
        s.delete(v)

def test_migrate_index_only_diff(monkeypatch):
    called = []

    def mock_migrate(**kwargs):
        called.append(True)

    called_2 = []
    def mock_migrate_2(**kwargs):
        called_2.append(True)

    monkeypatch.setattr(
         'indexd.utils.check_engine_for_migrate',
         lambda _: True
    )
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION', 1)
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS',
        [mock_migrate, mock_migrate_2])

    update_version_table_for_testing('index.sq3', 'index_schema_version', 0)

    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
    assert len(called) == 1
    assert len(called_2) == 0

    called = []
    called_2 = []
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION', 2)

    update_version_table_for_testing('index.sq3', 'index_schema_version', 1)
    driver = SQLAlchemyIndexDriver('sqlite:///index.sq3')
    assert len(called) == 0
    assert len(called_2) == 1

    with driver.session as s:
        v = s.query(IndexSchemaVersion).first()
        assert v.version == 2
        #s.delete(v)

def test_migrate_alias(monkeypatch):
    called = []

    def mock_migrate(**kwargs):
        called.append(True)

    monkeypatch.setattr(
        'indexd.alias.drivers.alchemy.CURRENT_SCHEMA_VERSION', 1)
    monkeypatch.setattr(
        'indexd.alias.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS',
        [mock_migrate])

    monkeypatch.setattr(
         'indexd.utils.check_engine_for_migrate',
         lambda _: True
    )

    update_version_table_for_testing('alias.sq3', 'alias_schema_version', 0)

    driver = SQLAlchemyAliasDriver('sqlite:///alias.sq3')
    assert len(called) == 1
    with driver.session as s:
        v = s.query(AliasSchemaVersion).first()
        assert v.version == 1
        s.delete(v)

def test_migrate_index_versioning(monkeypatch):

    monkeypatch.setattr(
       'indexd.index.drivers.alchemy.CURRENT_SCHEMA_VERSION', 2)
    monkeypatch.setattr(
        'indexd.index.drivers.alchemy.SCHEMA_MIGRATION_FUNCTIONS',
        [migrate_1, migrate_2])

    monkeypatch.setattr(
         'indexd.utils.check_engine_for_migrate',
         lambda _: True
    )

    setup_database('test', 'test', 'test_migration_db',
                   no_drop=False, no_user=False)
    create_tables('localhost', 'test', 'test', 'test_migration_db')

    stm = "postgres://test:test@localhost/test_migration_db"

    engine = create_engine(stm)
    conn = engine.connect()

    conn.execute("\
        INSERT INTO index_schema_version VALUES(0)\
        ")
    conn.execute("commit")

    for _ in range(10):
        did = str(uuid.uuid4())
        rev = str(uuid.uuid4())[:8]
        size = 512
        form = 'object'
        conn.execute("\
            INSERT INTO index_record(did, rev, form, size) \
            VALUES ('{}','{}','{}',{})".format(did, rev, form, size))

    conn.execute("commit")
    driver = SQLAlchemyIndexDriver(stm)
    with driver.session as s:
        v = s.query(IndexSchemaVersion).first()
        assert v.version == 2
        s.delete(v)

    tables = conn.execute("\
        SELECT tablename \
        FROM pg_catalog.pg_tables \
        WHERE tableowner = 'test'")

    tables = [t[0] for t in tables]

    for table in INDEX_TABLES:
        assert table in tables, '{table} not created'.format(table=table)

    # for table, schema in INDEX_TABLES.items():
    #     cols = conn.execute("\
    #         SELECT column_name, data_type \
    #         FROM information_schema.columns \
    #         WHERE table_schema = 'public' AND table_name = '{table}'".
    #         format(table=table))
    #     assert schema == [i for i in cols]

    vids = conn.execute("SELECT baseid FROM index_record").fetchall()

    for baseid in vids:
        c = conn.execute("\
            SELECT COUNT(*) AS number_rows \
            FROM base_version \
            WHERE baseid = '{}';".format(baseid[0])).fetchone()[0]
        assert c == 1

    conn.close()
