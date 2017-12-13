import sqlite3

import util

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver


OLD_SQLITE = sqlite3.sqlite_version_info < (3, 7, 16)

INDEX_HOST = 'index.sq3'
ALIAS_HOST = 'alias.sq3'

INDEX_TABLES = {
    'base_version': [
        (0, u'baseid', u'VARCHAR', 1, None, 1),
    ],
    'index_record': [
        (0, u'did', u'VARCHAR', 1, None, 1),
        (1, u'baseid', u'VARCHAR', 0, None, 0),
        (2, u'rev', u'VARCHAR', 0, None, 0),
        (3, u'form', u'VARCHAR', 0, None, 0),
        (4, u'size', u'BIGINT', 0, None, 0),
        (5, u'created_date', u'DATETIME', 0, None, 0),
        (6, u'updated_date', u'DATETIME', 0, None, 0),
    ],
    'index_record_hash': [
        (0, u'did', u'VARCHAR', 1, None, 1),
        (1, u'hash_type', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
        (2, u'hash_value', u'VARCHAR', 0, None, 0),
    ],
    'index_record_url': [
        (0, u'did', u'VARCHAR', 1, None, 1),
        (1, u'url', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
    ],
}

ALIAS_TABLES = {
    'alias_record': [
        (0, u'name', u'VARCHAR', 1, None, 1),
        (1, u'rev', u'VARCHAR', 0, None, 0),
        (2, u'size', u'BIGINT', 0, None, 0),
        (3, u'release', u'VARCHAR', 0, None, 0),
        (4, u'metastring', u'VARCHAR', 0, None, 0),
        (5, u'keeper_authority', u'VARCHAR', 0, None, 0),
    ],
    'alias_record_hash': [
        (0, u'name', u'VARCHAR', 1, None, 1),
        (1, u'hash_type', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
        (2, u'hash_value', u'VARCHAR', 0, None, 0)
    ],
    'alias_record_host_authority': [
        (0, u'name', u'VARCHAR', 1, None, 1),
        (1, u'host', u'VARCHAR', 1, None, 1 if OLD_SQLITE else 2),
    ],
}

INDEX_CONFIG = {
    'driver': SQLAlchemyIndexDriver('sqlite:///index.sq3'),
}

ALIAS_CONFIG = {
    'driver': SQLAlchemyAliasDriver('sqlite:///alias.sq3'),
}


@util.removes(INDEX_HOST)
def test_sqlite3_index_setup_tables():
    '''
    Tests that the SQLite3 index database gets set up correctly.
    '''
    SQLAlchemyIndexDriver('sqlite:///index.sq3')

    with sqlite3.connect(INDEX_HOST) as conn:
        c = conn.execute('''
            SELECT name FROM sqlite_master WHERE type = 'table'
        ''')

        tables = [i[0] for i in c]

        for table in INDEX_TABLES:
            assert table in tables, '{table} not created'.format(table=table)

        for table, schema in INDEX_TABLES.items():
            # NOTE PRAGMA's don't work with parameters...
            c = conn.execute('''
                PRAGMA table_info ('{table}')
            '''.format(table=table))

            assert schema == [i for i in c]

@util.removes(ALIAS_HOST)
def test_sqlite3_alias_setup_tables():
    '''
    Tests that the SQLite3 alias database gets set up correctly.
    '''
    SQLAlchemyAliasDriver('sqlite:///alias.sq3')

    with sqlite3.connect(ALIAS_HOST) as conn:
        c = conn.execute('''
            SELECT name FROM sqlite_master WHERE type = 'table'
        ''')

        tables = [i[0] for i in c]

        for table in ALIAS_TABLES:
            assert table in tables, '{table} not created'.format(table=table)

        for table, schema in ALIAS_TABLES.items():
            # NOTE PRAGMA's don't work with parameters...
            c = conn.execute('''
                PRAGMA table_info ('{table}')
            '''.format(table=table))

            assert schema == [i for i in c]
