import sqlite3

import util

from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.alias.sqlite import SQLiteAliasDriver


OLD_SQLITE = sqlite3.sqlite_version_info < (3, 7, 16)

INDEX_HOST = 'index.sq3'
ALIAS_HOST = 'alias.sq3'

INDEX_TABLES = {
    'index_record': [
        (0, u'did', u'VARCHAR', 1, None, 1),
        (1, u'rev', u'VARCHAR', 0, None, 0),
        (2, u'form', u'VARCHAR', 0, None, 0),
        (3, u'size', u'INTEGER', 0, None, 0),
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
    'aliases': [
        (0, u'alias', u'TEXT', 0, None, 1),
        (1, u'data', u'TEXT', 0, None, 0),
    ],
}

INDEX_CONFIG = {
    'driver': SQLAlchemyIndexDriver('sqlite:///index.sq3'),
}

ALIAS_CONFIG = {
    'SQLITE3': {
        'host': ALIAS_HOST,
    }
}

@util.removes('index.sq3')
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

@util.removes(ALIAS_CONFIG['SQLITE3']['host'])
def test_sqlite3_alias_setup_tables():
    '''
    Tests that the SQLite3 alias database gets set up correctly.
    '''
    SQLiteAliasDriver(**ALIAS_CONFIG)

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
