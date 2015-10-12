import sqlite3

from indexd.index.sqlite import SQLiteIndexDriver
from indexd.alias.sqlite import SQLiteAliasDriver

INDEX_HOST = 'index.sq3'
ALIAS_HOST = 'alias.sq3'

INDEX_TABLES = {
    'records': [
        (0, u'id', u'TEXT', 0, None, 1),
        (1, u'rev', u'TEXT', 0, None, 0),
        (2, u'type', u'TEXT', 0, None, 0),
        (3, u'size', u'INTEGER', 0, None, 0),
    ],
    'records_hash': [
        (0, u'id', u'TEXT', 0, None, 1),
        (1, u'type', u'TEXT', 0, None, 2),
        (2, u'hash', u'TEXT', 0, None, 0),
    ],
    'records_urls': [
        (0, u'id', u'TEXT', 0, None, 1),
        (1, u'url', u'TEXT', 0, None, 2),
    ],
}

ALIAS_TABLES = {
    'aliases': [
        (0, u'alias', u'TEXT', 0, None, 1),
        (1, u'data', u'TEXT', 0, None, 0),
    ],
}

INDEX_CONFIG = {
    'SQLITE3': {
        'host': INDEX_HOST,
    }
}

ALIAS_CONFIG = {
    'SQLITE3': {
        'host': ALIAS_HOST,
    }
}

def test_sqlite3_index_setup_tables():
    '''
    Tests that the SQLite3 index database gets set up correctly.
    '''
    SQLiteIndexDriver(**INDEX_CONFIG)

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
