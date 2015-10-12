import sqlite3

from indexd.index.sqlite import SQLiteIndexDriver
from indexd.alias.sqlite import SQLiteAliasDriver

INDEX_HOST = 'index.sq3'
ALIAS_HOST = 'alias.sq3'

INDEX_TABLES = [
    'records',
    'records_hash',
    'records_urls',
]

ALIAS_TABLES = [
    'aliases',
]

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
