import json
import uuid
import sqlite3

from . import driver
from . import errors


class SQLiteIndexDriver(driver.IndexDriverABC):
    '''
    SQLite3 implementation of index driver.
    '''

    def __init__(self, **kwargs):
        '''
        Initialize the SQLite3 database driver.
        '''
        self.config = kwargs.get('SQLITE3', {})
        
        with self.conn as conn:
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS records (
                    id TEXT PRIMARY KEY,
                    rev TEXT,
                    type TEXT,
                    size INTEGER
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS records_urls (
                    id TEXT,
                    url TEXT,
                    PRIMARY KEY (id, url) ON CONFLICT IGNORE,
                    FOREIGN KEY (id) REFERENCES records (id)
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS records_urls_index
                ON records_urls (id)
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS records_hash (
                    id TEXT,
                    type TEXT,
                    hash TEXT,
                    PRIMARY KEY (id, type),
                    FOREIGN KEY (id) REFERENCES records (id)
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS records_hash_index
                ON records_hash (type, hash)
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS records_docs (
                    id TEXT PRIMARY KEY,
                    doc TEXT,
                    FOREIGN KEY (id) REFERENCES records (id)
                )
            ''')

    @property
    def conn(self):
        return sqlite3.connect(self.config['host'],
            check_same_thread=False,
        )

    def ids(self, limit=100, start=''):
        '''
        Returns list of records stored by the backend.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT id FROM records WHERE id > (?) ORDER BY id LIMIT (?)
        ''', (start, limit,))
        
        return [i[0] for i in cursor]

    def __getitem__(self, record):
        '''
        Returns record if stored by backend.
        Raises KeyError otherwise.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT id, doc FROM records_docs WHERE id = (?)
        ''', (record,))
        
        records = cursor.fetchmany(2)
        
        if not len(records):
            raise errors.NoRecordError('no record found')
        
        if len(records) > 1:
            raise errors.MultipleRecordsError('multiple records found')
        
        return json.loads(records[0][1])

    def __setitem__(self, record, data):
        '''
        Replaces record if stored by backend.
        Raises KeyError otherwise.
        '''
        rev = str(uuid.uuid4())
        
        with self.conn as conn:
            
            conn.execute('''
                INSERT OR IGNORE INTO records (id, rev) VALUES (?, ?)
            ''', (record, rev))
            
            conn.execute('''
                INSERT OR REPLACE INTO records_docs (id, doc) VALUES (?, ?)
            ''', (record, json.dumps(data)))

    def __delitem__(self, record):
        '''
        Removes record if stored by backend.
        Raises KeyError otherwise.
        '''
        with self.conn as conn:
            
            conn.execute('''
                DELETE FROM records_docs WHERE id = (?)
            ''', (record,))
            
            conn.execute('''
                DELETE FROM records WHERE id = (?)
            ''', (record,))

    def __contains__(self, record):
        '''
        Returns True if record is stored by backend.
        Returns False otherwise.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT id, doc FROM records_docs WHERE id = (?)
        ''', (record,))
        
        return cursor.fetchone() is not None

    def __iter__(self):
        '''
        Iterator over unique records stored by backend.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT id FROM records
        ''')
        
        return (record[0] for record in cursor)

    def __len__(self):
        '''
        Number of unique records stored by backend.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(id) FROM records
        ''')
        
        return cursor.fetchone()[0]
