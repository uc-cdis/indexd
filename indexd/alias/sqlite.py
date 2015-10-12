import sqlite3

from . import driver
from . import errors


class SQLiteAliasDriver(driver.AliasDriverABC):
    '''
    SQLite3 implementation of alias driver.
    '''

    def __init__(self, **kwargs):
        '''
        Initialize the SQLite3 database driver.
        '''
        self.config = kwargs.get('SQLITE3', {})
        
        with self.conn as conn:
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS aliases (
                    alias TEXT PRIMARY KEY,
                    data TEXT
                )
            ''')

    @property
    def conn(self):
        return sqlite3.connect(self.config['host'],
            check_same_thread=False,
        )

    def aliass(self, limit=100, start=''):
        '''
        Returns list of aliass stored by the backend.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT alias FROM aliases WHERE alias > (?) ORDER BY alias LIMIT (?)
        ''', (start, limit,))
        
        return [i[0] for i in cursor]

    def __getitem__(self, alias):
        '''
        Returns data associated with alias if alias exists.
        Raises KeyError otherwise.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT data FROM aliases WHERE alias = (?)
        ''', (alias,))
        
        try: data = cursor.fetchone()[0]
        except TypeError as err:
            raise KeyError('no alias found')
        
        return data

    def __setitem__(self, alias, data):
        '''
        Sets data for the specified alias.
        Raises KeyError otherwise.
        '''
        with self.conn as conn:
            
            conn.execute('''
                INSERT OR IGNORE INTO aliases (alias, data) VALUES (?, ?)
            ''', (alias, data))

    def __delitem__(self, alias):
        '''
        Removes alias if stored by backend.
        Raises KeyError otherwise.
        '''
        if not alias in self:
            raise KeyError('alias does not exist')
        
        with self.conn as conn:
            
            conn.execute('''
                DELETE FROM alias WHERE alias = (?)
            ''', (alias,))

    def __contains__(self, alias):
        '''
        Returns True if alias is stored by backend.
        Returns False otherwise.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(alias) > 0 FROM aliases WHERE alias = (?)
        ''', (alias,))
        
        return bool(cursor.fetchone()[0])

    def __iter__(self):
        '''
        Returns an iterator over aliass.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT alias FROM aliases
        ''')
        
        return (alias[0] for alias in cursor)

    def __len__(self):
        '''
        Returns number of aliass.
        '''
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(alias) FROM aliases
        ''')
        
        return cursor.fetchone()[0]
