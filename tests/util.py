import datetime
import os
import shutil


class removes:
    """
    Decorator to remove a path after function call.
    """
    def __init__(self, path):
        self.path = path

    def __call__(self, f):
        """
        Ensures path is removed after function call.
        """
        def wrapper(*args, **kwargs):
            try: return f(*args, **kwargs)
            finally:
                if not os.path.exists(self.path):
                    pass
                elif os.path.isdir(self.path):
                    shutil.rmtree(self.path)
                elif os.path.isfile(self.path):
                    os.remove(self.path)

        return wrapper


def assert_blank(r):
    """
    Check that the fields that should be empty in a
    blank record are empty.
    """
    assert r.records[0].baseid
    assert r.records[0].did
    assert not r.records[0].size
    assert not r.records[0].acl
    assert not r.records[0].hashes.crc
    assert not r.records[0].hashes.md5
    assert not r.records[0].hashes.sha
    assert not r.records[0].hashes.sha256
    assert not r.records[0].hashes.sha512


def make_sql_statement(statement, args):
    """Postgres does not support question marks as placeholder variables.

    The previous sqlite implementation used a lot of raw sql statements.
    This function re-formats them as postgres statements by manually applying
    the placeholders before the sql statement is executed.

    The alternative is to make prepared statements in postgres syntax and
    execute those statements, or maybe figure out how to replace the
    raw sql into sqlalchemy.
    """
    if statement.count('?') != len(args):
        raise ValueError('Mismatch between ?\'s and args')

    for arg in args:
        if isinstance(arg, str):
            arg = f"'{arg}'"
        elif arg is None:
            arg = 'null'
        elif isinstance(arg, datetime.datetime):
            arg = f"'{arg.isoformat()}'"
        else:
            arg = str(arg)

        statement = statement.replace('?', arg, 1)

    return statement.strip()
