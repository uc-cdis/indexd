import os
import shutil


class removes(object):
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
            try:
                return f(*args, **kwargs)
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
    assert not r.records[0].authz
    assert not r.records[0].hashes.crc
    assert not r.records[0].hashes.md5
    assert not r.records[0].hashes.sha
    assert not r.records[0].hashes.sha256
    assert not r.records[0].hashes.sha512
