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


def assert_blank(record, with_authz=False):
    """
    Check that the fields that should be empty in a
    blank record are empty.

    Args:
        r (dict): either an indexd record or a JSON response
            such as { "records": [ { <indexd record } ] }
            (only the first record is validated).
        with_authz (bool, optional): Whether the record should contain
            an authz. Defaults to False.
    """
    # handle passing an indexd JSON response directly
    if "records" in record:
        record = record["records"][0]

    assert record["baseid"]
    assert record["did"]
    assert not record["size"]
    assert not record["acl"]
    assert not record["hashes"]
    assert not record["urls_metadata"]
    assert not record["version"]

    if with_authz:
        assert record["authz"]
    else:
        assert not record["authz"]
