import os
import shutil


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
