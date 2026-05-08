import random
import hashlib
import uuid

from indexclient.indexclient.client import Document


# Util functions copied over from cdisutils-test to avoid an transitive circular dependencies created when adding
# cdisutils-test to indexd dependencies.
def create_random_index(index_client, did=None, version=None, hashes=None):
    """
    Shorthand for creating new index entries for test purposes.
    Note:
        Expects index client v1.5.2 and above
    Args:
        index_client (indexclient.client.IndexClient): pytest fixture for index_client
        passed from actual test functions
        did (str): if specified it will be used as document did, else allows indexd to create one
        version (str): version of the index being added
        hashes (dict): hashes to store on the index, if not specified a random one is created
    Returns:
        indexclient.client.Document: the document just created
    """

    did = str(uuid.uuid4()) if did is None else did

    if not hashes:
        md5_hasher = hashlib.md5()
        md5_hasher.update(did.encode("utf-8"))
        hashes = {"md5": md5_hasher.hexdigest()}

    doc = index_client.create(
        did=did,
        hashes=hashes,
        size=random.randint(10, 1000),
        version=version,
        acl=["a", "b"],
        authz=["/gen3/programs/a/projects/b"],
        file_name="{}_warning_huge_file.svs".format(did),
        urls=["s3://super-safe.com/{}_warning_huge_file.svs".format(did)],
        urls_metadata={
            "s3://super-safe.com/{}_warning_huge_file.svs".format(did): {"a": "b"}
        },
        description="a description",
    )

    return doc


def create_random_index_version(index_client, did, version_did=None, version=None):
    """
    Shorthand for creating a dummy version of an existing index, use wisely as it does not assume any versioning
    scheme and null versions are allowed
    Args:
        index_client (IndexClient): pytest fixture for index_client
        passed from actual test functions
        did (str): existing member did
        version_did (str): did for the version to be created
        version (str): version number for the version to be created
    Returns:
        Document: the document just created
    """
    md5_hasher = hashlib.md5()
    md5_hasher.update(did.encode("utf-8"))
    file_name = did

    data = {}
    if version_did:
        data["did"] = version_did
        file_name += version_did
        md5_hasher.update(version_did.encode("utf-8"))

    data["acl"] = ["ax", "bx"]
    data["authz"] = ["/gen3/programs/ax/projects/bx"]
    data["size"] = random.randint(10, 1000)
    data["hashes"] = {"md5": md5_hasher.hexdigest()}
    data["urls"] = ["s3://super-safe.com/{}_warning_huge_file.svs".format(file_name)]
    data["form"] = "object"
    data["file_name"] = "{}_warning_huge_file.svs".format(file_name)
    data["urls_metadata"] = {
        "s3://super-safe.com/{}_warning_huge_file.svs".format(did): {"a": "b"}
    }
    data["description"] = "a description"

    if version:
        data["version"] = version

    return index_client.add_version(did, Document(None, None, data))
