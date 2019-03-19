import hashlib
import random
import uuid
from multiprocessing import Process

import pytest
import requests
from indexclient.client import Document, IndexClient
from indexd import get_app
from indexd.alias.drivers.alchemy import (
    Base as alias_base,
    SQLAlchemyAliasDriver,
)
from indexd.auth.drivers.alchemy import Base as auth_base, SQLAlchemyAuthDriver
from indexd.index.drivers.alchemy import (
    Base as index_base,
    SQLAlchemyIndexDriver,
)
from indexd.utils import setup_database, try_drop_test_data

PG_URL = 'postgres://test:test@localhost/indexd_test'


@pytest.fixture(scope='session', autouse=True)
def setup_indexd_test_database(request):
    """Set up the database to be used for the tests.

    autouse: every test runs this fixture, without calling it directly
    session scope: all tests share the same fixture

    Basically this only runs once at the beginning of the full test run. This
    sets up the test database and test user to use for the rest of the tests.
    """

    # try_drop_test_data() is run before the tests starts and after the tests
    # complete. This ensures a clean database on start and end of the tests.
    setup_database()
    request.addfinalizer(try_drop_test_data)


def drop_tables(driver, base):
    """Drop all the tables in this application's scope.

    This has the same effect as deleting the sqlite file. Your test will have a
    fresh database for it's run.
    """

    with driver.session:
        # Drop tables in reverse order to avoid cascade drop errors.
        # metadata is a sqlalchemy property.
        # sorted_tables is a list of tables sorted by their dependencies.
        for table in reversed(base.metadata.sorted_tables):
            # Check first to see if the table exists before dropping it.
            table.drop(checkfirst=True)


@pytest.fixture
def index_driver():
    driver = SQLAlchemyIndexDriver(PG_URL)
    yield driver
    drop_tables(driver, index_base)
    driver.dispose()


@pytest.fixture
def alias_driver():
    driver = SQLAlchemyAliasDriver(PG_URL)
    yield driver
    drop_tables(driver, alias_base)
    driver.dispose()


@pytest.fixture
def auth_driver():
    driver = SQLAlchemyAuthDriver(PG_URL)
    yield driver
    drop_tables(driver, auth_base)
    driver.dispose()


@pytest.fixture
def index_driver_no_migrate():
    """
    This fixture is designed for testing migration scripts and can be used for
    any other situation where a migration is not desired on instantiation.
    """
    driver = SQLAlchemyIndexDriver(PG_URL, auto_migrate=False)
    yield driver
    drop_tables(driver, index_base)
    driver.dispose()


@pytest.fixture
def alias_driver_no_migrate():
    """
    This fixture is designed for testing migration scripts and can be used for
    any other situation where a migration is not desired on instantiation.
    """
    driver = SQLAlchemyAliasDriver(PG_URL, auto_migrate=False)
    yield driver
    drop_tables(driver, alias_base)
    driver.dispose()


@pytest.fixture
def create_indexd_tables(index_driver, alias_driver, auth_driver):
    """Make sure the tables are created but don't operate on them directly.

    Also set up the password to be accessed by the client tests.
    """
    auth_driver.add('admin', 'admin')


@pytest.fixture
def create_indexd_tables_no_migrate(
        index_driver_no_migrate, alias_driver_no_migrate, auth_driver):
    """Make sure the tables are created but don't operate on them directly.

    There is no migration required for the SQLAlchemyAuthDriver.
    Also set up the password to be accessed by the client tests.
    """
    auth_driver.add('admin', 'admin')


@pytest.fixture
def indexd_client(indexd_server, create_indexd_tables):
    """Create the tables and add an auth user"""
    return IndexClient('http://localhost:8001', auth=('admin', 'admin'))


@pytest.fixture(scope='session')
def indexd_server():
    """
    Starts the indexd server, and cleans up its mess.
    Most tests will use the client which stems from this
    server fixture.

    Runs once per test session.
    """
    app = get_app()
    hostname = 'localhost'
    port = 8001
    debug = False

    indexd = Process(
        target=app.run,
        args=(hostname, port),
        kwargs={'debug': debug},
    )
    indexd.start()
    wait_for_indexd_alive(port)

    yield MockServer(port=port)
    indexd.terminate()
    wait_for_indexd_not_alive(port)


def wait_for_indexd_alive(port):
    url = 'http://localhost:{}'.format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return wait_for_indexd_alive(port)
    else:
        return


def wait_for_indexd_not_alive(port):
    url = 'http://localhost:{}'.format(port)
    try:
        requests.get(url)
    except requests.ConnectionError:
        return
    else:
        return wait_for_indexd_not_alive(port)


class MockServer(object):
    def __init__(self, port):
        self.port = port
        self.baseurl = 'http://localhost:{}'.format(port)


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
        hashes = {'md5': md5_hasher.hexdigest()}

    doc = index_client.create(
        did=did,
        hashes=hashes,
        size=random.randint(10, 1000),
        version=version,
        acl=["a", "b"],
        file_name="{}_warning_huge_file.svs".format(did),
        urls=["s3://super-safe.com/{}_warning_huge_file.svs".format(did)],
        urls_metadata={"s3://super-safe.com/{}_warning_huge_file.svs".format(did): {"a": "b"}}
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
    data["size"] = random.randint(10, 1000)
    data["hashes"] = {"md5": md5_hasher.hexdigest()}
    data["urls"] = ["s3://super-safe.com/{}_warning_huge_file.svs".format(file_name)]
    data["form"] = "object"
    data["file_name"] = "{}_warning_huge_file.svs".format(file_name)
    data["urls_metadata"] = {"s3://super-safe.com/{}_warning_huge_file.svs".format(did): {"a": "b"}}

    if version:
        data["version"] = version

    return index_client.add_version(did, Document(None, None, data))
