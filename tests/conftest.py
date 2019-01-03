import base64
import os
from multiprocessing import Process

import flask
import pytest
import requests
# indexd_server and indexd_client is needed as fixtures
from indexclient.client import IndexClient
from indexd import app_init, get_app
from indexd.alias.drivers.alchemy import (
    Base as alias_base,
    SQLAlchemyAliasDriver,
)
from indexd.auth.drivers.alchemy import Base as auth_base, SQLAlchemyAuthDriver
# from indexd.default_settings import PG_URL
from indexd.index.drivers.alchemy import (
    Base as index_base,
    SQLAlchemyIndexDriver,
)
from indexd.utils import setup_database, try_drop_test_data
from sqlalchemy import MetaData, create_engine
from tests.test_driver_alchemy_auth import PASSWORD, USERNAME

import swagger_client

PG_URL = 'postgres://test:test@localhost/indexd_test'
try:
    reload  # Python 2.7
except NameError:
    try:
        from importlib import reload  # Python 3.4+
    except ImportError:
        from imp import reload  # Python 3.0 - 3.3<Paste>


@pytest.fixture(scope='session', autouse=True)
def setup_test_database(request):
    """Set up the database to be used for the tests.

    autouse: every test runs this fixture, without calling it directly
    session scope: all tests share the same fixture

    Basically this only runs once at the beginning of the full test run. This
    sets up the test database and test user to use for the rest of the tests.
    """

    # try_drop_test_data() is run in this step, when the test suite starts,
    # so the step below is not entirely necessary. It's just good housekeeping.
    setup_database()

    def tearDown():
        try_drop_test_data()

    request.addfinalizer(tearDown)


@pytest.fixture
def app(index_driver, alias_driver, auth_driver):
    """
    We have to give all the settings here because when a driver is initiated
    it goes through an entire migration process that creates all the tables.
    The tables are already created from the fixtures in this module
    """
    app = flask.Flask('indexd')
    settings = {
        'config': {
            'INDEX': {
                'driver': index_driver,
            },
            'ALIAS': {
                'driver': alias_driver,
            },
        },
        'auth': auth_driver,
    }
    app_init(app, settings=settings)
    return app


@pytest.fixture
def user(auth_driver):
    auth_driver.add('test', 'test')
    return {
        'Authorization': (
            'Basic ' +
            base64.b64encode(b'test:test').decode('ascii')),
        'Content-Type': 'application/json'
    }


@pytest.fixture
def swg_config(indexd_client): # noqa
    config = swagger_client.Configuration()
    config.host = indexd_client.url
    config.username = indexd_client.auth[0]
    config.password = indexd_client.auth[1]
    return config


@pytest.fixture
def swg_index_client(swg_config):
    return swagger_client.IndexApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_global_client(swg_config):
    return swagger_client.GlobalApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_alias_client(swg_config):
    return swagger_client.AliasApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_dos_client(swg_config):
    return swagger_client.DOSApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_query_client(swg_config):
    return swagger_client.QueryApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_bulk_client(swg_config):
    return swagger_client.BulkApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def index_driver():
    driver = SQLAlchemyIndexDriver(PG_URL)
    yield driver
    drop_tables(driver, index_base)
    driver.dispose()
    # import pdb; pdb.set_trace()
    # pass


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
def create_tables(index_driver, alias_driver, auth_driver):
    """Make sure the tables are created but don't operate on them directly.

    Also set up the password to be accessed by the client tests.
    """
    # This is the set of credentials that the swagger client uses.
    auth_driver.add('admin', 'admin')


def database_engine():
    engine = create_engine(PG_URL)
    return engine


@pytest.fixture
def database_conn():
    engine = database_engine()
    conn = engine.connect()
    yield conn
    conn.close()
    engine.dispose()


def drop_tables(driver, base):
    """Drop all the tables in this application's scope.

    This has the same effect as deleting the sqlite file. Your test will have a
    fresh database for it's run.
    """

    with driver.session:
        # Drop tables in reverse order to avoid cascade drop errors.
        for model in base.__subclasses__()[::-1]:
            # if isinstance(driver, SQLAlchemyIndexDriver):
                # import pdb; pdb.set_trace()
            # Check first to see if the table exists.
            model.__table__.drop()


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

    indexd = Process(target=app.run, args=(hostname, port, debug))
    indexd.start()
    wait_for_indexd_alive(port)

    yield MockServer(port=port)
    indexd.terminate()
    wait_for_indexd_not_alive(port)


@pytest.fixture
def indexd_client(indexd_server, create_tables):
    """
    Returns a IndexClient. This will delete any documents,
    aliases, or users made by this
    client after the test has completed.
    Currently the default user is the admin user
    Runs once per test.
    """
    client = IndexClient(
        baseurl='http://localhost:8001',
        auth=('admin', 'admin'))
    yield client


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
