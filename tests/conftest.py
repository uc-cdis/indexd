import base64
from multiprocessing import Process

import flask
import pytest
import requests
from indexclient.client import IndexClient
from sqlalchemy import create_engine

import swagger_client
from indexd import app_init, get_app
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
def setup_test_database(request):
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


@pytest.fixture
def app(index_driver, alias_driver, auth_driver):
    """
    We have to give all the settings here because when a driver is initiated
    it goes through an entire migration process that creates all the tables.
    The tables are already created from the fixtures in this module.
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
def swg_config_no_migrate(indexd_client_no_migrate): # noqa
    config = swagger_client.Configuration()
    config.host = indexd_client_no_migrate.url
    config.username = indexd_client_no_migrate.auth[0]
    config.password = indexd_client_no_migrate.auth[1]
    return config


@pytest.fixture
def swg_index_client(swg_config):
    return swagger_client.IndexApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_index_client_no_migrate(swg_config_no_migrate):
    return swagger_client.IndexApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_global_client(swg_config):
    return swagger_client.GlobalApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_global_client_no_migrate(swg_config):
    return swagger_client.GlobalApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_alias_client(swg_config):
    return swagger_client.AliasApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_alias_client_no_migrate(swg_config):
    return swagger_client.AliasApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_dos_client(swg_config):
    return swagger_client.DOSApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_dos_client_no_migrate(swg_config):
    return swagger_client.DOSApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_query_client(swg_config):
    return swagger_client.QueryApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_query_client_no_migrate(swg_config):
    return swagger_client.QueryApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_bulk_client(swg_config):
    return swagger_client.BulkApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_bulk_client_no_migrate(swg_config):
    return swagger_client.BulkApi(swagger_client.ApiClient(swg_config_no_migrate))


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
def create_tables(index_driver, alias_driver, auth_driver):
    """Make sure the tables are created but don't operate on them directly.

    Also set up the password to be accessed by the client tests.
    """
    auth_driver.add('admin', 'admin')


@pytest.fixture
def create_tables_no_migrate(
        index_driver_no_migrate, alias_driver_no_migrate, auth_driver):
    """Make sure the tables are created but don't operate on them directly.

    There is no migration required for the SQLAlchemyAuthDriver.
    Also set up the password to be accessed by the client tests.
    """
    auth_driver.add('admin', 'admin')


@pytest.fixture
def database_engine():
    engine = create_engine(PG_URL)
    yield engine
    engine.dispose()


@pytest.fixture
def database_conn(database_engine):
    conn = database_engine.connect()
    yield conn
    conn.close()


def drop_tables(driver, base):
    """Drop all the tables in this application's scope.

    This has the same effect as deleting the sqlite file. Your test will have a
    fresh database for it's run.
    """

    with driver.session:
        # Drop tables in reverse order to avoid cascade drop errors.
        for model in base.__subclasses__()[::-1]:
            # Check first to see if the table exists before dropping it.
            model.__table__.drop(checkfirst=True)


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


@pytest.fixture
def indexd_client(indexd_server, create_tables):
    """
    Returns a IndexClient. This will delete any documents,
    aliases, or users made by this client after the test has completed.
    Currently the default user is the admin user
    Runs once per test.
    """
    return IndexClient(
        baseurl='http://localhost:8001',
        auth=('admin', 'admin'),
    )


@pytest.fixture
def indexd_client_no_migrate(indexd_server, create_tables_no_migrate):
    """
    Returns a IndexClient. This will delete any documents,
    aliases, or users made by this client after the test has completed.
    Currently the default user is the admin user
    Runs once per test.
    """
    return IndexClient(
        baseurl='http://localhost:8001',
        auth=('admin', 'admin'),
    )


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
