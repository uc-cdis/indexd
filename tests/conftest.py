import base64
import threading

import flask
import pytest
import requests
import swagger_client
from sqlalchemy import create_engine

from indexd import app_init, get_app
from indexd import utils as indexd_utils
from indexd.alias.drivers.alchemy import Base as alias_base
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from indexd.index.drivers.alchemy import Base as index_base
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver

PG_URL = (
    f"postgresql://{indexd_utils.IndexdConfig['user']}:{indexd_utils.IndexdConfig['password']}@"
    f"{indexd_utils.IndexdConfig['host']}/{indexd_utils.IndexdConfig['database']}"
)


@pytest.fixture(scope="session", autouse=True)
def setup_indexd_test_database(request):
    """Set up the database to be used for the tests.

    autouse: every test runs this fixture, without calling it directly
    session scope: all tests share the same fixture

    Basically this only runs once at the beginning of the full test run. This
    sets up the test database and test user to use for the rest of the tests.
    """

    # try_drop_test_data() is run before the tests starts and after the tests
    # complete. This ensures a clean database on start and end of the tests.
    indexd_utils.setup_database()
    request.addfinalizer(indexd_utils.try_drop_test_data)


def truncate_tables(driver, base):
    """Drop all the tables in this application's scope.

    This has the same effect as deleting the sqlite file. Your test will have a
    fresh database for it's run.
    """

    # Drop tables in reverse order to avoid cascade drop errors.
    # metadata is a sqlalchemy property.
    # sorted_tables is a list of tables sorted by their dependencies.
    with driver.engine.begin() as txn:
        for table in reversed(base.metadata.sorted_tables):
            # do not clear schema versions so each test does not re-trigger migration.
            if table.name not in ["index_schema_version", "alias_schema_version"]:
                txn.execute(f"TRUNCATE {table.name} CASCADE;")


@pytest.fixture
def index_driver():
    driver = SQLAlchemyIndexDriver(PG_URL, auto_migrate=False)
    yield driver
    truncate_tables(driver, index_base)
    driver.dispose()


@pytest.fixture
def alias_driver():
    driver = SQLAlchemyAliasDriver(PG_URL, auto_migrate=False)
    yield driver
    truncate_tables(driver, alias_base)
    driver.dispose()


@pytest.fixture(scope="session")
def auth_driver():
    driver = SQLAlchemyAuthDriver(PG_URL)
    yield driver
    driver.dispose()


@pytest.fixture
def indexd_admin_user(auth_driver):
    username = password = "admin"
    auth_driver.add(username, password)
    yield username, password
    auth_driver.delete("admin")


@pytest.fixture
def index_driver_no_migrate():
    """
    This fixture is designed for testing migration scripts and can be used for
    any other situation where a migration is not desired on instantiation.
    """
    driver = SQLAlchemyIndexDriver(PG_URL, auto_migrate=False)
    yield driver
    truncate_tables(driver, index_base)
    driver.dispose()


@pytest.fixture
def alias_driver_no_migrate():
    """
    This fixture is designed for testing migration scripts and can be used for
    any other situation where a migration is not desired on instantiation.
    """
    driver = SQLAlchemyAliasDriver(PG_URL, auto_migrate=False)
    yield driver
    truncate_tables(driver, alias_base)
    driver.dispose()


@pytest.fixture
def create_indexd_tables(index_driver, alias_driver, auth_driver):
    """Make sure the tables are created but don't operate on them directly.
    Also set up the password to be accessed by the client tests.
    Migration not required as tables will be created with most recent models
    """
    pass


@pytest.fixture
def create_indexd_tables_no_migrate(
    index_driver_no_migrate, alias_driver_no_migrate, auth_driver
):
    """Make sure the tables are created but don't operate on them directly.

    There is no migration required for the SQLAlchemyAuthDriver.
    Also set up the password to be accessed by the client tests.
    """
    pass


@pytest.fixture(scope="session")
def indexd_server():
    """
    Starts the indexd server, and cleans up its mess.
    Most tests will use the client which stems from this
    server fixture.

    Runs once per test session.
    """
    app = get_app()
    hostname = "localhost"
    port = 8001
    debug = False
    t = threading.Thread(
        target=app.run, kwargs={"host": hostname, "port": port, "debug": debug}
    )
    t.setDaemon(True)
    t.start()
    wait_for_indexd_alive(port)
    yield MockServer(port=port)


def wait_for_indexd_alive(port):
    url = f"http://localhost:{port}"
    try:
        requests.get(url)
    except requests.ConnectionError:
        return wait_for_indexd_alive(port)
    else:
        return


class MockServer:
    def __init__(self, port):
        self.port = port
        self.baseurl = f"http://localhost:{port}"


@pytest.fixture
def app(index_driver, alias_driver, auth_driver):
    """
    We have to give all the settings here because when a driver is initiated
    it goes through an entire migration process that creates all the tables.
    The tables are already created from the fixtures in this module.
    """
    app = flask.Flask("indexd")
    settings = {
        "config": {
            "INDEX": {
                "driver": index_driver,
            },
            "ALIAS": {
                "driver": alias_driver,
            },
        },
        "auth": auth_driver,
    }
    app_init(app, settings=settings)
    return app


@pytest.fixture
def user(auth_driver):
    auth_driver.add("test", "test")
    yield {
        "Authorization": ("Basic " + base64.b64encode(b"test:test").decode("ascii")),
        "Content-Type": "application/json",
    }

    # clean user
    auth_driver.delete("test")


@pytest.fixture
def swg_config(indexd_server, index_driver, alias_driver, indexd_admin_user):
    config = swagger_client.Configuration()
    config.host = indexd_server.baseurl
    config.username = indexd_admin_user[0]
    config.password = indexd_admin_user[1]
    return config


@pytest.fixture
def swg_config_no_migrate(indexd_server_no_migrate, create_indexd_tables_no_migrate):
    config = swagger_client.Configuration()
    config.host = "http://localhost:8001"
    config.username = "admin"
    config.password = "admin"
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
def swg_global_client_no_migrate(swg_config_no_migrate):
    return swagger_client.GlobalApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_alias_client(swg_config):
    return swagger_client.AliasApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_alias_client_no_migrate(swg_config_no_migrate):
    return swagger_client.AliasApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_query_client(swg_config):
    return swagger_client.QueryApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_query_client_no_migrate(swg_config_no_migrate):
    return swagger_client.QueryApi(swagger_client.ApiClient(swg_config_no_migrate))


@pytest.fixture
def swg_bulk_client(swg_config):
    return swagger_client.BulkApi(swagger_client.ApiClient(swg_config))


@pytest.fixture
def swg_bulk_client_no_migrate(swg_config_no_migrate):
    return swagger_client.BulkApi(swagger_client.ApiClient(swg_config_no_migrate))


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
