import base64
import os

import pytest
# indexd_server and indexd_client is needed as fixtures
from cdisutilstest.code.conftest import indexd_client, indexd_server  # noqa
from cdisutilstest.code.indexd_fixture import clear_database
from sqlalchemy import MetaData, create_engine

import swagger_client
from indexd import get_app
from indexd.alias.drivers.alchemy import SQLAlchemyAliasDriver
from indexd.auth.drivers.alchemy import SQLAlchemyAuthDriver
from indexd.index.drivers.alchemy import SQLAlchemyIndexDriver
from indexd.utils import setup_database, try_drop_test_data

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
        # FIXME: This doesn't completely drop the database because pg is holding
        # onto connections. Try to find the connections and close them properly.
        try_drop_test_data()

    request.addfinalizer(tearDown)


@pytest.fixture
def app():
    # this is to make sure sqlite is initialized
    # for every unittest
    from indexd import default_settings
    reload(default_settings)
    yield get_app()
    try:
        clear_database()

    except:
        pass


@pytest.fixture
def user(app):
    app.auth.add('test', 'test')
    yield {
        'Authorization': (
            'Basic ' +
            base64.b64encode(b'test:test').decode('ascii')),
        'Content-Type': 'application/json'
    }
    app.auth.delete('test')


@pytest.fixture
def swg_config(indexd_client): # noqa
    config = swagger_client.Configuration()
    config.host = indexd_client.url
    config.username = indexd_client.auth[0]
    config.password = indexd_client.auth[1]
    yield config


@pytest.fixture
def swg_index_client(swg_config):
    api = swagger_client.IndexApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_global_client(swg_config):
    api = swagger_client.GlobalApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_alias_client(swg_config):
    api = swagger_client.AliasApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_dos_client(swg_config):
    api = swagger_client.DOSApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_query_client(swg_config):
    api = swagger_client.QueryApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def swg_bulk_client(swg_config):
    api = swagger_client.BulkApi(swagger_client.ApiClient(swg_config))
    yield api


@pytest.fixture
def index_driver():
    drop_tables()
    conn = SQLAlchemyIndexDriver('postgres://test:test@localhost/indexd_test')  # , auto_migrate=False)
    yield conn
    # cleanup
    drop_tables()


@pytest.fixture
def alias_driver():
    drop_tables()
    conn = SQLAlchemyAliasDriver('postgres://test:test@localhost/indexd_test')  # , auto_migrate=False)
    yield conn
    # cleanup
    drop_tables()


@pytest.fixture
def auth_driver():
    drop_tables()
    conn = SQLAlchemyAuthDriver('postgres://test:test@localhost/indexd_test')
    yield conn
    # cleanup
    drop_tables()


def database_engine():
    return create_engine(
        'postgres://{user}:{password}@localhost/indexd_test'.format(
            user='test',
            password='test',
        )
    )


@pytest.fixture
def database_conn():
    engine = database_engine()
    conn = engine.connect()
    yield conn
    conn.close()


def drop_tables():
    """Drop all the tables in this application's scope.

    This has the same effect as deleting the sqlite file. Your test will have a
    fresh database for it's run.
    """
    engine = database_engine()
    meta = MetaData(engine)
    meta.reflect()
    meta.drop_all()
